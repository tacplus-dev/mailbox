//! Round-trip, paired-control and read-after-write tests for the Mailbox service.
//!
//! WHAT THESE DRIVE. Three seams, and the tests go through the real one where a
//! real one can be reached:
//!
//!   - The two RPCs go in through the GENERATED `ValidationWrapper`, with a
//!     signed bearer on the request metadata, exactly as a real client's call
//!     arrives. The `(spec.principal)` and `(spec.namespace)` stamps and the
//!     `(spec.store_principal)` / `(spec.store_namespace)` egress backstops are
//!     rendered into that wrapper and NOWHERE else, so a test that called the
//!     handler trait directly would quietly be asserting that this service
//!     re-implements them — which it deliberately does not.
//!   - The store is a FAKE STRATUM behind the `MailboxEntryStore` seam, answering
//!     the way stratum's generated write surface does (tacplus 2.5): a write is
//!     final and synchronous, done with a new revision or refused with the
//!     reason — ALREADY_EXISTS, ABORTED, NOT_FOUND, and UNAVAILABLE while its
//!     redo is unreachable. An acked write is readable at once and lands on the
//!     record's change feed, the tail `WatchMailboxEntryChanges` serves; a
//!     refused one does neither.
//!   - The consumer-group path goes in through `on_message` with a `RecordMeta`,
//!     which is what the generated subscriber hands the handler.
//!
//! WHY THE STORE IS A FAKE AND NOT THE GENERATED CLIENT: the record plane's
//! server trait (`query::RecordMap`) covers every `(spec.record)` in the fleet,
//! its query plane and now its write surface, and a fixture implementing it is
//! not a fixture. So `RecordPlaneStore` — the lines that forward to the
//! generated `RecordMapGrpcClient` and reader — is the one part of the store
//! path these tests do not cover, exactly as letsencrypt's `recordStore` is the
//! part its `certRecordStore` fake does not cover.
//!
//! WHAT THESE DO NOT PROVE. That stratum and its redo answer as the fake does
//! (stratum's own tests hold that), or anything about the record store's
//! residency clock: `(spec.record).residency` is a declaration the store acts
//! on, with no runtime surface in this service to drive or observe.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::mailbox::{
    Authorizer, ValidationWrapper,
};
use mailbox_model::types::generated::eventbus_support::eventbus_support::RecordMeta;
use mailbox_model::types::services::mailbox::mailbox_server::Mailbox;
use mailbox_model::types::services::mailbox::*;
use mailbox_model::types::services::messaging::Message;
use mailbox_model::types::sys::accounting::accounting::Event;
use mailbox_model::types::vservices::query::{
    MailboxEntryFilter, MailboxEntryWatchChange, RecordChangeType,
};

use crate::service::{MailboxEntryStore, MailboxService};

/// The tenant every fixture token carries, and a second one for the namespace
/// and routing tests to disagree with. Both are `/tenants/<tid>`: a namespace in
/// no tenant has no Stratum to hold its mail.
const TENANT: &str = "/tenants/acme0eng";
const OTHER_TENANT: &str = "/tenants/acme0sal";

/// The tid of a namespace, as the chassis reads it: the tenant whose Stratum
/// holds that namespace's rows.
fn tid(namespace: &str) -> Option<String> {
    servicemanager::tenant_route::tenant_of_caller_namespace(namespace)
}

const ALICE: &str = "user:alice";
const BOB: &str = "user:bob";

// ---------------------------------------------------------------------------
// The fake stratum
// ---------------------------------------------------------------------------

/// One record type's rows in every tenant's Stratum, answering as stratum's
/// generated write surface does.
///
/// ONE STORE PER TENANT, as Stratum is (roadmap
/// decisions/stratum-per-tenant-2026-10-25.md): rows are keyed by the tid a call
/// was routed to, so a row is only ever found in the tenant it was written to.
/// Every call is checked as the generated client and the tenant-owned instance
/// check it: an empty or malformed `target_tenant` is INVALID_ARGUMENT before
/// anything is reached, and a write of a row outside the routed tenant is
/// PERMISSION_DENIED (tacplus#207). `routes` records each call's tenant, so a
/// test can assert where a call went and not only what it found.
///
/// Every acked write gets the next revision and goes onto `feed`, the change
/// feed `WatchMailboxEntryChanges` serves; a refused write changes nothing and
/// is on no feed. `refuse_writes` stands in for the redo service being
/// unreachable (UNAVAILABLE) or for a durability level not yet served
/// (FAILED_PRECONDITION): reads still answer, because the rows are in memory,
/// and every write is refused with that code.
#[derive(Default)]
struct FakeStratum {
    /// (tid, delivery_id) -> (row, revision).
    rows: Mutex<HashMap<(String, String), (MailboxEntry, i64)>>,
    revision: Mutex<i64>,
    feed: Mutex<Vec<MailboxEntryWatchChange>>,
    refuse_writes: Mutex<Option<tonic::Code>>,
    /// Writes another writer lands just before this service's next
    /// `replace_if_unchanged`, so that one finds the entry changed underneath.
    interleaved: Mutex<Vec<MailboxEntry>>,
    /// (method, target_tenant) of every call, in order.
    routes: Mutex<Vec<(&'static str, String)>>,
}

impl FakeStratum {
    fn refuse_writes(&self, code: Option<tonic::Code>) {
        *self.refuse_writes.lock().unwrap() = code;
    }

    fn interleave(&self, entry: MailboxEntry) {
        self.interleaved.lock().unwrap().push(entry);
    }

    fn refusal(&self) -> Result<(), tonic::Status> {
        match *self.refuse_writes.lock().unwrap() {
            Some(code) => Err(tonic::Status::new(code, "fake stratum: write refused")),
            None => Ok(()),
        }
    }

    /// Record the call's tenant and refuse it as the generated router does.
    fn route(&self, method: &'static str, tenant: &str) -> Result<(), tonic::Status> {
        self.routes
            .lock()
            .unwrap()
            .push((method, tenant.to_string()));
        servicemanager::tenant_route::route_tenant("target_tenant", tenant).map(|_| ())
    }

    /// A tenant's Stratum holds only that tenant's rows (tacplus#207).
    fn owned(tenant: &str, entry: &MailboxEntry) -> Result<(), tonic::Status> {
        match tid(&entry.namespace) {
            Some(t) if t == tenant => Ok(()),
            _ => Err(tonic::Status::permission_denied(
                "fake stratum: row outside the routed tenant",
            )),
        }
    }

    /// Apply an acked write: the next revision, the row, and the feed.
    fn commit(&self, tenant: &str, entry: MailboxEntry, change: RecordChangeType) -> i64 {
        let mut revision = self.revision.lock().unwrap();
        *revision += 1;
        self.rows.lock().unwrap().insert(
            (tenant.to_string(), entry.delivery_id.clone()),
            (entry.clone(), *revision),
        );
        self.feed.lock().unwrap().push(MailboxEntryWatchChange {
            r#type: change as i32,
            item: Some(entry),
            revision: *revision,
            ..Default::default()
        });
        *revision
    }

    /// The changes `WatchMailboxEntryChanges{from_revision}` delivers after
    /// `from_revision`, in order.
    fn watch(&self, from_revision: i64) -> Vec<MailboxEntryWatchChange> {
        self.feed
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.revision > from_revision)
            .cloned()
            .collect()
    }

    /// Every row of one tenant, in key order — the order a store with an
    /// indexed key would page in, so `list` below can page deterministically.
    fn all_in(&self, tenant: &str) -> Vec<MailboxEntry> {
        let mut rows: Vec<MailboxEntry> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|(_, (row, _))| row.clone())
            .collect();
        rows.sort_by(|a, b| a.delivery_id.cmp(&b.delivery_id));
        rows
    }

    /// Every row of every tenant.
    fn all(&self) -> Vec<MailboxEntry> {
        self.rows
            .lock()
            .unwrap()
            .values()
            .map(|(row, _)| row.clone())
            .collect()
    }

    /// The row for `delivery_id` in whichever tenant holds it.
    fn stored(&self, delivery_id: &str) -> Option<MailboxEntry> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .find(|((_, id), _)| id == delivery_id)
            .map(|(_, (row, _))| row.clone())
    }

    /// The row for `delivery_id` in `tenant`'s Stratum.
    fn stored_in(&self, tenant: &str, delivery_id: &str) -> Option<MailboxEntry> {
        self.rows
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), delivery_id.to_string()))
            .map(|(row, _)| row.clone())
    }

    fn routes(&self) -> Vec<(&'static str, String)> {
        self.routes.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl MailboxEntryStore for FakeStratum {
    /// `Get<T>` answers a key only for an entry in the named tenant.
    async fn get(
        &self,
        tenant: &str,
        delivery_id: &str,
        namespace: &str,
    ) -> Result<Option<(MailboxEntry, i64)>, tonic::Status> {
        self.route("get", tenant)?;
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), delivery_id.to_string()))
            .filter(|(row, _)| row.namespace == namespace)
            .cloned())
    }

    /// The criteria this fake honours are the ones `MailboxEntryFilter` carries
    /// and the service actually sets: `recipient`. Anything else the service
    /// started sending would silently not narrow here, which is why the
    /// namespace tests below assert on rows and not on the predicate.
    async fn list(
        &self,
        tenant: &str,
        filter: MailboxEntryFilter,
        page_size: i32,
        page_token: String,
    ) -> Result<(Vec<MailboxEntry>, String), tonic::Status> {
        self.route("list", tenant)?;
        let matching: Vec<MailboxEntry> = self
            .all_in(tenant)
            .into_iter()
            .filter(|r| {
                filter
                    .recipient
                    .as_ref()
                    .map_or(true, |want| &r.recipient == want)
            })
            .filter(|r| r.delivery_id.as_str() > page_token.as_str())
            .collect();
        let page: Vec<MailboxEntry> = matching.iter().take(page_size as usize).cloned().collect();
        let next = if matching.len() > page.len() {
            page.last().map(|r| r.delivery_id.clone()).unwrap_or_default()
        } else {
            String::new()
        };
        Ok((page, next))
    }

    async fn insert_if_absent(&self, tenant: &str, entry: MailboxEntry) -> Result<i64, tonic::Status> {
        self.route("insert_if_absent", tenant)?;
        Self::owned(tenant, &entry)?;
        self.refusal()?;
        let key = (tenant.to_string(), entry.delivery_id.clone());
        if self.rows.lock().unwrap().contains_key(&key) {
            return Err(tonic::Status::already_exists("fake stratum: entry exists"));
        }
        Ok(self.commit(tenant, entry, RecordChangeType::Add))
    }

    async fn replace_if_unchanged(
        &self,
        tenant: &str,
        entry: MailboxEntry,
        expected_revision: i64,
    ) -> Result<i64, tonic::Status> {
        self.route("replace_if_unchanged", tenant)?;
        Self::owned(tenant, &entry)?;
        self.refusal()?;
        for other in std::mem::take(&mut *self.interleaved.lock().unwrap()) {
            let other_tenant = tid(&other.namespace).expect("an interleaved row in a tenant");
            self.commit(&other_tenant, other, RecordChangeType::Modify);
        }
        let current = self
            .rows
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), entry.delivery_id.clone()))
            .map(|(_, revision)| *revision);
        match current {
            None => Err(tonic::Status::not_found("fake stratum: no such entry")),
            Some(r) if r != expected_revision => {
                Err(tonic::Status::aborted("fake stratum: changed underneath"))
            }
            Some(_) => Ok(self.commit(tenant, entry, RecordChangeType::Modify)),
        }
    }
}

// ---------------------------------------------------------------------------
// The generated seam's two collaborators
// ---------------------------------------------------------------------------

/// The authorizer the wrapper calls before every RPC.
///
/// A real deployment's authorizer verifies the token's signature against the
/// issuer's JWK and checks entitlements; this one only PARSES it, because the
/// thing under test is the wrapper's contract, not oauth.
///
/// Note what it does NOT supply: the namespace. The wrapper reads that off the
/// PARSED TOKEN (`Caller::namespace()`), not off this map, so an authorizer
/// cannot widen a caller's tenant by returning a different one. The fixture is
/// deliberately shaped so the tests could not accidentally paper over that.
struct TokenAuthorizer;

impl TokenAuthorizer {
    fn identity(authorization: Option<&str>) -> Result<HashMap<String, String>, tonic::Status> {
        let parsed = authorization
            .and_then(|v| v.strip_prefix("Bearer "))
            .and_then(|t| token::token::Token::parse(t).ok())
            .ok_or_else(|| tonic::Status::unauthenticated("no parsable bearer token"))?;
        let mut identity = HashMap::new();
        identity.insert(
            "sub".to_string(),
            parsed.subject().unwrap_or_default().to_string(),
        );
        Ok(identity)
    }
}

#[tonic::async_trait]
impl Authorizer for TokenAuthorizer {
    async fn authorize(
        &self,
        _full_method: &str,
        authorization: Option<&str>,
    ) -> Result<HashMap<String, String>, tonic::Status> {
        Self::identity(authorization)
    }

    async fn check_at_or_below(
        &self,
        _full_method: &str,
        authorization: Option<&str>,
        _target_namespace: &str,
    ) -> Result<HashMap<String, String>, tonic::Status> {
        Self::identity(authorization)
    }

    // Signature-and-expiry only, per the trait's own contract. Same body as the
    // other two here for the same reason they share one: this fixture's whole
    // job is to turn a fixture bearer into a caller identity, and the three
    // differ only in the checks a REAL authorizer layers on top of that.
    async fn authenticate(
        &self,
        _full_method: &str,
        authorization: Option<&str>,
    ) -> Result<HashMap<String, String>, tonic::Status> {
        Self::identity(authorization)
    }
}

/// An accounting `Sink` that keeps the events instead of publishing them.
/// Mailbox writes no accounting itself; the wrapper does, and it needs somewhere
/// to put it.
struct CapturingSink(Arc<Mutex<Vec<Event>>>);

#[tonic::async_trait]
impl am::Sink for CapturingSink {
    async fn emit(&self, event: &Event) {
        self.0.lock().unwrap().push(event.clone());
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    /// The service, reachable directly for the consumer-group path (which is not
    /// an RPC) and through the wrapper for the RPCs.
    svc: Arc<MailboxService>,
    wrapper: ValidationWrapper,
    /// The store the service writes through, so a test can read the rows and
    /// the change feed — the only way to assert on a field no response carries.
    stratum: Arc<FakeStratum>,
}

impl Harness {
    async fn start() -> Harness {
        let stratum = Arc::new(FakeStratum::default());
        let svc = Arc::new(MailboxService::new(stratum.clone()));

        // The generated seam, wired the way the ServiceManager wires it at
        // startup: an authorizer (mandatory — the wrapper fails closed without
        // one) and an accounting emitter.
        let accounting: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let wrapper = ValidationWrapper::new(svc.clone());
        wrapper
            .set_authorize_func(Some(Arc::new(TokenAuthorizer)))
            .await;
        wrapper.set_instance_id("testinst".to_string()).await;
        wrapper
            .set_emitter(Some(Arc::new(am::Emitter::new(
                "mailbox-testinst".to_string(),
                Box::new(CapturingSink(accounting)),
            ))))
            .await;

        Harness {
            svc,
            wrapper,
            stratum,
        }
    }

    /// A signed bearer the wrapper's `token::Token::parse` accepts.
    ///
    /// Signed with a throwaway key because `Token::base64()` only exists once a
    /// token has been signed — nothing downstream verifies the signature, so the
    /// key is a means of SERIALISING a token, not of trusting one.
    fn bearer(subject: &str, namespace: &str) -> String {
        let key = key::keys::generate_ed25519_key().unwrap();
        let mut t = token::token::Token::new();
        t.set_issuer("mailbox-tests").unwrap();
        t.set_subject(subject).unwrap();
        t.set_expiration(now_secs() + 3600).unwrap();
        t.set_namespace(namespace).unwrap();
        t.sign(&key).unwrap();
        t.base64().unwrap().to_string()
    }

    /// A `tonic::Request` carrying that bearer, exactly as a client would send
    /// one — the wrapper reads the principal and the namespace off the metadata,
    /// never off a field.
    fn signed<T>(req: T, subject: &str, namespace: &str) -> tonic::Request<T> {
        let mut request = tonic::Request::new(req);
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", Harness::bearer(subject, namespace))
                .parse()
                .unwrap(),
        );
        request
    }

    /// An unsigned request, for the "no caller at all" case.
    fn unsigned<T>(req: T) -> tonic::Request<T> {
        tonic::Request::new(req)
    }

    /// Every change the store's Watch delivers from the start.
    fn watched(&self) -> Vec<MailboxEntryWatchChange> {
        self.stratum.watch(0)
    }

    fn row(&self, delivery_id: &str) -> Option<MailboxEntry> {
        self.stratum.stored(delivery_id)
    }

    fn rows(&self) -> Vec<MailboxEntry> {
        self.stratum.all()
    }

    /// Deliver one message on the consumer group, as the generated subscriber
    /// would. `at_ms` is the BROKER timestamp, which is what makes a replay
    /// deliver the same value twice.
    async fn deliver(&self, msg: Message, at_ms: i64) {
        self.svc
            .on_message(msg, &meta_at(at_ms))
            .await
            .expect("consume");
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn meta_at(ms: i64) -> RecordMeta {
    RecordMeta {
        timestamp: Some(prost_wkt_types::Timestamp {
            seconds: ms / 1000,
            nanos: ((ms % 1000) * 1_000_000) as i32,
        }),
        ..Default::default()
    }
}

/// One delivery of one message, as messaging publishes it: `delivery_id` is
/// derived as `<message_id>/<recipient>`, and `namespace` is already stamped by
/// eventbus by the time a consumer sees it.
fn delivery(message_id: &str, recipient: &str, namespace: &str) -> Message {
    Message {
        delivery_id: format!("{message_id}/{recipient}"),
        message_id: message_id.to_string(),
        sender: "user:dennis".to_string(),
        recipient: recipient.to_string(),
        recipients: vec![recipient.to_string()],
        subject: "standup".to_string(),
        text: "0930 in the small room".to_string(),
        sent_at_ms: 1_700_000_000_000,
        namespace: namespace.to_string(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// The consumer group — one delivery in, one HELD entry out
// ---------------------------------------------------------------------------

/// A message on the topic becomes a HELD entry on the SAME key, carrying no
/// body.
#[tokio::test]
async fn a_delivery_becomes_a_held_entry_on_the_same_key() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let row = h.row("m1/alice").or_else(|| h.row(&format!("m1/{ALICE}")));
    let row = row.expect("an entry for the delivery");

    // Same key as the Message. This is what makes the two joinable by identity.
    assert_eq!(row.delivery_id, format!("m1/{ALICE}"));
    assert_eq!(row.message_id, "m1");
    assert_eq!(row.recipient, ALICE);
    assert_eq!(row.status, MailboxEntryStatus::Held as i32);
    assert_eq!(row.namespace, TENANT);

    // The arrival clock is the BROKER's timestamp, not this process's.
    assert_eq!(row.held_since_ms, 1_700_000_100_000);
    assert_eq!(row.retrieved_at_ms, 0);

    // The summary line, and NOT the body. `text` has no field to land in — the
    // record has none — so this asserts the three that exist and that the
    // struct genuinely carries no body field to have populated.
    assert_eq!(row.sender, "user:dennis");
    assert_eq!(row.subject, "standup");
    assert_eq!(row.sent_at_ms, 1_700_000_000_000);

    // Written through the store, and so delivered by its Watch: one ADD, the
    // row as stored.
    let watched = h.watched();
    assert_eq!(watched.len(), 1);
    assert_eq!(watched[0].r#type, RecordChangeType::Add as i32);
    assert_eq!(watched[0].item.as_ref(), Some(&row));
}

/// The service stamps NO identity and NO store epoch. Both are the store's to
/// stamp, and a value written here would be a claim nothing verified.
#[tokio::test]
async fn the_service_stamps_no_identity_and_no_store_epoch() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let written = h.row(&format!("m1/{ALICE}")).unwrap();
    assert_eq!(written.spiffe_id, "");
    assert_eq!(written.store_epoch_ms, 0);
}

/// REPLAY IS A NO-OP. `earliest` means the whole topic is re-delivered on every
/// reconnect, so this is not an edge case — it is the steady state.
#[tokio::test]
async fn a_replayed_delivery_writes_nothing_new() {
    let h = Harness::start().await;
    let msg = delivery("m1", ALICE, TENANT);

    h.deliver(msg.clone(), 1_700_000_100_000).await;
    h.deliver(msg.clone(), 1_700_000_100_000).await;
    h.deliver(msg, 1_700_000_100_000).await;

    assert_eq!(h.rows().len(), 1, "one delivery is one row, however often it is replayed");
    assert_eq!(
        h.watched().len(),
        1,
        "a replay must not rewrite; the store's copy is already correct"
    );
}

/// THE REPLAY HAZARD THIS DESIGN EXISTS TO AVOID: a replay must never write a
/// RETRIEVED row back to HELD, which would hand a caller mail they already took.
#[tokio::test]
async fn a_replay_does_not_resurrect_retrieved_mail() {
    let h = Harness::start().await;
    let msg = delivery("m1", ALICE, TENANT);
    h.deliver(msg.clone(), 1_700_000_100_000).await;

    h.wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve");

    h.deliver(msg, 1_700_000_100_000).await;

    let row = h.row(&format!("m1/{ALICE}")).unwrap();
    assert_eq!(
        row.status,
        MailboxEntryStatus::Retrieved as i32,
        "a replay must not undo a retrieval"
    );
}

/// A delivery with no key or no addressee is dropped rather than retried — a
/// returned error would make the subscriber reconnect and replay into it
/// forever.
#[tokio::test]
async fn a_delivery_with_no_key_is_dropped_not_retried() {
    let h = Harness::start().await;
    let mut msg = delivery("m1", ALICE, TENANT);
    msg.delivery_id = String::new();
    h.deliver(msg, 1_700_000_100_000).await;
    assert!(h.rows().is_empty());
    assert!(h.watched().is_empty());
}

/// While stratum's redo is unreachable it refuses every write, and the delivery
/// is NOT acknowledged: the error goes back to the subscriber, which redelivers
/// it. A delivery acked without its row would be mail nobody is ever told about.
#[tokio::test]
async fn a_delivery_refused_while_redo_is_unreachable_is_redelivered() {
    let h = Harness::start().await;
    h.stratum.refuse_writes(Some(tonic::Code::Unavailable));

    let err = h
        .svc
        .on_message(delivery("m1", ALICE, TENANT), &meta_at(1_700_000_100_000))
        .await
        .expect_err("a refused write must not acknowledge the delivery");
    let status = err
        .downcast_ref::<tonic::Status>()
        .expect("the store's refusal, as it answered");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(h.rows().is_empty());
    assert!(h.watched().is_empty(), "a refused write is on no change feed");

    // The redelivery, once the redo is back, holds it.
    h.stratum.refuse_writes(None);
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    assert_eq!(
        h.row(&format!("m1/{ALICE}")).unwrap().status,
        MailboxEntryStatus::Held as i32
    );
}

// ---------------------------------------------------------------------------
// ListHeldMail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_held_mail_returns_only_held_entries_for_the_caller() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.deliver(delivery("m2", ALICE, TENANT), 1_700_000_200_000)
        .await;
    h.deliver(delivery("m3", BOB, TENANT), 1_700_000_300_000)
        .await;

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    let ids: Vec<&str> = resp.entries.iter().map(|e| e.delivery_id.as_str()).collect();
    assert_eq!(ids, vec![format!("m1/{ALICE}"), format!("m2/{ALICE}")]);
    assert!(resp.entries.iter().all(|e| e.recipient == ALICE));
}

/// The declared `(spec.method_collection).order_by` is `delivery_id`, and a
/// paginated read whose order is whatever a map iterated in is not paginated.
#[tokio::test]
async fn list_held_mail_is_ordered_by_delivery_id() {
    let h = Harness::start().await;
    for m in ["m9", "m3", "m5", "m1"] {
        h.deliver(delivery(m, ALICE, TENANT), 1_700_000_100_000).await;
    }

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    let ids: Vec<String> = resp.entries.iter().map(|e| e.delivery_id.clone()).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

/// Retrieved mail leaves the held list. That is the whole contract of the pair.
#[tokio::test]
async fn retrieved_mail_leaves_the_held_list() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    h.wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve");

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    assert!(resp.entries.is_empty());
}

// ---------------------------------------------------------------------------
// UpdateHeldMail
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_held_mail_marks_retrieved_and_stamps_the_clock() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    let before = now_ms();

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve")
        .into_inner();

    assert_eq!(resp.retrieved_delivery_ids, vec![format!("m1/{ALICE}")]);
    assert!(resp.retrieved_at_ms >= before);

    let row = h.row(&format!("m1/{ALICE}")).unwrap();
    assert_eq!(row.status, MailboxEntryStatus::Retrieved as i32);
    assert_eq!(row.retrieved_at_ms, resp.retrieved_at_ms);

    // The retrieval replaced the row in the store, and Watch delivers it after
    // the ADD that held it.
    let watched = h.watched();
    assert_eq!(watched.len(), 2);
    assert_eq!(watched[1].r#type, RecordChangeType::Modify as i32);
    assert!(watched[1].revision > watched[0].revision);
    assert_eq!(watched[1].item.as_ref(), Some(&row));
}

/// IDEMPOTENT: a second call is a no-op, returns the id as NOT newly retrieved,
/// and — the part that matters — does not rewrite `retrieved_at_ms`.
#[tokio::test]
async fn a_second_retrieval_is_a_no_op_and_does_not_move_the_clock() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    let id = format!("m1/{ALICE}");

    let first = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("first")
        .into_inner();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let second = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("second — an already-acked id must not be an error")
        .into_inner();

    assert!(second.retrieved_delivery_ids.is_empty());
    assert_eq!(second.retrieved_at_ms, 0);
    assert_eq!(
        h.row(&id).unwrap().retrieved_at_ms,
        first.retrieved_at_ms,
        "the FIRST retrieval's timestamp stands"
    );
}

/// An id naming somebody else's delivery is absent from the response, not an
/// error — and above all it is not retrieved.
#[tokio::test]
async fn another_recipients_delivery_is_not_retrievable() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", BOB, TENANT), 1_700_000_100_000)
        .await;

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{BOB}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("call")
        .into_inner();

    assert!(resp.retrieved_delivery_ids.is_empty());
    assert_eq!(
        h.row(&format!("m1/{BOB}")).unwrap().status,
        MailboxEntryStatus::Held as i32
    );
}

/// An unknown id is a no-op, not an error: a client cannot tell "already swept
/// away" from "never existed" and should not have to.
#[tokio::test]
async fn an_unknown_delivery_id_is_not_an_error() {
    let h = Harness::start().await;
    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec!["nope/user:alice".to_string()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("call")
        .into_inner();
    assert!(resp.retrieved_delivery_ids.is_empty());
}

// ---------------------------------------------------------------------------
// The PRINCIPAL paired control — generated, not re-implemented here
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recipient_is_stamped_from_the_verified_caller() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    // The request names no recipient at all; the wrapper stamps it.
    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].recipient, ALICE);
}

/// A caller naming SOMEONE ELSE as the recipient is refused before the handler
/// runs. This is `(spec.principal)`'s third case, and it is the one that makes
/// "read my mail" unable to mean "read Bob's".
#[tokio::test]
async fn a_spoofed_recipient_is_refused() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", BOB, TENANT), 1_700_000_100_000)
        .await;

    let err = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest {
                recipient: BOB.to_string(),
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("naming another principal must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn a_spoofed_recipient_is_refused_on_the_write_path_too() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", BOB, TENANT), 1_700_000_100_000)
        .await;

    let err = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                recipient: BOB.to_string(),
                delivery_ids: vec![format!("m1/{BOB}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("naming another principal must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        h.row(&format!("m1/{BOB}")).unwrap().status,
        MailboxEntryStatus::Held as i32
    );
}

#[tokio::test]
async fn an_unauthenticated_call_is_refused() {
    let h = Harness::start().await;
    let err = h
        .wrapper
        .list_held_mail(Harness::unsigned(ListHeldMailRequest::default()))
        .await
        .expect_err("no caller must be refused");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ---------------------------------------------------------------------------
// The NAMESPACE paired control — the same rigor item 5 applied
// ---------------------------------------------------------------------------

/// WRITE/INGEST half, stamp: an empty namespace field is filled from the token.
#[tokio::test]
async fn namespace_is_stamped_from_the_verified_caller() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].namespace, TENANT);
}

/// accept: a namespace that MATCHES the caller's claim is taken unchanged.
#[tokio::test]
async fn a_matching_namespace_is_accepted() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest {
                namespace: TENANT.to_string(),
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(resp.entries.len(), 1);
}

/// refuse: a namespace that DIFFERS is INVALID_ARGUMENT, never a silent
/// overwrite — on both RPCs, because the read path is as much a tenant boundary
/// as the write path.
#[tokio::test]
async fn a_mismatched_namespace_is_refused_on_read() {
    let h = Harness::start().await;
    let err = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest {
                namespace: OTHER_TENANT.to_string(),
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("a namespace the caller does not hold must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        h.stratum.routes().is_empty(),
        "a refused namespace reaches no tenant's Stratum"
    );
}

#[tokio::test]
async fn a_mismatched_namespace_is_refused_on_write() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let err = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                namespace: OTHER_TENANT.to_string(),
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("a namespace the caller does not hold must be refused");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        h.row(&format!("m1/{ALICE}")).unwrap().status,
        MailboxEntryStatus::Held as i32,
        "nothing may be written under a refused namespace"
    );
    assert_eq!(
        h.stratum.routes(),
        vec![("insert_if_absent", tid(TENANT).unwrap())],
        "a refused namespace reaches no tenant's Stratum"
    );
}

/// A row stamped with ANOTHER tenant is not returned to this caller: it is in
/// the other tenant's Stratum, and the read is routed to the caller's.
///
/// The service's own namespace predicate is the second line, for a row of
/// another namespace in the SAME tenant (the test after this one). It asserts the
/// SERVICE's predicate, not the generated backstop — and the
/// difference is why the assertion is worth making. `(spec.store_namespace)`'s
/// generated filter ABORTS THE PROCESS when it has to drop a row, so if this
/// service's own namespace predicate were missing, this test would not fail with
/// a bad assertion: it would take the test process down. Passing normally is the
/// proof that the handler filtered correctly and the backstop never had to fire.
#[tokio::test]
async fn a_row_from_another_tenant_is_not_returned() {
    let h = Harness::start().await;
    // Same principal, different tenant. Only the namespace distinguishes them,
    // so nothing but the namespace check can be what excludes it.
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.deliver(delivery("m2", ALICE, OTHER_TENANT), 1_700_000_200_000)
        .await;

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].delivery_id, format!("m1/{ALICE}"));
}

/// One tenant's Stratum holds every namespace below it, so a row of another
/// namespace in the caller's tenant IS in the store the read reaches. The
/// service's predicate is what keeps it out (or the backstop would abort).
#[tokio::test]
async fn a_row_from_another_namespace_of_the_same_tenant_is_not_returned() {
    let h = Harness::start().await;
    let sub = format!("{TENANT}/team");
    h.deliver(delivery("m1", ALICE, &sub), 1_700_000_100_000)
        .await;
    assert!(h.stratum.stored_in(&tid(TENANT).unwrap(), &format!("m1/{ALICE}")).is_some());

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();
    assert!(resp.entries.is_empty());
}

/// The cross-tenant row cannot be RETRIEVED either, even naming its exact id.
#[tokio::test]
async fn a_row_from_another_tenant_is_not_retrievable() {
    let h = Harness::start().await;
    h.deliver(delivery("m2", ALICE, OTHER_TENANT), 1_700_000_200_000)
        .await;

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m2/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("call")
        .into_inner();

    assert!(resp.retrieved_delivery_ids.is_empty());
    assert_eq!(
        h.row(&format!("m2/{ALICE}")).unwrap().status,
        MailboxEntryStatus::Held as i32
    );
}

// ---------------------------------------------------------------------------
// Tenant routing: every record-plane call names the tenant of the record
// ---------------------------------------------------------------------------

/// Every call this service makes of the record plane is routed to the tenant
/// of the row it is about — never left empty, never another tenant's.
#[tokio::test]
async fn every_record_plane_call_is_routed_to_the_rows_tenant() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list");
    h.wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve");

    let tenant = tid(TENANT).unwrap();
    let routes = h.stratum.routes();
    let methods: Vec<&str> = routes.iter().map(|(m, _)| *m).collect();
    assert_eq!(
        methods,
        vec!["insert_if_absent", "list", "get", "replace_if_unchanged"]
    );
    for (method, routed) in &routes {
        assert_eq!(routed, &tenant, "{method} routed to the wrong tenant");
    }
}

/// A delivery for another tenant's recipient lands in THAT tenant's Stratum,
/// whatever tenant this service runs in, and is read and retrieved there.
#[tokio::test]
async fn a_cross_tenant_delivery_lands_in_the_recipient_tenants_stratum() {
    let h = Harness::start().await;
    let other = tid(OTHER_TENANT).unwrap();
    h.deliver(delivery("m1", BOB, OTHER_TENANT), 1_700_000_100_000)
        .await;

    let id = format!("m1/{BOB}");
    assert!(h.stratum.stored_in(&other, &id).is_some());
    assert!(h.stratum.stored_in(&tid(TENANT).unwrap(), &id).is_none());
    assert_eq!(h.stratum.routes(), vec![("insert_if_absent", other.clone())]);

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            BOB,
            OTHER_TENANT,
        ))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(resp.entries.len(), 1);

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            BOB,
            OTHER_TENANT,
        ))
        .await
        .expect("retrieve")
        .into_inner();
    assert_eq!(resp.retrieved_delivery_ids, vec![id]);
    assert!(h.stratum.routes().iter().all(|(_, t)| t == &other));
}

/// A delivery whose namespace is in no tenant has no tenant's Stratum to be
/// held in: it is dropped rather than retried, and nothing is called with an
/// empty or made-up tenant.
#[tokio::test]
async fn a_delivery_in_no_tenant_is_dropped_not_routed() {
    let h = Harness::start().await;
    for namespace in ["", "/system", "/acme/eng", "/tenants/Not-A-Tid"] {
        h.deliver(delivery("m1", ALICE, namespace), 1_700_000_100_000)
            .await;
    }
    assert!(h.rows().is_empty());
    assert!(h.stratum.routes().is_empty());
}

/// A caller in no tenant has an empty mailbox, and asking reaches no store.
#[tokio::test]
async fn a_caller_in_no_tenant_reaches_no_store() {
    let h = Harness::start().await;
    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            "/system",
        ))
        .await
        .expect("list")
        .into_inner();
    assert!(resp.entries.is_empty());

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            "/system",
        ))
        .await
        .expect("retrieve")
        .into_inner();
    assert!(resp.retrieved_delivery_ids.is_empty());
    assert!(h.stratum.routes().is_empty());
}

/// The store refuses a row outside the tenant it was routed to (tacplus#207),
/// and a call that names no tenant. Through the service neither can happen — the tenant is the row's own — so the seam is driven directly, to
/// pin the fake to the refusal the tests above rely on it making.
#[tokio::test]
async fn the_store_refuses_a_row_outside_the_routed_tenant() {
    let h = Harness::start().await;
    let mut row = MailboxEntry {
        delivery_id: format!("m1/{ALICE}"),
        recipient: ALICE.to_string(),
        namespace: OTHER_TENANT.to_string(),
        ..Default::default()
    };
    let err = h
        .stratum
        .insert_if_absent(&tid(TENANT).unwrap(), row.clone())
        .await
        .expect_err("a row of another tenant");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    let err = h
        .stratum
        .insert_if_absent("", row.clone())
        .await
        .expect_err("no tenant");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    row.namespace = TENANT.to_string();
    h.stratum
        .insert_if_absent(&tid(TENANT).unwrap(), row)
        .await
        .expect("a row of the routed tenant");
}

// ---------------------------------------------------------------------------
// The store's answers — final and synchronous
// ---------------------------------------------------------------------------

/// While stratum's redo is unreachable a retrieval is REFUSED, and the caller is
/// told so: the mail is still held, and the next `ListHeldMail` says so.
#[tokio::test]
async fn a_retrieval_is_refused_while_redo_is_unreachable() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.stratum.refuse_writes(Some(tonic::Code::Unavailable));

    let err = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("a refused write must not be reported as saved");
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(h.watched().len(), 1, "only the ADD that held it");

    // Reads still answer: the rows are in stratum's memory.
    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();
    assert_eq!(resp.entries.len(), 1);
}

/// A durability level the store cannot serve yet (REMOTE, until
/// tacplus-dev/redo#1 step 2) is refused with FAILED_PRECONDITION, and that is
/// what the caller gets — never a retrieval reported as done.
#[tokio::test]
async fn a_refusal_for_an_unserved_durability_reaches_the_caller() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.stratum
        .refuse_writes(Some(tonic::Code::FailedPrecondition));

    let err = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![format!("m1/{ALICE}")],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect_err("a refused write must not be reported as saved");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        h.row(&format!("m1/{ALICE}")).unwrap().status,
        MailboxEntryStatus::Held as i32
    );
}

/// Two retrievals of one delivery race: the other one lands first, so this
/// one's replace is ABORTED. It reads the row again, finds it retrieved, and
/// leaves it — the FIRST retrieval's timestamp stands, and it is no error.
#[tokio::test]
async fn a_retrieval_that_loses_a_race_leaves_the_winner_standing() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    let id = format!("m1/{ALICE}");
    let mut winner = h.row(&id).unwrap();
    winner.status = MailboxEntryStatus::Retrieved as i32;
    winner.retrieved_at_ms = 1_700_000_500_000;
    h.stratum.interleave(winner);

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("losing a race is not an error")
        .into_inner();

    assert!(resp.retrieved_delivery_ids.is_empty());
    assert_eq!(h.row(&id).unwrap().retrieved_at_ms, 1_700_000_500_000);
}

/// An entry changed underneath in a way that leaves it HELD: the replace is
/// ABORTED, and the retrieval is made again on the row as it now stands.
#[tokio::test]
async fn a_retrieval_changed_underneath_is_retried_on_the_new_row() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    let id = format!("m1/{ALICE}");
    let mut other = h.row(&id).unwrap();
    other.subject = "standup, moved".to_string();
    h.stratum.interleave(other);

    let resp = h
        .wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve")
        .into_inner();

    assert_eq!(resp.retrieved_delivery_ids, vec![id.clone()]);
    let row = h.row(&id).unwrap();
    assert_eq!(row.status, MailboxEntryStatus::Retrieved as i32);
    assert_eq!(row.subject, "standup, moved", "the other write is not undone");
}

// ---------------------------------------------------------------------------
// Paging — the scan that replaced a whole-collection fetch
// ---------------------------------------------------------------------------

/// `ListHeldMail` pages the store until it has the rows it needs.
///
/// HELD-ness is not a `(spec.query_field)` and deliberately is not one, so the
/// status filter runs over pages rather than in the predicate: a recipient whose
/// mailbox is larger than one page must still get their held mail.
#[tokio::test]
async fn list_held_mail_pages_the_store() {
    let h = Harness::start().await;
    // More rows than one page of the scan, in an order that is not key order.
    for i in (0..450).rev() {
        h.deliver(
            delivery(&format!("m{i:04}"), ALICE, TENANT),
            1_700_000_100_000,
        )
        .await;
    }

    let resp = h
        .wrapper
        .list_held_mail(Harness::signed(
            ListHeldMailRequest::default(),
            ALICE,
            TENANT,
        ))
        .await
        .expect("list")
        .into_inner();

    // The declared page of 100, in key order, from the front of the mailbox.
    assert_eq!(resp.entries.len(), 100);
    assert_eq!(resp.entries[0].delivery_id, format!("m0000/{ALICE}"));
    assert_eq!(resp.entries[99].delivery_id, format!("m0099/{ALICE}"));
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Config is applied, and both of its fields are INERT.
///
/// `sweep_interval_seconds` paced a state-root sweep that no longer exists and
/// `retrieved_grace_seconds` deferred an eviction mark retired on 2026-09-20.
/// Neither is deleted from the proto because kernel sets them; this asserts that
/// delivery still works and that nothing in this service acts on either value.
#[tokio::test]
async fn config_is_applied_and_both_fields_are_inert() {
    let h = Harness::start().await;
    assert_eq!(h.svc.sweep_interval_seconds(), 300);

    h.svc.set_config(&mailbox::Config {
        sweep_interval_seconds: 30,
        retrieved_grace_seconds: 60,
        ..Default::default()
    });
    assert_eq!(h.svc.sweep_interval_seconds(), 30);

    // Nothing observable changed: no timer to retime, and no row touched.
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    assert_eq!(h.watched().len(), 1);
}

/// Gmail on without its key is refused, and the refusal names the env var the
/// operator's SecretEnvVars entry must set.
#[tokio::test]
async fn gmail_config_without_the_service_account_key_is_refused() {
    let h = Harness::start().await;
    let handler = crate::service::MailboxServiceHandler::new(h.svc.clone());
    let gmail = mailbox::GmailConfig {
        mailbox: "hello@example.test".to_string(),
        send_as: "tally@example.test".to_string(),
        send_as_name: "Tally".to_string(),
        watch_topic: "projects/p/topics/t".to_string(),
        ..Default::default()
    };

    let err = handler.apply_gmail(Some(&gmail), "").unwrap_err();

    assert!(
        err.to_string()
            .contains("PROTO_SERVICES_MAILBOX_SECRETS_GMAIL_SERVICE_ACCOUNT_JSON"),
        "{err}"
    );
}

/// Without `gmail` the service touches no Google API, key or not.
#[tokio::test]
async fn no_gmail_config_needs_no_key() {
    let h = Harness::start().await;
    let handler = crate::service::MailboxServiceHandler::new(h.svc.clone());

    handler.apply_gmail(None, "").unwrap();
}

/// The outbound-email kill switch (ops/datagate/README.md): a config delivery
/// without `gmail` stops the running Gmail task, and no Google endpoint is
/// called again. The key's token endpoint is a local listener that drops every
/// connection, so each watch attempt is one connection and then a retry after
/// `WATCH_RETRY`; paused time runs those retries without waiting.
#[tokio::test(start_paused = true)]
async fn gmail_off_stops_every_google_call() {
    let h = Harness::start().await;
    let handler = crate::service::MailboxServiceHandler::new(h.svc.clone());
    let google = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_uri = format!("http://{}/token", google.local_addr().unwrap());

    handler
        .apply_gmail(
            Some(&crate::gmail_tests::gmail_config()),
            &crate::gmail_tests::key_file(&token_uri),
        )
        .unwrap();
    // The first attempt, and the retry: the task is live and keeps calling.
    drop(google.accept().await.unwrap());
    drop(google.accept().await.unwrap());

    handler.apply_gmail(None, "").unwrap();

    let later = tokio::time::timeout(Duration::from_secs(24 * 3600), google.accept()).await;
    assert!(later.is_err(), "a Google call after gmail was switched off");
}
