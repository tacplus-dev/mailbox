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
//!   - The store is a FAKE STRATUM behind the `MailboxEntryRows` seam, fed by the
//!     publisher: publishing a row "ingests" it, after a configurable lag, and
//!     only then does a read see it. That is the record pipeline's actual shape —
//!     a publish, an asynchronous ingest, a read — so the read-after-write the
//!     handler performs is exercised rather than assumed.
//!   - The consumer-group path goes in through `on_message` with a `RecordMeta`,
//!     which is what the generated subscriber hands the handler.
//!
//! WHY THE STORE IS A FAKE AND NOT THE GENERATED READER, since the previous shape
//! of this file DID stand up the generated `MailboxStateClient` over an
//! in-process server: the record plane's server trait (`query::RecordMap`) is 112
//! methods and 28 stream types, because it covers every `(spec.record)` in the
//! fleet. A fixture implementing it is not a fixture. So `RecordPlaneRows` — the
//! four lines that forward to the generated `MailboxEntryRecordReader` — is the
//! one part of the read path these tests do not cover, exactly as letsencrypt's
//! `recordStore` is the part its `certRecordStore` fake does not cover.
//!
//! WHAT THESE DO NOT PROVE. That a broker accepted a publish, or anything about
//! the record store's residency clock: `(spec.record).residency` is a declaration
//! the store acts on, with no runtime surface in this service to drive or
//! observe. There are no sweep tests any more either — the state root the sweep
//! pruned went away on 2026-09-27 and nothing replaced it.

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
use mailbox_model::types::vservices::query::MailboxEntryFilter;
use tokio::time::Instant;

use crate::service::{MailboxEntryRows, MailboxService};

/// The tenant every fixture token carries. Nothing asserts on the literal except
/// the namespace tests, which need a SECOND value to disagree with it.
const TENANT: &str = "/acme/eng";
const OTHER_TENANT: &str = "/acme/sales";

const ALICE: &str = "user:alice";
const BOB: &str = "user:bob";

// ---------------------------------------------------------------------------
// The fake stratum
// ---------------------------------------------------------------------------

/// One record's rows, with the ingest lag that makes this a record store rather
/// than a map.
///
/// `ingest` is what the harness's publisher calls — publishing IS writing, so the
/// fake's only writer is the topic. A row becomes readable `lag` after it is
/// published, which is the window `MailboxService::publish_and_confirm` exists to
/// close. `lag` defaults to zero so every test that is not about visibility reads
/// what it just wrote.
#[derive(Default)]
struct FakeStratum {
    /// delivery_id -> (row, the moment it becomes readable).
    rows: Mutex<HashMap<String, (MailboxEntry, Option<Instant>)>>,
    lag: Mutex<Duration>,
}

impl FakeStratum {
    fn set_lag(&self, lag: Duration) {
        *self.lag.lock().unwrap() = lag;
    }

    /// stratum's side of a publish.
    fn ingest(&self, entry: MailboxEntry) {
        let lag = *self.lag.lock().unwrap();
        let visible_at = if lag.is_zero() {
            None
        } else {
            Some(Instant::now() + lag)
        };
        self.rows
            .lock()
            .unwrap()
            .insert(entry.delivery_id.clone(), (entry, visible_at));
    }

    /// Every readable row, in key order — the order a store with an indexed key
    /// would page in, so `list` below can page deterministically.
    fn visible(&self) -> Vec<MailboxEntry> {
        let now = Instant::now();
        let mut rows: Vec<MailboxEntry> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|(_, visible_at)| visible_at.map_or(true, |t| now >= t))
            .map(|(row, _)| row.clone())
            .collect();
        rows.sort_by(|a, b| a.delivery_id.cmp(&b.delivery_id));
        rows
    }

    /// One row whether or not it is readable yet — for assertions about what was
    /// WRITTEN, which is not the same question as what a caller can see.
    fn stored(&self, delivery_id: &str) -> Option<MailboxEntry> {
        self.rows
            .lock()
            .unwrap()
            .get(delivery_id)
            .map(|(row, _)| row.clone())
    }
}

#[async_trait::async_trait]
impl MailboxEntryRows for FakeStratum {
    async fn get(&self, delivery_id: &str) -> Result<Option<MailboxEntry>, tonic::Status> {
        Ok(self
            .visible()
            .into_iter()
            .find(|r| r.delivery_id == delivery_id))
    }

    /// The criteria this fake honours are the ones `MailboxEntryFilter` carries
    /// and the service actually sets: `recipient`. Anything else the service
    /// started sending would silently not narrow here, which is why the
    /// namespace tests below assert on rows and not on the predicate.
    async fn list(
        &self,
        filter: MailboxEntryFilter,
        page_size: i32,
        page_token: String,
    ) -> Result<(Vec<MailboxEntry>, String), tonic::Status> {
        let matching: Vec<MailboxEntry> = self
            .visible()
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
    /// Every `MailboxEntry` the service published, in order — the record-side
    /// evidence of what was written, and in what sequence.
    published: Arc<Mutex<Vec<MailboxEntry>>>,
    /// The store the publisher feeds, so a test can read the rows the service
    /// wrote — the only way to assert on a field no response carries.
    stratum: Arc<FakeStratum>,
}

impl Harness {
    async fn start() -> Harness {
        let stratum = Arc::new(FakeStratum::default());
        let published: Arc<Mutex<Vec<MailboxEntry>>> = Arc::new(Mutex::new(Vec::new()));

        // The publisher IS the write path, so the fixture's publisher is what
        // feeds the store — a test cannot write a row the service did not
        // publish, which is the property the old two-store fixture could not have.
        let sink = published.clone();
        let ingest = stratum.clone();
        let svc = Arc::new(MailboxService::new(
            stratum.clone(),
            Arc::new(move |e: MailboxEntry| {
                sink.lock().unwrap().push(e.clone());
                ingest.ingest(e);
                Ok(())
            }),
        ));

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
            published,
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

    fn published(&self) -> Vec<MailboxEntry> {
        self.published.lock().unwrap().clone()
    }

    fn row(&self, delivery_id: &str) -> Option<MailboxEntry> {
        self.stratum.stored(delivery_id)
    }

    fn rows(&self) -> Vec<MailboxEntry> {
        self.stratum.visible()
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

    // The record went to the topic too — the store's copy.
    assert_eq!(h.published().len(), 1);
    assert_eq!(h.published()[0].delivery_id, row.delivery_id);
}

/// The service stamps NO identity and NO store epoch. Both are stamped for it —
/// `spiffe_id` by eventbus at publish, `store_epoch_ms` by the generated
/// wrapper — and a value written here would be a claim nothing verified.
#[tokio::test]
async fn the_service_stamps_no_identity_and_no_store_epoch() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;

    let published = h.published();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].spiffe_id, "");
    assert_eq!(published[0].store_epoch_ms, 0);
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
        h.published().len(),
        1,
        "a replay must not republish; the store's copy is already correct"
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
    assert!(h.published().is_empty());
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

    // The retrieval republished the row: the store's copy has to learn the new
    // status too.
    assert_eq!(h.published().len(), 2);
    assert_eq!(
        h.published()[1].status,
        MailboxEntryStatus::Retrieved as i32
    );
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
}

/// A row stamped with ANOTHER tenant is not returned to this caller.
///
/// This asserts the SERVICE's predicate, not the generated backstop — and the
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
// Read-after-write — what replaced the state root
// ---------------------------------------------------------------------------

/// A retrieval does not answer until the store will hand the row back.
///
/// THE POINT OF THE WHOLE MIGRATION, and the one thing kernel state used to give
/// for free. The store here takes 200ms to ingest; the handler must not return
/// before then, because the client's very next `ListHeldMail` would otherwise
/// still see the row as HELD and hand it the same mail again.
#[tokio::test(start_paused = true)]
async fn a_retrieval_waits_for_the_store_to_make_it_visible() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    h.stratum.set_lag(Duration::from_millis(200));
    let id = format!("m1/{ALICE}");

    let started = Instant::now();
    h.wrapper
        .update_held_mail(Harness::signed(
            UpdateHeldMailRequest {
                delivery_ids: vec![id.clone()],
                ..Default::default()
            },
            ALICE,
            TENANT,
        ))
        .await
        .expect("retrieve");

    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the rpc returned before its own write was readable"
    );

    // And the read that follows it is correct, which is what the wait buys.
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

/// A store that never makes the write readable is a FAILED SAVE, not a success.
///
/// Reported rather than swallowed: the publish will still land, but a client told
/// "retrieved" and then handed the same mail again is the outcome the wait exists
/// to prevent, so exceeding the bound has to be visible to the caller.
#[tokio::test(start_paused = true)]
async fn a_retrieval_that_never_becomes_visible_fails() {
    let h = Harness::start().await;
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    // Far beyond VISIBILITY_TIMEOUT. Paused time makes this instant.
    h.stratum.set_lag(Duration::from_secs(600));

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
        .expect_err("an unreadable write must not be reported as saved");
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);

    // The row WAS published — the publish is not undone by the wait failing, and
    // saying otherwise would be a lie about what the service did.
    assert_eq!(
        h.row(&format!("m1/{ALICE}")).unwrap().status,
        MailboxEntryStatus::Retrieved as i32
    );
}

/// The consume path does NOT wait. A delivery per ingest round trip would put
/// that latency in front of every message of every replay, and nothing reads
/// these rows back in the same breath.
#[tokio::test(start_paused = true)]
async fn the_consume_path_does_not_wait_for_ingest() {
    let h = Harness::start().await;
    h.stratum.set_lag(Duration::from_secs(600));

    let started = Instant::now();
    h.deliver(delivery("m1", ALICE, TENANT), 1_700_000_100_000)
        .await;
    assert!(started.elapsed() < Duration::from_millis(50));
    assert_eq!(h.published().len(), 1);
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
    assert_eq!(h.published().len(), 1);
}

/// Gmail on without its key is refused, and the refusal names the env var the
/// operator's SecretEnvVars entry must set.
#[tokio::test]
async fn gmail_config_without_the_service_account_key_is_refused() {
    let h = Harness::start().await;
    let handler = crate::service::MailboxServiceHandler::new(h.svc.clone());
    let gmail = mailbox::GmailConfig {
        mailbox: "hello@seabeam.test".to_string(),
        send_as: "tally@seabeam.test".to_string(),
        send_as_name: "Tally for SeaBeam".to_string(),
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
