//! Round-trip, paired-control and eviction-sweep tests for the Mailbox service.
//!
//! WHAT THESE DRIVE. Three seams, and the tests go through the real one in each
//! case rather than around it:
//!
//!   - The two RPCs go in through the GENERATED `ValidationWrapper`, with a
//!     signed bearer on the request metadata, exactly as a real client's call
//!     arrives. The `(spec.principal)` and `(spec.namespace)` stamps and the
//!     `(spec.store_principal)` / `(spec.store_namespace)` egress backstops are
//!     rendered into that wrapper and NOWHERE else, so a test that called the
//!     handler trait directly would quietly be asserting that this service
//!     re-implements them — which it deliberately does not.
//!   - The state reads and writes go through the GENERATED `MailboxStateClient`,
//!     which talks gRPC to whatever hosts `MailboxStateService` — kernel in
//!     production. The fixture here is an in-process server implementing that
//!     generated trait over one `HashMap`, and the service under test cannot
//!     tell the difference. This is scotty's fixture shape, for the same reason.
//!   - The consumer-group path goes in through `on_message` with a `RecordMeta`,
//!     which is what the generated subscriber hands the handler.
//!
//! WHAT THESE DO NOT PROVE. The publisher is a capturing closure standing in for
//! the generated EventBus publisher: it shows the real handler's real output —
//! the actual `MailboxEntry` values, the actual count, the actual `ejectionable`
//! transitions — but it cannot show that a broker accepted them, and it cannot
//! show that the record store acts on `ejectionable` at all (nothing in the
//! fleet does yet — see workers/big8-p1-mailbox-result.md §5).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::mailbox::{
    Authorizer, ValidationWrapper,
};
use mailbox_model::s2sm::internal::state::services::mailbox_state::state::MailboxStateClient;
use mailbox_model::types::generated::eventbus_support::eventbus_support::RecordMeta;
use mailbox_model::types::services::mailbox::mailbox_server::Mailbox;
use mailbox_model::types::services::mailbox::mailbox_state_service_server::{
    MailboxStateService, MailboxStateServiceServer,
};
use mailbox_model::types::services::mailbox::*;
use mailbox_model::types::services::messaging::Message;
use mailbox_model::types::sys::accounting::accounting::Event;
use mailbox_model::types::sys::spec::spec::Ejectionable;
use tonic::codegen::tokio_stream;
use tonic::transport::Channel;

use crate::service::MailboxService;

/// The tenant every fixture token carries. Nothing asserts on the literal except
/// the namespace tests, which need a SECOND value to disagree with it.
const TENANT: &str = "/acme/eng";
const OTHER_TENANT: &str = "/acme/sales";

const ALICE: &str = "user:alice";
const BOB: &str = "user:bob";

// ---------------------------------------------------------------------------
// The fake kernel
// ---------------------------------------------------------------------------

/// One collection, so this is written out rather than macro-generated the way
/// scotty's six collections are. `get_all_entries`, `put_entries` and
/// `delete_entries` are the only methods on any path this service takes; the
/// rest exist so the generated trait is satisfied and fail loudly rather than
/// returning a plausible empty value, so a test that starts depending on one
/// finds out instead of quietly reading nothing.
///
/// `watch_entries` returns a stream that never yields, which is what a healthy
/// idle Watch looks like — the client's reconnect loop stays parked rather than
/// spinning. It is also why this service reads with `fetch_all_entries` and
/// never with the locally cached `get_entries_map`: against this fixture the
/// cache would be permanently empty, and in production it would be merely
/// stale, which for "have I already handed out this mail" is the same bug.
#[derive(Default)]
struct FakeKernel {
    entries: Mutex<HashMap<String, MailboxEntry>>,
}

#[tonic::async_trait]
impl MailboxStateService for FakeKernel {
    async fn get_entries(
        &self,
        request: tonic::Request<EntriesGetRequest>,
    ) -> Result<tonic::Response<EntriesValueResponse>, tonic::Status> {
        let value = self
            .entries
            .lock()
            .unwrap()
            .get(&request.into_inner().key)
            .cloned();
        Ok(tonic::Response::new(EntriesValueResponse {
            found: value.is_some(),
            value,
            ..Default::default()
        }))
    }

    async fn put_entries(
        &self,
        request: tonic::Request<EntriesPutRequest>,
    ) -> Result<tonic::Response<EntriesValueResponse>, tonic::Status> {
        let req = request.into_inner();
        let value = req
            .value
            .ok_or_else(|| tonic::Status::invalid_argument("put with no value"))?;
        self.entries.lock().unwrap().insert(req.key, value.clone());
        Ok(tonic::Response::new(EntriesValueResponse {
            found: true,
            value: Some(value),
            ..Default::default()
        }))
    }

    async fn get_all_entries(
        &self,
        _request: tonic::Request<EntriesGetAllRequest>,
    ) -> Result<tonic::Response<EntriesEntriesResponse>, tonic::Status> {
        Ok(tonic::Response::new(EntriesEntriesResponse {
            entries: self.entries.lock().unwrap().clone(),
            ..Default::default()
        }))
    }

    async fn delete_entries(
        &self,
        request: tonic::Request<EntriesGetRequest>,
    ) -> Result<tonic::Response<MapEmpty>, tonic::Status> {
        self.entries
            .lock()
            .unwrap()
            .remove(&request.into_inner().key);
        Ok(tonic::Response::new(MapEmpty::default()))
    }

    async fn set_entries(
        &self,
        _request: tonic::Request<EntriesPutRequest>,
    ) -> Result<tonic::Response<MapEmpty>, tonic::Status> {
        Err(tonic::Status::unimplemented("set_entries"))
    }
    async fn remove_entries(
        &self,
        _request: tonic::Request<EntriesGetRequest>,
    ) -> Result<tonic::Response<EntriesValueResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("remove_entries"))
    }
    async fn contains_key_entries(
        &self,
        _request: tonic::Request<EntriesGetRequest>,
    ) -> Result<tonic::Response<EntriesValueResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("contains_key_entries"))
    }
    async fn key_set_entries(
        &self,
        _request: tonic::Request<EntriesKeysRequest>,
    ) -> Result<tonic::Response<EntriesKeysResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("key_set_entries"))
    }
    async fn size_entries(
        &self,
        _request: tonic::Request<EntriesSizeRequest>,
    ) -> Result<tonic::Response<EntriesSizeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("size_entries"))
    }
    async fn clear_entries(
        &self,
        _request: tonic::Request<EntriesClearRequest>,
    ) -> Result<tonic::Response<MapEmpty>, tonic::Status> {
        Err(tonic::Status::unimplemented("clear_entries"))
    }
    async fn put_if_absent_entries(
        &self,
        _request: tonic::Request<EntriesPutRequest>,
    ) -> Result<tonic::Response<EntriesValueResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("put_if_absent_entries"))
    }
    async fn query_entries(
        &self,
        _request: tonic::Request<EntriesQueryRequest>,
    ) -> Result<tonic::Response<EntriesQueryResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("query_entries"))
    }

    type WatchEntriesStream = tokio_stream::Pending<Result<EntriesEntriesResponse, tonic::Status>>;
    async fn watch_entries(
        &self,
        _request: tonic::Request<mailbox_model::types::sys::trace::trace::TraceRequest>,
    ) -> Result<tonic::Response<Self::WatchEntriesStream>, tonic::Status> {
        Ok(tonic::Response::new(tokio_stream::pending()))
    }

    type WatchQueryEntriesStream = tokio_stream::Pending<Result<EntriesQueryResponse, tonic::Status>>;
    async fn watch_query_entries(
        &self,
        _request: tonic::Request<EntriesQueryRequest>,
    ) -> Result<tonic::Response<Self::WatchQueryEntriesStream>, tonic::Status> {
        Ok(tonic::Response::new(tokio_stream::pending()))
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
    /// The service, reachable directly for the consumer-group and sweep tests
    /// (neither of which is an RPC) and through the wrapper for the RPCs.
    svc: Arc<MailboxService>,
    wrapper: ValidationWrapper,
    /// Every `MailboxEntry` the service published, in order. The record-side
    /// evidence: what the store would have been sent.
    published: Arc<Mutex<Vec<MailboxEntry>>>,
    /// The store itself, so a test can read the rows the service WROTE — the
    /// only way to assert on a field no response carries.
    kernel: Arc<FakeKernel>,
}

impl Harness {
    async fn start() -> Harness {
        // A port is claimed, released and immediately re-bound by tonic. The
        // connect loop below covers the gap rather than assuming it away.
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let kernel = Arc::new(FakeKernel::default());
        let served = kernel.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MailboxStateServiceServer::from_arc(served))
                .serve(addr)
                .await
                .unwrap();
        });

        let uri = format!("http://{addr}");
        let mut channel: Option<Channel> = None;
        for _ in 0..100 {
            match Channel::from_shared(uri.clone()).unwrap().connect().await {
                Ok(c) => {
                    channel = Some(c);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
        let channel = channel.expect("fake kernel never came up");

        let state = MailboxStateClient::new(channel, None).await.unwrap();
        let published: Arc<Mutex<Vec<MailboxEntry>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = published.clone();
        let svc = Arc::new(MailboxService::new(
            state,
            Arc::new(move |e: MailboxEntry| {
                sink.lock().unwrap().push(e);
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
            kernel,
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
        self.kernel.entries.lock().unwrap().get(delivery_id).cloned()
    }

    fn rows(&self) -> HashMap<String, MailboxEntry> {
        self.kernel.entries.lock().unwrap().clone()
    }

    /// Put a row straight into the store, bypassing the service. Used only to
    /// age a row past a clock the test cannot wait out.
    fn seed(&self, entry: MailboxEntry) {
        self.kernel
            .entries
            .lock()
            .unwrap()
            .insert(entry.delivery_id.clone(), entry);
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

    // HELD is NEVER-evict, stated rather than left at UNSPECIFIED.
    assert_eq!(row.ejectionable, Ejectionable::Never as i32);

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
/// and — the part that matters — does not push the eviction clock forward.
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
// The eviction sweep
// ---------------------------------------------------------------------------

/// A HELD row is NEVER-evict and the sweep does not touch it, at any age.
///
/// This is the decision doc's rejected alternative ("residency clock keyed off
/// held_since_ms") asserted as behaviour: the row below is 89 days old, older
/// than any plausible residency window, and still unretrieved.
#[tokio::test]
async fn the_sweep_never_marks_unretrieved_mail() {
    let h = Harness::start().await;
    let eighty_nine_days_ago = now_ms() - 89 * 24 * 60 * 60 * 1000;
    h.deliver(delivery("m1", ALICE, TENANT), eighty_nine_days_ago)
        .await;

    let outcome = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(outcome.marked_evictable, 0);
    assert_eq!(outcome.expired, 0);

    let row = h.row(&format!("m1/{ALICE}")).unwrap();
    assert_eq!(row.ejectionable, Ejectionable::Never as i32);
    assert_eq!(row.status, MailboxEntryStatus::Held as i32);
}

/// At the default grace of 0, retrieval marks the gate INLINE — the mark and the
/// retrieval are one write, so `(spec.store_epoch)` (what the residency window
/// is measured against) starts at the retrieval instant and nowhere else.
#[tokio::test]
async fn retrieval_marks_the_row_evictable_inline_at_zero_grace() {
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

    let row = h.row(&format!("m1/{ALICE}")).unwrap();
    assert_eq!(row.ejectionable, Ejectionable::Window as i32);

    // And the sweep then has nothing left to do.
    let outcome = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(outcome.marked_evictable, 0);
}

/// With a grace period configured, retrieval leaves the row NEVER-evict and the
/// SWEEP is what promotes it — once the grace has actually elapsed.
#[tokio::test]
async fn the_sweep_marks_a_retrieved_row_only_after_the_grace_period() {
    let h = Harness::start().await;
    h.svc.set_config(&mailbox::Config {
        sweep_interval_seconds: 300,
        retrieved_grace_seconds: 3600,
        ..Default::default()
    });
    // A RECENT arrival: the sweep also expires rows at 90 days, and a
    // fixture timestamp from 2023 would be swept away before this test could
    // look at it.
    h.deliver(delivery("m1", ALICE, TENANT), now_ms()).await;
    let id = format!("m1/{ALICE}");

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

    // Just retrieved: inside the grace, so still NEVER.
    assert_eq!(
        h.row(&id).unwrap().ejectionable,
        Ejectionable::Never as i32,
        "a grace period means the row stays resident across it"
    );
    assert_eq!(h.svc.sweep_once().await.expect("sweep").marked_evictable, 0);

    // Age the retrieval past the grace. The clock is retrieved_at_ms, exactly as
    // the decision doc rules — held_since_ms is untouched here and stays recent.
    let mut aged = h.row(&id).unwrap();
    aged.retrieved_at_ms = now_ms() - 2 * 3600 * 1000;
    h.seed(aged);

    let outcome = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(outcome.marked_evictable, 1);
    assert_eq!(h.row(&id).unwrap().ejectionable, Ejectionable::Window as i32);
    assert_eq!(
        h.row(&id).unwrap().status,
        MailboxEntryStatus::Retrieved as i32,
        "the sweep changes the GATE, not the status"
    );
}

/// The sweep is the reconciler for a mark that never landed — the case the
/// inline path cannot cover, because the process that would have marked it is
/// gone.
#[tokio::test]
async fn the_sweep_repairs_a_retrieved_row_whose_mark_was_lost() {
    let h = Harness::start().await;
    // A RECENT arrival: the sweep also expires rows at 90 days, and a
    // fixture timestamp from 2023 would be swept away before this test could
    // look at it.
    h.deliver(delivery("m1", ALICE, TENANT), now_ms()).await;
    let id = format!("m1/{ALICE}");

    // A row as an instance that died mid-retrieval would have left it:
    // RETRIEVED, timestamped, but never marked.
    let mut half_done = h.row(&id).unwrap();
    half_done.status = MailboxEntryStatus::Retrieved as i32;
    half_done.retrieved_at_ms = now_ms() - 60_000;
    half_done.ejectionable = Ejectionable::Never as i32;
    h.seed(half_done);

    let outcome = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(outcome.marked_evictable, 1);
    assert_eq!(h.row(&id).unwrap().ejectionable, Ejectionable::Window as i32);

    // The repaired row was republished, so the record store learns the gate too.
    let published = h.published();
    assert_eq!(
        published.last().unwrap().ejectionable,
        Ejectionable::Window as i32
    );
}

/// A RETRIEVED row with no timestamp cannot have its eligibility measured, so it
/// stays NEVER. The safe direction for a value that cannot be computed is "keep
/// the row" — the same reasoning EJECTIONABLE_UNSPECIFIED itself is built on.
#[tokio::test]
async fn a_retrieved_row_with_no_timestamp_stays_never_evict() {
    let h = Harness::start().await;
    // A RECENT arrival: the sweep also expires rows at 90 days, and a
    // fixture timestamp from 2023 would be swept away before this test could
    // look at it.
    h.deliver(delivery("m1", ALICE, TENANT), now_ms()).await;
    let id = format!("m1/{ALICE}");

    let mut broken = h.row(&id).unwrap();
    broken.status = MailboxEntryStatus::Retrieved as i32;
    broken.retrieved_at_ms = 0;
    h.seed(broken);

    assert_eq!(h.svc.sweep_once().await.expect("sweep").marked_evictable, 0);
    assert_eq!(h.row(&id).unwrap().ejectionable, Ejectionable::Never as i32);
}

/// THE ONE PLACE THIS SERVICE DELETES ANYTHING: a row that reached the record's
/// own 90-day retention leaves kernel state, so `MailboxState.entries` cannot
/// grow past the bound it declares. It is measured from held_since_ms, the same
/// clock `(spec.record).retention` uses, so the two stores expire a row together.
#[tokio::test]
async fn the_sweep_expires_rows_at_the_records_own_retention_age() {
    let h = Harness::start().await;
    let ninety_one_days_ago = now_ms() - 91 * 24 * 60 * 60 * 1000;
    h.deliver(delivery("m1", ALICE, TENANT), ninety_one_days_ago)
        .await;
    h.deliver(delivery("m2", ALICE, TENANT), now_ms()).await;

    let outcome = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(outcome.expired, 1);
    assert!(h.row(&format!("m1/{ALICE}")).is_none());
    assert!(h.row(&format!("m2/{ALICE}")).is_some());
}

/// The sweep is idempotent: a second pass over the same state changes nothing.
/// That is what makes running it on a plain timer, on every instance, without a
/// lock, correct.
#[tokio::test]
async fn the_sweep_is_idempotent() {
    let h = Harness::start().await;
    // A RECENT arrival: the sweep also expires rows at 90 days, and a
    // fixture timestamp from 2023 would be swept away before this test could
    // look at it.
    h.deliver(delivery("m1", ALICE, TENANT), now_ms()).await;
    let id = format!("m1/{ALICE}");
    let mut retrieved = h.row(&id).unwrap();
    retrieved.status = MailboxEntryStatus::Retrieved as i32;
    retrieved.retrieved_at_ms = now_ms() - 60_000;
    h.seed(retrieved);

    let first = h.svc.sweep_once().await.expect("sweep");
    let second = h.svc.sweep_once().await.expect("sweep");
    assert_eq!(first.marked_evictable, 1);
    assert_eq!(second.marked_evictable, 0);
    assert_eq!(second.expired, 0);
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// The sweep cadence is kernel config, re-read every pass, so a change takes
/// effect with no redeploy and no restart.
#[tokio::test]
async fn the_sweep_interval_comes_from_kernel_config() {
    let h = Harness::start().await;
    assert_eq!(h.svc.sweep_interval(), std::time::Duration::from_secs(300));
    h.svc.set_config(&mailbox::Config {
        sweep_interval_seconds: 30,
        retrieved_grace_seconds: 0,
        ..Default::default()
    });
    assert_eq!(h.svc.sweep_interval(), std::time::Duration::from_secs(30));
}
