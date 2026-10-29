//! The Mailbox service handler.
//!
//! Two jobs. There used to be three.
//!
//!   1. CONSUME. mailbox is a third consumer group on
//!      `messaging.MessageBus.stream`. Every delivery messaging publishes lands
//!      here as its own full copy — that fan-out is free and arranged entirely
//!      by `s2s_subscribe` on `messaging.Message`, so it appears nowhere in this
//!      file except as the handler method the generated `Handler` trait declares.
//!      On receipt this service writes a HELD `MailboxEntry` keyed by the SAME
//!      `delivery_id`, holding no message body.
//!
//!   2. SERVE. `ListHeldMail` answers "do I have new mail"; `UpdateHeldMail`
//!      marks deliveries retrieved. Neither returns a body: `messaging.Message`
//!      already owns and serves that, with its own c2s query plane, and
//!      duplicating it here would create a second source of truth for content
//!      this service has no business owning.
//!
//! WHAT WENT AWAY on 2026-09-27, and it is the whole shape of this file:
//! `MailboxState` in kernel. Rows are `MailboxEntry` in stratum and nothing
//! else. There is no mirror, no cache and no second copy, so there is no
//! ordering question between two stores and no sweep: the third job this file
//! used to have was pruning the kernel state root, a bound that no longer exists
//! (see `Config.sweep_interval_seconds` in the proto — it is inert, and nothing
//! deletes a mailbox entry now).
//!
//! HOW A ROW IS WRITTEN, since 2026-10-29 (#7): through stratum's generated
//! write surface (tacplus 2.5), not by publishing on the record's topic. Stratum
//! no longer ingests from the bus (tacplus-dev/stratum#2), and its answer is
//! final and synchronous: done, acked at the record's durability (LOCAL: on the
//! local redo's disk), or refused with the reason. That removed two things this
//! file used to carry. The read-back poll that made `UpdateHeldMail` wait for
//! stratum's asynchronous ingest is gone, because a write acked is a write
//! readable. And the consume path's replay window is closed: a delivery is held
//! with `InsertIfAbsent`, so a replay can never write over a retrieval.
//!
//! WHAT THIS SERVICE NEVER DOES: decide anything about a message. It copies
//! four header fields and an id. `messaging.Message` is the primitive with no
//! opinions and this is the index over it, so an opinion here would be an
//! opinion about somebody else's type.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::mailbox::MailboxHandler;
use mailbox_model::types::generated::eventbus_support::eventbus_support::RecordMeta;
use mailbox_model::types::services::mailbox::helpers::secrets_from_env_var;
use mailbox_model::types::services::mailbox::mailbox::{Config as MailboxConfig, GmailConfig};
use mailbox_model::types::services::mailbox::{
    ListHeldMailRequest, ListHeldMailResponse, MailboxEntry, MailboxEntryStatus,
    UpdateHeldMailRequest, UpdateHeldMailResponse,
};
use mailbox_model::types::services::messaging::Message;
use mailbox_model::types::sys::healthreport::healthreport::InstanceStatus;
use mailbox_model::s2sm::internal::grpc::clients::vservices::query::record_map::RecordMapGrpcClient;
use mailbox_model::types::vservices::query::{
    replace_if_unchanged_mailbox_entry_request, GetMailboxEntryRequest,
    InsertIfAbsentMailboxEntryRequest, MailboxEntryFilter, ReplaceIfUnchangedMailboxEntryRequest,
};

/// The reads and writes this service makes of its own rows.
///
/// A SEAM, and the same one letsencrypt's `certRecordStore` is: the record plane
/// is reachable only through a live client, and the full `RecordMap` server trait
/// a test would otherwise have to stand up covers every record in the fleet. So
/// the shape of the store is declared here, the production implementation below
/// is the only place the generated clients are named, and the tests substitute a
/// store of their own.
///
/// Nothing wider than what this service asks: this row, the rows for this
/// recipient, hold this delivery, retrieve this row.
#[async_trait::async_trait]
pub trait MailboxEntryStore: Send + Sync {
    /// One row of the tenant `namespace` by record key, with its revision, or
    /// `None` when the store holds none. A miss is an ordinary answer: "no such
    /// delivery" and "the store is unreachable" must not look alike to the
    /// callers below.
    async fn get(
        &self,
        delivery_id: &str,
        namespace: &str,
    ) -> Result<Option<(MailboxEntry, i64)>, tonic::Status>;

    /// One page of rows matching `filter`, with the next page token (empty on the
    /// last page).
    async fn list(
        &self,
        filter: MailboxEntryFilter,
        page_size: i32,
        page_token: String,
    ) -> Result<(Vec<MailboxEntry>, String), tonic::Status>;

    /// Write `entry` unless its key exists: its revision, or ALREADY_EXISTS.
    async fn insert_if_absent(&self, entry: MailboxEntry) -> Result<i64, tonic::Status>;

    /// Replace the row `entry` keys if it is still at `expected_revision`: the
    /// new revision, ABORTED when it changed underneath, NOT_FOUND when absent.
    async fn replace_if_unchanged(
        &self,
        entry: MailboxEntry,
        expected_revision: i64,
    ) -> Result<i64, tonic::Status>;
}

/// The production store: stratum's record plane, through the generated client.
///
/// Nothing here constructs a client, names an address, or acquires a token:
/// `(spec.record_read)` in mailbox.proto is what caused the client to be
/// generated and wired, and `MailboxEntry`'s `s2s_read`/`s2s_write` grants are
/// what let this service call it. Writes ask for no durability, so the generated
/// wrapper on stratum's side holds them to the record's declared floor (LOCAL).
pub struct RecordPlaneStore(pub Arc<mailbox_model::s2sm::ServiceManager>);

impl RecordPlaneStore {
    fn client(&self) -> &RecordMapGrpcClient {
        self.0.s2s().stratum().record_map_client()
    }
}

#[async_trait::async_trait]
impl MailboxEntryStore for RecordPlaneStore {
    async fn get(
        &self,
        delivery_id: &str,
        namespace: &str,
    ) -> Result<Option<(MailboxEntry, i64)>, tonic::Status> {
        let resp = self
            .client()
            .get_mailbox_entry(GetMailboxEntryRequest {
                delivery_id: delivery_id.to_string(),
                namespace: namespace.to_string(),
                ..Default::default()
            })
            .await;
        match resp {
            Ok(resp) => {
                let resp = resp.into_inner();
                Ok(resp.item.map(|item| (item, resp.revision)))
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status),
        }
    }

    async fn list(
        &self,
        filter: MailboxEntryFilter,
        page_size: i32,
        page_token: String,
    ) -> Result<(Vec<MailboxEntry>, String), tonic::Status> {
        let (rows, next, _total) = self
            .0
            .mailbox_entry_record_reader()
            .list(Some(filter), page_size, page_token)
            .await?;
        Ok((rows, next))
    }

    async fn insert_if_absent(&self, entry: MailboxEntry) -> Result<i64, tonic::Status> {
        Ok(self
            .client()
            .insert_if_absent_mailbox_entry(InsertIfAbsentMailboxEntryRequest {
                item: Some(entry),
                ..Default::default()
            })
            .await?
            .into_inner()
            .revision)
    }

    async fn replace_if_unchanged(
        &self,
        entry: MailboxEntry,
        expected_revision: i64,
    ) -> Result<i64, tonic::Status> {
        Ok(self
            .client()
            .replace_if_unchanged_mailbox_entry(ReplaceIfUnchangedMailboxEntryRequest {
                item: Some(entry),
                expected: Some(replace_if_unchanged_mailbox_entry_request::Expected::ExpectedRevision(
                    expected_revision,
                )),
                ..Default::default()
            })
            .await?
            .into_inner()
            .revision)
    }
}

/// How many times one retrieval re-reads its row after an ABORTED replace.
///
/// ABORTED means another write landed on the entry between this call's read and
/// its replace — in practice a second retrieval of the same delivery racing this
/// one. One re-read settles that (the row is now RETRIEVED and is left alone);
/// the bound is only so that a row rewritten continuously cannot hold a caller
/// forever, and running out of it is reported as the store's ABORTED.
const REPLACE_ATTEMPTS: usize = 3;

/// Rows per page when scanning a recipient's mailbox, and the cap on how many
/// pages one `ListHeldMail` will walk.
///
/// The predicate the plane can be given is `recipient` (the record's only EQ
/// criterion that names a person); HELD-ness is not a `(spec.query_field)` and
/// deliberately is not one, so the status filter is applied here, over pages. A
/// recipient with a long retrieved history therefore needs more than one page to
/// yield 100 held rows, and the page cap is what keeps that read bounded instead
/// of walking a mailbox of any size. Hitting it means a caller sees fewer than
/// the rows that exist, which is the same thing pagination already means.
const SCAN_PAGE_SIZE: i32 = 200;
const MAX_SCAN_PAGES: usize = 25;

pub struct MailboxService {
    /// The record store, for every read and write this service performs. Reads
    /// are authoritative by construction: there is no local cache to be stale
    /// against, because the store is the only copy.
    rows: Arc<dyn MailboxEntryStore>,

    /// Kernel-delivered config, held so a change in kernel takes effect with no
    /// redeploy. `sweep_interval_seconds` is the only field left and it is inert
    /// (see the proto): it is retained here so that `mailbox_config` has
    /// something truthful to log, and read by nothing.
    sweep_interval_seconds: RwLock<i32>,

    /// Counters, for the shutdown line. Deliberately not per-recipient: nothing
    /// in this service logs a subject, a body or a principal.
    held: AtomicI64,
    retrieved: AtomicI64,
}

impl MailboxService {
    pub fn new(rows: Arc<dyn MailboxEntryStore>) -> Self {
        Self {
            rows,
            // Overwritten by the first `mailbox_config` call before the server
            // accepts traffic; the generated SM delivers config first. The
            // default matches spec.default_int in the proto.
            sweep_interval_seconds: RwLock::new(300),
            held: AtomicI64::new(0),
            retrieved: AtomicI64::new(0),
        }
    }

    pub fn set_config(&self, cfg: &MailboxConfig) {
        if let Ok(mut w) = self.sweep_interval_seconds.write() {
            *w = cfg.sweep_interval_seconds;
        }
        // Neither config field is read for behaviour any more:
        // `sweep_interval_seconds` paced a state-root sweep that no longer
        // exists, and `retrieved_grace_seconds` deferred an eviction mark
        // retired on 2026-09-20. Both are documented as inert in the proto.
    }

    pub fn sweep_interval_seconds(&self) -> i32 {
        self.sweep_interval_seconds.read().map(|v| *v).unwrap_or(300)
    }

    pub fn counts(&self) -> (i64, i64) {
        (
            self.held.load(Ordering::Relaxed),
            self.retrieved.load(Ordering::Relaxed),
        )
    }

    /// Mark one row RETRIEVED, unless it already is: `true` when this call
    /// retrieved it.
    ///
    /// The replace is conditional on the revision this call read, so it can
    /// never write over a retrieval that landed in between. When one did
    /// (ABORTED), the row is read again and judged as it now stands; when it was
    /// deleted underneath (NOT_FOUND), there is nothing to retrieve. Every other
    /// refusal — UNAVAILABLE while stratum's redo is unreachable, above all — is
    /// the caller's answer: the row is still HELD and must not be reported
    /// otherwise.
    async fn retrieve(
        &self,
        delivery_id: &str,
        recipient: &str,
        namespace: &str,
        now: i64,
    ) -> Result<bool, tonic::Status> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let Some((entry, revision)) = self.rows.get(delivery_id, namespace).await? else {
                // An id naming no row is absent from the response, not an error.
                return Ok(false);
            };
            // Not the caller's row: skipped exactly as an unknown id is. The
            // (spec.principal) binding means `recipient` IS the caller, so this
            // is not a second authorization decision — it is the lookup being
            // scoped to the mailbox the RPC is about.
            if entry.recipient != recipient || entry.namespace != namespace {
                return Ok(false);
            }
            // IDEMPOTENT. Already retrieved is a no-op and NOT an error: a
            // client polling on reconnect must not fail on ids it already acked,
            // and re-stamping retrieved_at_ms would rewrite when the recipient
            // took the mail every time it asked.
            if entry.status != MailboxEntryStatus::Held as i32 {
                return Ok(false);
            }

            let mut updated = entry;
            updated.status = MailboxEntryStatus::Retrieved as i32;
            updated.retrieved_at_ms = now;
            match self.rows.replace_if_unchanged(updated, revision).await {
                Ok(_) => return Ok(true),
                Err(status) if status.code() == tonic::Code::NotFound => return Ok(false),
                Err(status)
                    if status.code() == tonic::Code::Aborted && attempt < REPLACE_ATTEMPTS =>
                {
                    continue
                }
                Err(status) => return Err(status),
            }
        }
    }

    /// Every row the store holds for one recipient, up to the page cap.
    ///
    /// The tenant is NOT part of the predicate: `namespace` on this record is
    /// deliberately not a `(spec.query_field)` (it exists to BE the egress
    /// boundary), and the record plane's S2S half leaves a `/system` peer's read
    /// unpinned — fleet-wide — so the caller's verified tenant is compared here,
    /// on rows, by the callers of this method.
    async fn rows_for_recipient(&self, recipient: &str) -> Result<Vec<MailboxEntry>, tonic::Status> {
        let mut rows = Vec::new();
        let mut page_token = String::new();
        for _ in 0..MAX_SCAN_PAGES {
            let filter = MailboxEntryFilter {
                recipient: Some(recipient.to_string()),
                ..Default::default()
            };
            let (items, next) = self
                .rows
                .list(filter, SCAN_PAGE_SIZE, page_token)
                .await?;
            rows.extend(items);
            if next.is_empty() {
                return Ok(rows);
            }
            page_token = next;
        }
        tracing::warn!(
            pages = MAX_SCAN_PAGES,
            "mailbox scan hit its page cap; a recipient's oldest rows were not examined"
        );
        Ok(rows)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The broker's timestamp for this record, or the wall clock when the meta
/// carries none.
///
/// The BROKER's, not this process's, and the difference is replay. EventBus
/// honours `start_from: earliest` with a real `seekToBeginning` on every
/// assignment, so a reconnect re-delivers the whole topic; taking the wall clock
/// would make `held_since_ms` — which is what "how long has this been sitting in
/// my mailbox" is answered from — jump forward every time a subscription
/// reconnected. The broker timestamp is a property of the delivery, so it is
/// stable across replays.
fn arrival_ms(meta: &RecordMeta) -> i64 {
    meta.timestamp
        .as_ref()
        .map(|t| t.seconds * 1000 + (t.nanos as i64) / 1_000_000)
        .filter(|ms| *ms > 0)
        .unwrap_or_else(now_ms)
}

impl MailboxService {
    /// Consume one delivery off `messaging.MessageBus.stream`.
    ///
    /// IDEMPOTENT BY CONSTRUCTION, because it has to be: `earliest` means the
    /// whole topic is re-delivered on every reconnect, so this runs again for
    /// every message this mailbox has ever seen. The row is written with
    /// `InsertIfAbsent`, so a row that already exists is left exactly as it is
    /// (the store answers ALREADY_EXISTS) — which is also what stops a replay
    /// from writing a RETRIEVED row back to HELD, with no window: the check and
    /// the write are one operation under the store's entry lock.
    ///
    /// A write the store refuses for any other reason (UNAVAILABLE while its
    /// redo is unreachable) is returned as an error, so the delivery is not
    /// acknowledged and the subscriber delivers it again.
    pub async fn on_message(
        &self,
        msg: Message,
        meta: &RecordMeta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if msg.delivery_id.is_empty() || msg.recipient.is_empty() {
            // Not retryable — returning Err would make the subscriber reconnect
            // and replay into the same row forever. A delivery with no key or no
            // addressee is not something this service can hold for anyone.
            tracing::error!(
                message_id = %msg.message_id,
                "message with no delivery_id or no recipient; not held"
            );
            return Ok(());
        }

        let entry = MailboxEntry {
            delivery_id: msg.delivery_id.clone(),
            message_id: msg.message_id,
            recipient: msg.recipient,
            status: MailboxEntryStatus::Held as i32,
            held_since_ms: arrival_ms(meta),
            retrieved_at_ms: 0,
            // The inbox summary line, and nothing else. `text`, `recipients` and
            // `ticket_id` are deliberately not carried: messaging owns the
            // message and serves it, and a second copy here would be a second
            // source of truth for content this service does not own.
            sender: msg.sender,
            subject: msg.subject,
            sent_at_ms: msg.sent_at_ms,
            // store_epoch_ms and spiffe_id are the store's to stamp, and writing
            // either here would be this service asserting a value nothing
            // verified.
            store_epoch_ms: 0,
            spiffe_id: String::new(),
            // namespace is COPIED FROM THE DELIVERY, not re-derived. The tenant
            // of a mailbox entry is the tenant of the message it tracks; taking
            // it from this service's own token would stamp the SERVICE's tenant
            // onto a user's row and make it unreadable by the person it is for.
            namespace: msg.namespace,
            ..Default::default()
        };

        let delivery_id = entry.delivery_id.clone();
        match self.rows.insert_if_absent(entry).await {
            Ok(_) => {
                self.held.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(delivery_id = %delivery_id, "mail held");
                Ok(())
            }
            // A replay: the row stands as it is, held or retrieved.
            Err(status) if status.code() == tonic::Code::AlreadyExists => Ok(()),
            Err(status) => {
                tracing::warn!(
                    code = ?status.code(),
                    delivery_id = %delivery_id,
                    "the record store refused the mailbox entry; the delivery will be redelivered"
                );
                Err(status.into())
            }
        }
    }
}

#[tonic::async_trait]
impl MailboxHandler for MailboxService {
    async fn list_held_mail(
        &self,
        _caller: Option<&servicemanager::auth::Caller>,
        req: ListHeldMailRequest,
    ) -> Result<ListHeldMailResponse, tonic::Status> {
        // recipient is (spec.principal) and namespace is (spec.namespace): the
        // generated wrapper has already bound BOTH to the authenticated caller,
        // stamping when empty, accepting when equal and refusing with
        // INVALID_ARGUMENT when they differ. Checking again here would be a
        // second, divergent copy of a decision the wrapper has already made.
        //
        // What this handler DOES have to do is filter on the same two values.
        // The wrapper filters the response too — `(spec.store_principal)`
        // silently, `(spec.store_namespace)` by ABORTING THE PROCESS — so a
        // predicate here that were wider than the caller's own row set would not
        // leak; it would crash. That is the backstop working, not a fallback to
        // rely on.
        let mut held: Vec<MailboxEntry> = self
            .rows_for_recipient(&req.recipient)
            .await?
            .into_iter()
            .filter(|e| e.status == MailboxEntryStatus::Held as i32)
            .filter(|e| e.recipient == req.recipient)
            .filter(|e| e.namespace == req.namespace)
            .collect();

        // The order the RPC declares in (spec.method_collection).order_by. A
        // paginated read whose order is whatever the store returned is not
        // paginated.
        held.sort_by(|a, b| a.delivery_id.cmp(&b.delivery_id));
        held.truncate(MAX_HELD_PAGE);

        Ok(ListHeldMailResponse {
            entries: held,
            ..Default::default()
        })
    }

    async fn update_held_mail(
        &self,
        _caller: Option<&servicemanager::auth::Caller>,
        req: UpdateHeldMailRequest,
    ) -> Result<UpdateHeldMailResponse, tonic::Status> {
        // One clock for the whole call, the same way messaging takes one
        // sent_at_ms for a whole submission: these ids were acknowledged by one
        // act and must not disagree about when.
        let now = now_ms();

        let mut retrieved_delivery_ids = Vec::new();
        for delivery_id in &req.delivery_ids {
            // A point read and a conditional replace per id rather than one
            // scan: the ids are the page ListHeldMail just handed out (bounded
            // at 100), and a keyed read is what the store is good at. No
            // read-back: the store answers once the write is acked, and an acked
            // write is what the client's very next ListHeldMail reads.
            if self
                .retrieve(delivery_id, &req.recipient, &req.namespace, now)
                .await?
            {
                self.retrieved.fetch_add(1, Ordering::Relaxed);
                retrieved_delivery_ids.push(delivery_id.clone());
            }
        }

        tracing::info!(
            requested = req.delivery_ids.len(),
            retrieved = retrieved_delivery_ids.len(),
            "mail retrieved"
        );

        Ok(UpdateHeldMailResponse {
            retrieved_at_ms: if retrieved_delivery_ids.is_empty() {
                0
            } else {
                now
            },
            retrieved_delivery_ids,
            ..Default::default()
        })
    }
}

/// The page `ListHeldMail` hands out — the same 100 declared in
/// `(spec.method_collection).max_items` and in `ListHeldMailResponse.entries`'
/// `(spec.assert_max_size)`. Exceeding it fails the generated RESPONSE
/// validation, which is an internal error, so the truncation is here.
const MAX_HELD_PAGE: usize = 100;

/// The generated `Handler` the ServiceManager registers, wrapping the service.
pub struct MailboxServiceHandler {
    svc: Arc<MailboxService>,
    /// The task keeping the Gmail watch alive (`gmail::keep_watch`), when
    /// `Config.gmail` is set. Replaced on every config delivery.
    gmail_watch: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MailboxServiceHandler {
    pub fn new(svc: Arc<MailboxService>) -> Self {
        Self {
            svc,
            gmail_watch: std::sync::Mutex::new(None),
        }
    }

    /// Stop any running watch and, when `gmail` is set, start one for it.
    ///
    /// `service_account_json` is `Secrets.gmail_service_account_json`; config
    /// that turns Gmail on without it is refused, naming the variable.
    pub fn apply_gmail(
        &self,
        gmail: Option<&GmailConfig>,
        service_account_json: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut running = self.gmail_watch.lock().map_err(|e| e.to_string())?;
        if let Some(task) = running.take() {
            task.abort();
        }
        let Some(gmail) = gmail else {
            return Ok(());
        };
        if service_account_json.is_empty() {
            return Err("Config.gmail is set but PROTO_SERVICES_MAILBOX_SECRETS_GMAIL_SERVICE_ACCOUNT_JSON is empty".into());
        }
        let adapter =
            crate::gmail::GmailAdapter::new(gmail, service_account_json, crate::gmail::GMAIL_API)?;
        *running = Some(tokio::spawn(crate::gmail::keep_watch(adapter)));
        Ok(())
    }
}

#[async_trait::async_trait]
impl mailbox_model::s2sm::handler::Handler for MailboxServiceHandler {
    fn mailbox_grpc_impl(&self) -> Arc<dyn MailboxHandler> {
        self.svc.clone()
    }

    async fn mailbox_config(&self, cfg: &MailboxConfig) -> Result<(), Box<dyn std::error::Error>> {
        // Delivered before the server accepts traffic, and again on every
        // change.
        self.svc.set_config(cfg);
        // Secret inputs come from the pod env (operator SecretEnvVars), never
        // from Config, which every config-holder in the fleet can read.
        let secrets = secrets_from_env_var(&std::env::vars().collect());
        self.apply_gmail(cfg.gmail.as_ref(), &secrets.gmail_service_account_json)?;
        tracing::info!(
            sweep_interval_seconds = cfg.sweep_interval_seconds,
            gmail = cfg.gmail.is_some(),
            "mailbox config applied (sweep fields are inert; see mailbox.proto)"
        );
        Ok(())
    }

    /// The consumer-group callback, named by tacplus from the topic ref. This
    /// method existing at all IS the third consumer group — nothing here names
    /// a group, a topic or an offset.
    async fn on_stream_messaging_message_bus_stream(
        &self,
        event: Message,
        meta: RecordMeta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.svc.on_message(event, &meta).await
    }

    fn check_health(&self) -> InstanceStatus {
        InstanceStatus::Healthy
    }

    async fn shutdown(&self) {
        let (held, retrieved) = self.svc.counts();
        tracing::info!(held, retrieved, "mailbox shutting down");
    }
}
