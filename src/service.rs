//! The Mailbox service handler.
//!
//! Two jobs. There used to be three.
//!
//!   1. CONSUME. mailbox is a third consumer group on
//!      `messaging.MessageBus.stream`. Every delivery messaging publishes lands
//!      here as its own full copy — that fan-out is free and arranged entirely
//!      by `s2s_subscribe` on `messaging.Message`, so it appears nowhere in this
//!      file except as the handler method the generated `Handler` trait declares.
//!      On receipt this service publishes a HELD `MailboxEntry` keyed by the SAME
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
//! else — written by publishing onto the record's own topic, read back through
//! the generated `MailboxEntryRecordReader` that `(spec.record_read)` in
//! mailbox.proto pairs with the record's `s2s_read` grant. There is no mirror,
//! no cache and no second copy, so there is no ordering question between two
//! stores and no sweep: the third job this file used to have was pruning the
//! kernel state root, a bound that no longer exists (see
//! `Config.sweep_interval_seconds` in the proto — it is inert, and nothing
//! deletes a mailbox entry now).
//!
//! READ-YOUR-OWN-WRITE is the one thing the store does not give for free: a
//! record's write path is a publish and stratum's asynchronous ingest of it. So
//! `UpdateHeldMail` publishes and then POINT-READS THE ROW BACK until it sees
//! its own write, bounded to `VISIBILITY_TIMEOUT`, before it answers the caller.
//! That is what makes the `ListHeldMail` a client issues immediately afterwards
//! correct. The consume path deliberately does not wait — nothing reads those
//! rows back in the same breath, and waiting per message would put an ingest
//! round trip in front of every delivery.
//!
//! WHAT THIS SERVICE NEVER DOES: decide anything about a message. It copies
//! four header fields and an id. `messaging.Message` is the primitive with no
//! opinions and this is the index over it, so an opinion here would be an
//! opinion about somebody else's type.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::mailbox::MailboxHandler;
use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::sign_in_mail::SignInMailHandler;
use mailbox_model::types::generated::eventbus_support::eventbus_support::RecordMeta;
use mailbox_model::types::services::mailbox::helpers::secrets_from_env_var;
use mailbox_model::types::services::mailbox::mailbox::{
    Config as MailboxConfig, GmailConfig, SendSignInMailRequest, SendSignInMailResponse,
};
use mailbox_model::types::services::mailbox::{
    ListHeldMailRequest, ListHeldMailResponse, MailboxEntry, MailboxEntryStatus,
    UpdateHeldMailRequest, UpdateHeldMailResponse,
};
use mailbox_model::types::services::messaging::Message;
use mailbox_model::types::sys::healthreport::healthreport::InstanceStatus;
use mailbox_model::types::vservices::query::MailboxEntryFilter;

/// The two reads this service makes of its own rows.
///
/// A SEAM, and the same one letsencrypt's `certRecordStore` is: the record plane
/// is reachable only through a live client, the generated
/// `MailboxEntryRecordReader` BORROWS that client from whatever owns it, and the
/// full `RecordMap` server trait a test would otherwise have to stand up is 112
/// methods over every record in the fleet. So the shape of the read is declared
/// here, the production implementation below is the only place the generated
/// reader is named, and the tests substitute a store of their own.
///
/// Nothing wider than the two questions this service asks: give me this row, give
/// me the rows for this recipient.
#[async_trait::async_trait]
pub trait MailboxEntryRows: Send + Sync {
    /// One row by record key, or `None` when the store holds none. A miss is an
    /// ordinary answer: "no such delivery" and "the store is unreachable" must
    /// not look alike to the callers below.
    async fn get(&self, delivery_id: &str) -> Result<Option<MailboxEntry>, tonic::Status>;

    /// One page of rows matching `filter`, with the next page token (empty on the
    /// last page).
    async fn list(
        &self,
        filter: MailboxEntryFilter,
        page_size: i32,
        page_token: String,
    ) -> Result<(Vec<MailboxEntry>, String), tonic::Status>;
}

/// The production store: the record plane, through the generated reader.
///
/// Nothing here constructs a client, names stratum, opens or closes a query, or
/// acquires a token for the `query` audience. `(spec.record_read)` in
/// mailbox.proto is what caused the client to be generated and wired;
/// `sm.mailbox_entry_record_reader()` is the generated accessor; and the
/// three-call `Create -> List -> Delete` lifecycle (with the handle released on
/// the failure path too) is inside the reader.
pub struct RecordPlaneRows(pub Arc<mailbox_model::s2sm::ServiceManager>);

#[async_trait::async_trait]
impl MailboxEntryRows for RecordPlaneRows {
    async fn get(&self, delivery_id: &str) -> Result<Option<MailboxEntry>, tonic::Status> {
        self.0.mailbox_entry_record_reader().get(delivery_id).await
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
}

/// The publisher the generated ServiceManager hands over for the topic
/// synthesized from `MailboxEntry`'s `spec.record`
/// (`mailbox.MailboxEntryBus.stream`).
///
/// This is the ONLY write path a record has: a record declares no write RPC, and
/// reaches the store on stratum's own subscription to this topic. So publishing
/// here IS writing the row, and a publish this service cannot enqueue is a write
/// it did not perform — which is why, unlike the old two-store shape, a failure
/// from this closure is returned to the caller rather than logged.
pub type MailboxEntryPublisher = Arc<
    dyn Fn(MailboxEntry) -> Result<(), tokio::sync::mpsc::error::TrySendError<MailboxEntry>>
        + Send
        + Sync,
>;

/// How long a caller-facing write waits for its own row to become readable.
///
/// The publish hands the row to eventbus and stratum ingests it; Jody's
/// measure of that hop is milliseconds, so this is a generous ceiling rather
/// than an expected wait — the poll below returns as soon as the row is visible.
/// Exceeding it is reported as a FAILED SAVE: the publish will still land, but a
/// client told "retrieved" and then handed the same mail again by the next
/// `ListHeldMail` is the one outcome this must not produce.
///
/// If this ever proves too tight in practice the answer is not a bigger number
/// here: it is the synchronous publish-and-read helper the record plane would
/// grow, generically, per the 2026-09-27 decision.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(2);

/// How often the visibility poll re-reads. Short enough that the common case
/// costs one extra point read, long enough that a slow ingest is not a busy loop.
const VISIBILITY_POLL: Duration = Duration::from_millis(20);

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
    /// The record plane, for every read this service performs. Reads are
    /// authoritative by construction now: there is no local cache to be stale
    /// against, because the store is the only copy.
    rows: Arc<dyn MailboxEntryRows>,

    /// The record topic. The ONLY path by which a row leaves this service, and
    /// no EventBus client, topic name or token acquisition appears anywhere in
    /// this file because all three are inside this generated closure.
    publish: MailboxEntryPublisher,

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
    pub fn new(rows: Arc<dyn MailboxEntryRows>, publish: MailboxEntryPublisher) -> Self {
        Self {
            rows,
            publish,
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

    /// Enqueue one row onto the record topic.
    ///
    /// A `TrySendError` means the row was NOT accepted for delivery, which since
    /// there is no second store means the write did not happen. It is therefore
    /// an error to whoever asked for it, not a warning — the previous shape could
    /// swallow it because kernel state had already recorded the truth.
    fn publish_row(&self, entry: &MailboxEntry) -> Result<(), tonic::Status> {
        (self.publish)(entry.clone()).map_err(|e| {
            tracing::error!(
                error = %e,
                delivery_id = %entry.delivery_id,
                "mailbox entry publish queue full; the row was not written"
            );
            tonic::Status::unavailable("mailbox entry could not be queued for the record store")
        })
    }

    /// Publish a row and wait until the store will hand it back.
    ///
    /// `settled` decides what "back" means: the poll reads the row by key and
    /// accepts it once the predicate holds, so a caller says what it wrote rather
    /// than comparing whole rows (the stored copy carries `store_epoch_ms` and
    /// `spiffe_id` this service never set, so an equality test would never pass).
    async fn publish_and_confirm(
        &self,
        entry: &MailboxEntry,
        settled: impl Fn(&MailboxEntry) -> bool,
    ) -> Result<(), tonic::Status> {
        self.publish_row(entry)?;
        let poll = async {
            loop {
                if let Some(row) = self.rows.get(&entry.delivery_id).await? {
                    if settled(&row) {
                        return Ok(());
                    }
                }
                tokio::time::sleep(VISIBILITY_POLL).await;
            }
        };
        // tokio's timeout rather than a wall-clock deadline inside the loop: the
        // loop then has one exit for success and one for the bound, and a test can
        // drive the bound with paused time instead of waiting for it.
        tokio::time::timeout(VISIBILITY_TIMEOUT, poll)
            .await
            .unwrap_or_else(|_| {
                Err(tonic::Status::deadline_exceeded(
                    "the record store did not make this write readable in time",
                ))
            })
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
    /// every message this mailbox has ever seen. A row that already exists is
    /// left exactly as it is — which is also what stops a replay from writing a
    /// RETRIEVED row back to HELD.
    ///
    /// THE ONE WINDOW WHERE THAT IS NOT AIRTIGHT, stated rather than hidden: the
    /// existence check is a point read of the store, and a retrieval's own write
    /// is visible only after stratum's ingest. `UpdateHeldMail` does not return
    /// until that has happened, so a client that has been told "retrieved" is
    /// safe; but a replay landing inside that sub-second window, between the
    /// retrieval's publish and its visibility, would find no row and re-hold the
    /// delivery. The user-visible consequence is seeing one piece of mail twice,
    /// and acking it again is a no-op. Closing it properly needs the record
    /// plane's synchronous publish-and-read, which the 2026-09-27 decision holds
    /// in reserve rather than builds preemptively.
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

        if self.rows.get(&msg.delivery_id).await?.is_some() {
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
            // store_epoch_ms and spiffe_id are STAMPED — by the generated
            // wrapper and by eventbus respectively — and writing either here
            // would be this service asserting a value nothing verified.
            store_epoch_ms: 0,
            spiffe_id: String::new(),
            // namespace is COPIED FROM THE DELIVERY, not re-derived. The tenant
            // of a mailbox entry is the tenant of the message it tracks; taking
            // it from this service's own token would stamp the SERVICE's tenant
            // onto a user's row and make it unreadable by the person it is for.
            namespace: msg.namespace,
            ..Default::default()
        };

        // NOT `publish_and_confirm`: nothing reads this row back in the same
        // breath, and an ingest round trip per delivery would be paid on every
        // message of every replay. The cost is the replay window documented
        // above, not a lost row — the publisher retries until the broker takes it.
        self.publish_row(&entry)?;
        self.held.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(delivery_id = %entry.delivery_id, "mail held");
        Ok(())
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
            // A point read per id rather than one scan: the ids are the page
            // ListHeldMail just handed out (bounded at 100), and a keyed read is
            // what the plane is good at.
            let Some(entry) = self.rows.get(delivery_id).await? else {
                // An id naming no row is absent from the response, not an error.
                continue;
            };
            // Not the caller's row: skipped exactly as an unknown id is. The
            // (spec.principal) binding above means `req.recipient` IS the
            // caller, so this is not a second authorization decision — it is the
            // lookup being scoped to the mailbox the RPC is about.
            if entry.recipient != req.recipient || entry.namespace != req.namespace {
                continue;
            }
            // IDEMPOTENT. Already retrieved is a no-op and NOT an error: a
            // client polling on reconnect must not fail on ids it already acked,
            // and re-stamping retrieved_at_ms would rewrite when the recipient
            // took the mail every time it asked.
            if entry.status != MailboxEntryStatus::Held as i32 {
                continue;
            }

            let mut updated = entry.clone();
            updated.status = MailboxEntryStatus::Retrieved as i32;
            updated.retrieved_at_ms = now;
            // WAIT for it. This is the read-your-own-write the store does not
            // give for free, and the reason it is here and not on the consume
            // path: the very next thing a client does is re-list, and a row that
            // is still HELD in the store would hand it the same mail again.
            self.publish_and_confirm(&updated, |row| {
                row.status == MailboxEntryStatus::Retrieved as i32
            })
            .await?;
            self.retrieved.fetch_add(1, Ordering::Relaxed);
            retrieved_delivery_ids.push(delivery_id.clone());
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

/// SignInMail: the oauth service's one mail, a sign-in link (mailbox.proto).
///
/// Subject, body and sender are fixed here; the caller names only the
/// recipient and the link. Nothing is logged but the outcome: the link is a
/// credential and the address is the person.
#[derive(Default)]
pub struct SignInMailService {
    /// The adapter `apply_gmail` built from the current config; None while
    /// `Config.gmail` is unset.
    gmail: RwLock<Option<Arc<crate::gmail::GmailAdapter>>>,
}

impl SignInMailService {
    pub fn set_gmail(&self, gmail: Option<Arc<crate::gmail::GmailAdapter>>) {
        *self.gmail.write().unwrap_or_else(|e| e.into_inner()) = gmail;
    }
}

/// The sign-in mail for `link`, which works for `expires_in_seconds`.
pub fn sign_in_mail(to: &str, link: &str, expires_in_seconds: i64) -> crate::gmail::OutboundMail {
    let minutes = (expires_in_seconds + 59) / 60;
    crate::gmail::OutboundMail {
        to: to.to_string(),
        subject: "Your SeaBeam sign-in link".to_string(),
        body: format!(
            "Use this link to sign in to SeaBeam:\r\n\r\n{link}\r\n\r\n\
             It works once, within {minutes} minutes. If you did not ask to sign in, \
             ignore this mail: nothing happens without the link.\r\n"
        ),
        agent: None,
    }
}

#[tonic::async_trait]
impl SignInMailHandler for SignInMailService {
    async fn send_sign_in_mail(
        &self,
        _caller: Option<&servicemanager::auth::Caller>,
        req: SendSignInMailRequest,
    ) -> Result<SendSignInMailResponse, tonic::Status> {
        let gmail = self
            .gmail
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| tonic::Status::failed_precondition("Config.gmail is unset: no mail path"))?;
        let id = gmail
            .send(&sign_in_mail(&req.to, &req.link, req.expires_in_seconds))
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "sign-in mail send failed");
                tonic::Status::unavailable("sign-in mail could not be sent")
            })?;
        tracing::info!(gmail_message_id = %id, "sign-in mail sent");
        Ok(SendSignInMailResponse::default())
    }
}

/// The generated `Handler` the ServiceManager registers, wrapping the service.
pub struct MailboxServiceHandler {
    svc: Arc<MailboxService>,
    sign_in_mail: Arc<SignInMailService>,
    /// The task keeping the Gmail watch alive (`gmail::keep_watch`), when
    /// `Config.gmail` is set. Replaced on every config delivery.
    gmail_watch: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MailboxServiceHandler {
    pub fn new(svc: Arc<MailboxService>) -> Self {
        Self {
            svc,
            sign_in_mail: Arc::new(SignInMailService::default()),
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
        self.sign_in_mail.set_gmail(None);
        let Some(gmail) = gmail else {
            return Ok(());
        };
        if service_account_json.is_empty() {
            return Err("Config.gmail is set but PROTO_SERVICES_MAILBOX_SECRETS_GMAIL_SERVICE_ACCOUNT_JSON is empty".into());
        }
        let adapter = Arc::new(crate::gmail::GmailAdapter::new(
            gmail,
            service_account_json,
            crate::gmail::GMAIL_API,
        )?);
        self.sign_in_mail.set_gmail(Some(adapter.clone()));
        *running = Some(tokio::spawn(crate::gmail::keep_watch(adapter)));
        Ok(())
    }
}

#[async_trait::async_trait]
impl mailbox_model::s2sm::handler::Handler for MailboxServiceHandler {
    fn mailbox_grpc_impl(&self) -> Arc<dyn MailboxHandler> {
        self.svc.clone()
    }

    fn sign_in_mail_grpc_impl(&self) -> Arc<dyn SignInMailHandler> {
        self.sign_in_mail.clone()
    }

    /// The same Config `mailbox_config` is handed, which has already applied
    /// its Gmail half for both services; applying it twice would restart the
    /// watch twice.
    async fn sign_in_mail_config(&self, _cfg: &MailboxConfig) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
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
