//! The Mailbox service handler.
//!
//! Three jobs, and the third is the one nothing in the fleet had done before.
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
//!      already owns and serves that, with its own 90-day retention and its own
//!      c2s query plane, and duplicating it here would create a second source of
//!      truth for content this service has no business owning.
//!
//!   3. SWEEP. mailbox is the first consumer of `(spec.ejectionable)` (BIG GOAL
//!      8 Phase 0 item 4), which landed as declaration-and-refusal plumbing with
//!      no runtime consumer anywhere. See `sweep_once` for what "evict" turned
//!      out to mean once read against the generator rather than against prose.
//!
//! WHAT THIS SERVICE NEVER DOES: decide anything about a message. It copies
//! four header fields and an id. `messaging.Message` is the primitive with no
//! opinions and this is the index over it, so an opinion here would be an
//! opinion about somebody else's type.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mailbox_model::s2sm::internal::grpc::servers::services::mailbox::mailbox::MailboxHandler;
use mailbox_model::s2sm::internal::state::services::mailbox_state::state::MailboxStateClient;
use mailbox_model::types::generated::eventbus_support::eventbus_support::RecordMeta;
use mailbox_model::types::services::mailbox::mailbox::Config as MailboxConfig;
use mailbox_model::types::services::mailbox::{
    MailboxEntryStatus, ListHeldMailRequest, ListHeldMailResponse, MailboxEntry,
    UpdateHeldMailRequest, UpdateHeldMailResponse,
};
use mailbox_model::types::services::messaging::Message;
use mailbox_model::types::sys::healthreport::healthreport::InstanceStatus;
use mailbox_model::types::sys::spec::spec::Ejectionable;

/// The publisher the generated ServiceManager hands over for the topic
/// synthesized from `MailboxEntry`'s `spec.record`
/// (`mailbox.MailboxEntryBus.stream`).
///
/// Every row this service writes goes to TWO places and they are not
/// interchangeable: kernel state is what this service reads back (see
/// `MailboxState`'s comment in the proto for why it is not the record store),
/// and the topic is what carries the row to the record store, where the
/// residency window `ejectionable` gates actually lives.
pub type MailboxEntryPublisher = Arc<
    dyn Fn(MailboxEntry) -> Result<(), tokio::sync::mpsc::error::TrySendError<MailboxEntry>>
        + Send
        + Sync,
>;

/// How long a `MailboxEntry` row stays in kernel state, in milliseconds.
///
/// 90 days, and NOT a config knob: it is `(spec.record).retention` on
/// `MailboxEntry` restated in the one place that can act on it. Retention
/// deletes the row in the RECORD STORE; nothing prunes this service's own state
/// root, so the sweep does that half, from the same clock (`held_since_ms`) and
/// at the same age. Two stores expiring a row at two different ages would leave
/// an index entry pointing at a message that no longer exists, or the reverse.
const STATE_RETENTION_MS: i64 = 7_776_000_000;

pub struct MailboxService {
    /// The generated state client — already connected, already authorized,
    /// already watching, by the time this service is constructed. Every read
    /// below is `fetch_all_entries`, the authoritative gRPC read, rather than
    /// the locally cached `get_entries_map`: a mailbox answering "is this
    /// already retrieved" off a cache that a Watch has not caught up on would
    /// hand out mail twice.
    state: MailboxStateClient,

    /// The record topic. The ONLY path by which a row leaves this service, and
    /// no EventBus client, topic name or token acquisition appears anywhere in
    /// this file because all three are inside this generated closure.
    publish: MailboxEntryPublisher,

    /// Kernel-delivered config. Held rather than read as constants so a change
    /// in kernel changes behaviour with no redeploy.
    sweep_interval_seconds: RwLock<i32>,
    retrieved_grace_seconds: RwLock<i32>,

    /// Counters, for the shutdown line. Deliberately not per-recipient: nothing
    /// in this service logs a subject, a body or a principal.
    held: AtomicI64,
    retrieved: AtomicI64,
    marked_evictable: AtomicI64,
    expired: AtomicI64,
}

/// What one sweep pass did. Returned rather than only logged so the sweep can be
/// driven directly by a test and by an operator, not only by the timer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Rows whose `ejectionable` moved NEVER -> WINDOW because they are
    /// retrieved and past the grace period.
    pub marked_evictable: usize,
    /// Rows deleted from kernel state because they reached the record's own
    /// retention age.
    pub expired: usize,
    /// Rows examined.
    pub scanned: usize,
}

impl MailboxService {
    pub fn new(state: MailboxStateClient, publish: MailboxEntryPublisher) -> Self {
        Self {
            state,
            publish,
            // Overwritten by the first `mailbox_config` call before the server
            // accepts traffic; the generated SM delivers config first. The
            // defaults match spec.default_int in the proto.
            sweep_interval_seconds: RwLock::new(300),
            retrieved_grace_seconds: RwLock::new(0),
            held: AtomicI64::new(0),
            retrieved: AtomicI64::new(0),
            marked_evictable: AtomicI64::new(0),
            expired: AtomicI64::new(0),
        }
    }

    pub fn set_config(&self, cfg: &MailboxConfig) {
        if let Ok(mut w) = self.sweep_interval_seconds.write() {
            *w = cfg.sweep_interval_seconds;
        }
        if let Ok(mut w) = self.retrieved_grace_seconds.write() {
            *w = cfg.retrieved_grace_seconds;
        }
    }

    pub fn sweep_interval(&self) -> Duration {
        // A poisoned lock must not take the service down over a cadence.
        let secs = self
            .sweep_interval_seconds
            .read()
            .map(|v| *v)
            .unwrap_or(300)
            .max(1) as u64;
        Duration::from_secs(secs)
    }

    fn retrieved_grace_ms(&self) -> i64 {
        self.retrieved_grace_seconds
            .read()
            .map(|v| *v)
            .unwrap_or(0)
            .max(0) as i64
            * 1000
    }

    pub fn counts(&self) -> (i64, i64, i64, i64) {
        (
            self.held.load(Ordering::Relaxed),
            self.retrieved.load(Ordering::Relaxed),
            self.marked_evictable.load(Ordering::Relaxed),
            self.expired.load(Ordering::Relaxed),
        )
    }

    /// Write one row to both places, state first.
    ///
    /// STATE FIRST, deliberately. The state row is what `ListHeldMail` and
    /// `UpdateHeldMail` read, so it is the one that decides what a caller sees;
    /// the published row is the copy the record store keeps and the residency
    /// window applies to. If the publish fails, the caller-visible answer is
    /// already correct and the sweep republishes the row on its next pass — a
    /// row whose state says RETRIEVED but whose `ejectionable` is not yet WINDOW
    /// is exactly the sweep's own predicate. If the STATE write fails there is
    /// nothing to reconcile from, so that failure propagates.
    async fn write(&self, entry: &MailboxEntry) -> Result<(), tonic::Status> {
        self.state
            .put_entries(entry.delivery_id.clone(), entry.clone())
            .await?;
        if let Err(e) = (self.publish)(entry.clone()) {
            // Not an error to the caller: the publish queue being full does not
            // make the mailbox's own answer wrong, and the sweep is the
            // reconciler for exactly this. Logged with the id and nothing else.
            tracing::warn!(
                error = %e,
                delivery_id = %entry.delivery_id,
                "mailbox entry publish queue full; state written, record deferred to the sweep"
            );
        }
        Ok(())
    }

    /// One eviction-eligibility pass. THE ANSWER TO WHAT "EVICT" MEANS HERE.
    ///
    /// Read against the generator rather than against the decision doc's prose,
    /// `(spec.ejectionable)` turned out not to be a delete marker at all:
    ///
    ///   - `(spec.record).residency` declares a MEMORY window for a record TYPE,
    ///     and `Residency`'s own definition says "a warm row evicted from memory
    ///     is still complete and queryable on disk". Eviction is cache
    ///     maintenance. It destroys nothing.
    ///   - `(spec.ejectionable)` names the field whose per-row value decides
    ///     whether that window is consulted for a given row, with
    ///     EJECTIONABLE_UNSPECIFIED defined as never-evict so the marker an
    ///     author forgets keeps the row.
    ///   - Deletion is `(spec.record).retention`'s job, from a different clock.
    ///
    /// So this sweep does not delete a retrieved row and does not move it to a
    /// terminal state. It sets the gate — and only mailbox can, because only
    /// mailbox knows that a delivery has been pulled. THE MARK IS A WRITE, which
    /// matters: `(spec.store_epoch)` is stamped on every write and is what the
    /// residency window is measured against, so marking a row WINDOW is also
    /// what starts its clock. With `retrieved_grace_seconds` at its default 0
    /// the mark lands at the retrieval instant, which is the decision doc's
    /// ruling — "retrieved_at_ms is the point eviction eligibility starts
    /// counting from" — expressed through the mechanism the annotation has.
    ///
    /// A HELD row is never touched. That is the doc's rejected alternative
    /// ("residency clock keyed off held_since_ms") and the enum's own worked
    /// example ("a mailbox's undelivered message" is the row that must not leave
    /// memory), and it is enforced by predicate here, not by comment.
    ///
    /// THE SECOND HALF, and the only place this service deletes anything: a row
    /// whose `held_since_ms` has reached the record's own 90-day retention is
    /// removed from kernel state. The record store expires its copy on that
    /// clock by declaration; nothing else prunes this service's state root, so
    /// leaving it out would make `MailboxState.entries` grow until it hit its
    /// declared 1,000,000 bound and started refusing writes.
    ///
    /// It is idempotent and safe to run at any time from any instance: every
    /// change it makes is a fixed function of the row it reads.
    pub async fn sweep_once(&self) -> Result<SweepOutcome, tonic::Status> {
        let now = now_ms();
        let grace_ms = self.retrieved_grace_ms();
        let entries = self.state.fetch_all_entries().await?;

        let mut outcome = SweepOutcome {
            scanned: entries.len(),
            ..Default::default()
        };

        for (key, entry) in entries {
            // Expiry first: a row old enough to leave state does not also need
            // its gate corrected on the way out.
            if entry.held_since_ms > 0 && now.saturating_sub(entry.held_since_ms) >= STATE_RETENTION_MS
            {
                self.state.delete_entries(key).await?;
                self.expired.fetch_add(1, Ordering::Relaxed);
                outcome.expired += 1;
                continue;
            }

            if entry.status != MailboxEntryStatus::Retrieved as i32 {
                continue;
            }
            if entry.ejectionable == Ejectionable::Window as i32 {
                continue;
            }
            // A RETRIEVED row with no timestamp cannot have its eligibility
            // measured, so it stays NEVER. Not an error: it is the safe
            // direction, and the row is visible to whoever wants to ask why.
            if entry.retrieved_at_ms <= 0 {
                continue;
            }
            if now.saturating_sub(entry.retrieved_at_ms) < grace_ms {
                continue;
            }

            let mut marked = entry;
            marked.ejectionable = Ejectionable::Window as i32;
            self.write(&marked).await?;
            self.marked_evictable.fetch_add(1, Ordering::Relaxed);
            outcome.marked_evictable += 1;
        }

        Ok(outcome)
    }

    /// The sweep's timer. Runs until the ServiceManager aborts it at shutdown.
    ///
    /// A TIMER IN THE SERVICE PROCESS, not a cluster cron, and that is a
    /// deliberate choice rather than an oversight: this fleet has no scheduler
    /// primitive, and the sweep does not need one, because every action it takes
    /// is idempotent and derived from the row it reads. Two instances sweeping
    /// the same row concurrently write the same value; a missed pass is caught
    /// by the next one. The FencedLock (Phase 0 item 1) exists for work where
    /// that is not true, and using it here would be buying exclusion this
    /// operation does not need.
    ///
    /// The interval is re-read every pass, so a kernel config change takes
    /// effect on the next tick with no redeploy and no restart.
    pub async fn sweep_loop(self: Arc<Self>) {
        loop {
            tokio::time::sleep(self.sweep_interval()).await;
            match self.sweep_once().await {
                Ok(o) if o.marked_evictable > 0 || o.expired > 0 => {
                    tracing::info!(
                        scanned = o.scanned,
                        marked_evictable = o.marked_evictable,
                        expired = o.expired,
                        "mailbox eviction sweep"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    // A failed pass is not fatal: the next one re-derives
                    // everything from state.
                    tracing::error!(error = %e, "mailbox eviction sweep failed");
                }
            }
        }
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
/// would make `held_since_ms` — the clock retention and state expiry both
/// measure from — jump forward every time a subscription reconnected. The broker
/// timestamp is a property of the delivery, so it is stable across replays.
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
    /// every message this mailbox has ever seen. An entry that already exists is
    /// left exactly as it is — which is also what stops a replay from writing a
    /// RETRIEVED row back to HELD, the one way this path could destroy state a
    /// caller depends on.
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

        let existing = self.state.fetch_all_entries().await?;
        if existing.contains_key(&msg.delivery_id) {
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
            // HELD is NEVER-evict, stated explicitly rather than left at
            // UNSPECIFIED. Both mean the same thing to the store; only one of
            // them says the row was thought about.
            ejectionable: Ejectionable::Never as i32,
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

        self.write(&entry).await?;
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
        let entries = self.state.fetch_all_entries().await?;
        let mut held: Vec<MailboxEntry> = entries
            .into_values()
            .filter(|e| e.status == MailboxEntryStatus::Held as i32)
            .filter(|e| e.recipient == req.recipient)
            .filter(|e| e.namespace == req.namespace)
            .collect();

        // The order the RPC declares in (spec.method_collection).order_by. A
        // paginated read whose order is whatever the map iterated in is not
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
        let grace_ms = self.retrieved_grace_ms();
        let entries = self.state.fetch_all_entries().await?;

        let mut retrieved_delivery_ids = Vec::new();
        for delivery_id in &req.delivery_ids {
            let Some(entry) = entries.get(delivery_id) else {
                // An id naming no entry is absent from the response, not an
                // error. A client cannot tell "already swept away" from "never
                // existed" and should not have to.
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
            // and re-stamping retrieved_at_ms would push the eviction clock
            // forward every time it asked.
            if entry.status != MailboxEntryStatus::Held as i32 {
                continue;
            }

            let mut updated = entry.clone();
            updated.status = MailboxEntryStatus::Retrieved as i32;
            updated.retrieved_at_ms = now;
            // Mark the gate INLINE when there is no grace to wait out. The sweep
            // would get to it, but a row that is eligible the moment it is
            // retrieved should say so in the same write that retrieved it —
            // otherwise its (spec.store_epoch), which is what the residency
            // window measures, would start at the sweep's clock instead of the
            // retrieval's. With a non-zero grace the sweep owns the mark, which
            // is the whole reason the grace knob and the sweep are one design.
            if grace_ms == 0 {
                updated.ejectionable = Ejectionable::Window as i32;
                self.marked_evictable.fetch_add(1, Ordering::Relaxed);
            }

            self.write(&updated).await?;
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

/// The generated `Handler` the ServiceManager registers, wrapping the service.
pub struct MailboxServiceHandler {
    svc: Arc<MailboxService>,
}

impl MailboxServiceHandler {
    pub fn new(svc: Arc<MailboxService>) -> Self {
        Self { svc }
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
        tracing::info!(
            sweep_interval_seconds = cfg.sweep_interval_seconds,
            retrieved_grace_seconds = cfg.retrieved_grace_seconds,
            "mailbox config applied"
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
        let (held, retrieved, marked, expired) = self.svc.counts();
        tracing::info!(
            held,
            retrieved,
            marked_evictable = marked,
            expired,
            "mailbox shutting down"
        );
    }
}
