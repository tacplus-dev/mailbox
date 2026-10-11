mod gmail;
#[cfg(test)]
mod gmail_tests;
mod service;
#[cfg(test)]
mod service_tests;

use std::error::Error;
use std::sync::Arc;

use mailbox_model::s2sm::ServiceManager;

#[tokio::main]
async fn main() {
    // The chassis logger, NOT tracing_subscriber::fmt().
    //
    // tracing allows exactly ONE global subscriber per process, so a service that
    // builds its own wins permanently and the chassis's redacting install can
    // only fail. install_bootstrap redacts from the first line and leaves the
    // layer swappable so the typed logger config still applies later.
    //
    // It matters here for the same reason it matters in messaging, one layer
    // down: every row this service handles names one person and carries the
    // subject line of another person's message to them. An unredacted log line
    // is the one place that leaves the entitlement model behind — which is why
    // nothing in service.rs logs a subject, a sender or a recipient, only ids
    // and counts.
    if let Err(e) = logger::install_bootstrap() {
        eprintln!("FATAL: could not install the redacting logger: {e}");
        std::process::exit(1);
    }

    if let Err(e) = run().await {
        tracing::error!(error = %e, "mailbox failed");
        let mut source = e.source();
        while let Some(cause) = source {
            tracing::error!(cause = %cause, "caused by");
            source = cause.source();
        }
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    // ServiceManager::new() reads BOOTSTRAP_CONFIG, connects to kernel over
    // mTLS, and returns once the service has an identity and config. sm.run()
    // then serves the generated gRPC surface, starts the event-bus subscription
    // that makes this service a consumer group, and blocks until shutdown.
    let sm = Arc::new(ServiceManager::new().await?);

    // The record topic is SYNTHESIZED from MailboxEntry's spec.record, so its
    // publisher factory is named after the generated wrapper (MailboxEntryBus)
    // and its field (stream), not after anything hand-written. Acquired before
    // the server starts rather than lazily, because it is on the path of the
    // very first message the subscription delivers.
    //
    // Note what is NOT here: no eventbus client is constructed, no topic name is
    // typed out, no token is acquired for the "eventbus" audience, and — for the
    // CONSUMER half — no consumer group is named either. The group is the
    // service label and the subscription is started by sm.run() from the
    // (spec.event_watch) declaration in the proto.
    let publish = sm.create_mailbox_mailbox_entry_bus_stream_publisher().await;

    // The READ half, and the whole of "wire up storage" since 2026-09-27: the
    // ServiceManager owns the record plane's client, generated because
    // mailbox.proto declares (spec.record_read) over MailboxEntry. There is no
    // state client here any more — mailbox has no kernel state root.
    let rows = Arc::new(service::RecordPlaneRows(sm.clone()));

    let svc = Arc::new(service::MailboxService::new(rows, publish));
    let handler = Arc::new(service::MailboxServiceHandler::new(svc));

    // No sweep task. The timer that used to run here pruned MailboxState in
    // kernel; there is no such root, and nothing in the record store is this
    // service's to delete (see Config.sweep_interval_seconds in the proto).
    sm.run(handler).await
}
