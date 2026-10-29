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

    // The whole of "wire up storage": the ServiceManager owns stratum's record
    // plane client, generated because mailbox.proto declares (spec.record_read)
    // over MailboxEntry, and MailboxEntry grants this service its reads
    // (s2s_read) and writes (s2s_write). Rows are written through that client
    // and not published on the record's topic: stratum no longer ingests from
    // the bus (#7). There is no state client either — mailbox has no kernel
    // state root.
    //
    // Note what is NOT here for the CONSUMER half: no eventbus client, no topic
    // name, no consumer group. The group is the service label and the
    // subscription is started by sm.run() from the (spec.event_watch)
    // declaration in the proto.
    let rows = Arc::new(service::RecordPlaneStore(sm.clone()));

    let svc = Arc::new(service::MailboxService::new(rows));
    let handler = Arc::new(service::MailboxServiceHandler::new(svc));

    // No sweep task. The timer that used to run here pruned MailboxState in
    // kernel; there is no such root, and nothing in the record store is this
    // service's to delete (see Config.sweep_interval_seconds in the proto).
    sm.run(handler).await
}
