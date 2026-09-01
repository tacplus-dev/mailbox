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

    // The state client is generated and owned by the ServiceManager: already
    // connected, already authorized, already watching every boundary by the time
    // this line runs. Cloning the handle is the whole of "wire up state access".
    let state = sm.state_clients().services().mailbox_state().clone();

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

    let svc = Arc::new(service::MailboxService::new(state, publish));

    // The eviction sweep. spawn_task ties it to the ServiceManager's lifecycle:
    // it is aborted at shutdown and the SM waits for it, so a sweep in flight
    // does not outlive the process that owns the state it is writing.
    //
    // This is the fleet's only scheduling primitive and it is enough here, for
    // the reason `sweep_loop` gives: every action the sweep takes is idempotent
    // and derived from the row it reads, so two instances running it concurrently
    // agree and a missed pass is picked up by the next one.
    let sweeper = svc.clone();
    sm.spawn_task(async move { sweeper.sweep_loop().await });

    let handler = Arc::new(service::MailboxServiceHandler::new(svc));

    sm.run(handler).await
}
