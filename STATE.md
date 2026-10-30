# State: mailbox

Overwritten to stay current (loopers and agents). Last updated: 2026-10-30 by looper (#8).

- **Version / line:** unreleased (no tag; v0.1.0 is #2); tacplus floor 2.5
- **Works:** pull delivery for `messaging.Message`: a third consumer group on `messaging.MessageBus.stream` holds
  each delivery as a `HELD` `MailboxEntry`; `ListHeldMail` and `UpdateHeldMail`. Rows live in Stratum only, written
  through its generated write surface (`InsertIfAbsent`, `ReplaceIfUnchanged`, LOCAL durability) and read through
  `Get` and the query plane; nothing is published on the bus for Stratum (#7). Every record-plane call is routed
  (`target_tenant`, tacplus 2.5.3) to the tid of the row's own namespace, `/tenants/<tid>[/...]`: the delivery's on
  the way in, the caller's verified one on the way out; a delivery in no tenant is dropped (#8). Optional Gmail
  watch/send (`Config.gmail`).
- **Released / deployed:** nowhere yet; the hub deploys with fleet-seatrials#3.
- **Known gaps:** the generated-client half of the store (`RecordPlaneStore`) is not tested against a real Stratum;
  whether Stratum's write side stamps `spiffe_id`/`store_epoch_ms` is unverified (PROPOSALS.md); per-tenant routing
  is tested against the fake only. The lockfile is relocked on tacplus 2.5.3: rsa and crypto-bigint are gone (#5, #8).
- **Blocked on:** #2 (release) on tacplus-dev/looper#12.
