# State: mailbox

Overwritten to stay current (loopers and agents). Last updated: 2026-11-03 by looper (#11).

- **Version / line:** unreleased (no tag; v0.1.0 is #2); tacplus floor 2.5
- **Works:** pull delivery for `messaging.Message`: a third consumer group on `messaging.MessageBus.stream` holds
  each delivery as a `HELD` `MailboxEntry`; `ListHeldMail` and `UpdateHeldMail`. Rows live in Stratum only, written
  through its generated write surface (`InsertIfAbsent`, `ReplaceIfUnchanged`, LOCAL durability) and read through
  `Get` and the query plane; nothing is published on the bus for Stratum (#7). Every record-plane call is routed
  (`target_tenant`, tacplus 2.5.3) to the tid of the row's own namespace, `/tenants/<tid>[/...]`: the delivery's on
  the way in, the caller's verified one on the way out; a delivery in no tenant is dropped (#8). A delivery's
  `trace_id` (field 200) rides on the insert it causes (#10), and an `UpdateHeldMail`'s on every get and replace it
  causes, retries included (#11); a `ListHeldMail`'s is threaded to the store's `list` on every page (#12). Optional Gmail watch/send (`Config.gmail`).
- **Released / deployed:** nowhere yet; the hub deploys with fleet-seatrials#3.
- **Known gaps:** the generated-client half of the store (`RecordPlaneStore`) is not tested against a real Stratum;
  whether Stratum's write side stamps `spiffe_id`/`store_epoch_ms` is unverified (PROPOSALS.md); per-tenant routing
  is tested against the fake only. `ListHeldMail`'s `trace_id` still stops at the generated record reader, which takes none (#12, blocked on
  tacplus-dev/tacplus#313). The lockfile is relocked on tacplus 2.5.3: rsa and crypto-bigint are gone (#5, #8).
- **Blocked on:** #2 (release) on tacplus-dev/looper#12; #12 (the reader's `trace_id`) on tacplus-dev/tacplus#313.
