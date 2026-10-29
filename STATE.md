# State: mailbox

Overwritten to stay current (loopers and agents). Last updated: 2026-10-29 by looper (#5).

- **Version / line:** unreleased (no tag; v0.1.0 is #2); tacplus floor 2.5
- **Works:** pull delivery for `messaging.Message`: a third consumer group on `messaging.MessageBus.stream` holds
  each delivery as a `HELD` `MailboxEntry`; `ListHeldMail` and `UpdateHeldMail`. Rows live in Stratum only, written
  through its generated write surface (`InsertIfAbsent`, `ReplaceIfUnchanged`, LOCAL durability) and read through
  `Get` and the query plane; nothing is published on the bus for Stratum (#7). Optional Gmail watch/send (`Config.gmail`).
- **Released / deployed:** nowhere yet; the hub deploys with fleet-seatrials#3.
- **Known gaps:** the generated-client half of the store (`RecordPlaneStore`) is not tested against a real Stratum;
  whether Stratum's write side stamps `spiffe_id`/`store_epoch_ms` is unverified (PROPOSALS.md); the rsa
  (RUSTSEC-2023-0071) and crypto-bigint (RUSTSEC-2026-0387) advisories stay in the lockfile through the generated
  chassis until tacplus v2.5.2 regenerates it (#5). Mailbox's own Gmail RS256 signing is on ring, off `rsa` (#5).
- **Blocked on:** #2 (release) on tacplus-dev/looper#12; #5 on tacplus v2.5.2 (backport tacplus-dev/tacplus#216).
