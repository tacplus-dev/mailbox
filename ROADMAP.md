# Roadmap: mailbox

Owned by Jody; changed only by an agent at Jody's request. Loopers propose in PROPOSALS.md.

## Now
Pull delivery for `messaging.Message`: a `HELD` `MailboxEntry` per delivery, `ListHeldMail` and `UpdateHeldMail`; rows live in Stratum only.
Ported from schematic 2026-10-23 (Rust). Unreleased; tacplus floor 2.4. Not yet in seatrials.

## Next
- Nothing unblocked; see Later.

## Later
- rsa (RUSTSEC-2023-0071) and crypto-bigint 0.5 (RUSTSEC-2026-0387) advisories (#5), blocked by tacplus-dev/tacplus#186
- Release v0.1.0 and publish its image (#2), blocked by tacplus-dev/looper#12 (needs Jody's decision)
- Decide whether messaging and mailbox are platform services; if so, port and prove (a message sent, held, pulled) (tacplus-dev/roadmap:decisions/tacplus-dev-stable-gate-2026-10-25.md)
- Write `MailboxEntry` to Stratum directly, not by publishing on the record's topic (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md)

## Not doing
- Nothing recorded yet.
