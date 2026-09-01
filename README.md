# mailbox

Pull delivery and eviction eligibility for `messaging.Message`. BIG GOAL 8
Phase 1 item 8.

A third consumer group on `messaging.MessageBus.stream`: every delivery lands
here as its own full copy, and mailbox writes a `HELD` `MailboxEntry` keyed by
the SAME `delivery_id`, holding no message body. `ListHeldMail` answers "do I
have new mail"; `UpdateHeldMail` marks deliveries retrieved, which is what makes
them eviction-eligible.

It is NOT a second copy of message storage. `messaging.Message` is the permanent
record — 90-day retention, its own c2s query plane — and a client reads the body
from there with an id it already holds.

It IS the first consumer of `(spec.ejectionable)` (Phase 0 item 4), which landed
with no runtime consumer anywhere in the fleet. "Evict" turned out to mean evict
FROM MEMORY, not delete: `(spec.record).residency` declares a memory window for
the type, `(spec.ejectionable)` is the per-row gate on it, and deletion is
`(spec.record).retention`'s job on a different clock. So the sweep sets the gate
(`NEVER` while held, `WINDOW` once retrieved) and deletes nothing except its own
kernel-state rows at the record's own retention age.

- Proto: `platform/proto/services/mailbox.proto`
- Language: Rust (matching messaging and scotty)
- Build: `tacplus build` via the app's `build.sh`, not a bare `just generate` —
  this service needs the app proto set alongside the platform's
- Design: `platform/decisions/big8-p1-item8-mailbox-service-shape-2026-09-01.md`
  and `workers/big8-p1-mailbox-result.md` (which records the two corrections
  that decision doc invited, and what the sweep actually does)

See `platform/services/vanilla/FAMILY.md` for the ecosystem conventions.
