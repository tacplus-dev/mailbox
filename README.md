# mailbox

Pull delivery for `messaging.Message`. BIG GOAL 8 Phase 1 item 8.

A third consumer group on `messaging.MessageBus.stream`: every delivery lands
here as its own full copy, and mailbox writes a `HELD` `MailboxEntry` keyed by
the SAME `delivery_id`, holding no message body. `ListHeldMail` answers "do I
have new mail"; `UpdateHeldMail` marks deliveries retrieved, which is what takes
them out of that answer.

It is NOT a second copy of message storage. `messaging.Message` is the permanent
record, with its own c2s query plane, and a client reads the body from there with
an id it already holds.

It WAS the fleet's only consumer of `(spec.ejectionable)` (Phase 0 item 4).
tacplus `cd8829fb` retired that annotation together with the HOT/WARM/COLD
residency model it gated, and this service was migrated off it on 2026-09-20:
`MailboxEntry` now declares
`(spec.record).residency{idle_seconds, deleted_max_age_seconds}` — one clock, per
record TYPE, with no per-row gate — and the sweep's marker half is gone. Nothing
was lost by that: eviction only ever removed a row from MEMORY, leaving it
complete and queryable on disk, so an unpulled delivery needs no pin to survive.

What the sweep still does, and all it does: delete this service's OWN
kernel-state rows once `held_since_ms` reaches 90 days, so `MailboxState.entries`
cannot grow past its declared bound.

- Proto: `platform/proto/services/mailbox.proto`
- Language: Rust (matching messaging and scotty)
- Build: `tacplus build` via the app's `build.sh`, not a bare `just generate` —
  this service needs the app proto set alongside the platform's
- Design: `platform/decisions/big8-p1-item8-mailbox-service-shape-2026-09-01.md`
  and `workers/big8-p1-mailbox-result.md` (which records the two corrections
  that decision doc invited, and what the sweep actually does)

See `platform/services/vanilla/FAMILY.md` for the ecosystem conventions.
