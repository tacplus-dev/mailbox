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

There is no sweep any more, and no kernel state for it to sweep. `MailboxState`
was removed on 2026-09-27 per
`looper/schematic/decisions/kernel-no-durability-user-data-to-stratum-2026-09-25.md`
item 3: a mailbox entry is one person's mail, and kernel holds no user data. Rows
live in stratum only — written through stratum's generated write surface
(`InsertIfAbsentMailboxEntry` to hold a delivery, `ReplaceIfUnchangedMailboxEntry`
to retrieve it, granted by `MailboxEntry`'s `s2s_write` and acked at its declared
`LOCAL` durability), read back through `GetMailboxEntry` and the generated reader
that the file-level `(spec.record_read)` pairs with `MailboxEntry`'s own
`s2s_read` grant, the same pairing `letsencrypt` uses for its certificate.
Rows are no longer published on the record's topic: stratum stopped ingesting
from the event bus (tacplus-dev/stratum#2, mailbox #7). A write's answer is
final, so the `ListHeldMail` a client issues after `UpdateHeldMail` is correct
without a kernel copy behind it; while stratum's redo is unreachable a write is
refused (UNAVAILABLE), `UpdateHeldMail` answers that, and a delivery is not
acknowledged, so it is delivered again.

One consequence, called out because nothing else in the tree says it: the 90-day
`held_since_ms` expiry went with the state root it bounded, and no record-side
clock replaced it. Mailbox entries are now kept for as long as stratum keeps
them.

- Proto: `platform/proto/services/mailbox.proto`
- Language: Rust (matching messaging)
- Build: `just generate` runs `tacplus build` on the floor and `depends` that
  `tacplus.yaml` declares; `just gates` is the check to run before pushing
  (and what the looper and nightly scanner run): fresh generation, a check that
  nothing under `target/` is tracked and that `proto/registry.yaml` is
  unchanged, then `cargo build --locked` and `cargo test --locked`. It exits
  non-zero if any step fails
- Design: `platform/decisions/big8-p1-item8-mailbox-service-shape-2026-09-01.md`
  and `workers/big8-p1-mailbox-result.md` (which records the two corrections
  that decision doc invited, and what the sweep actually does)

See `platform/services/vanilla/FAMILY.md` for the ecosystem conventions.
