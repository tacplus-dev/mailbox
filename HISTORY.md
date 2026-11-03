# History: mailbox

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-29 looper issue Write records to Stratum through its write surface: Stratum no longer ingests them from the event bus (#7, 4c04eee)
- 2026-10-30 looper issue Gates fail on main with tacplus 2.5.3: stale Cargo.lock, and record reader list() now needs target_tenant (#8, 6223871)
- 2026-11-02 looper issue Gates fail on main: Cargo.lock is stale against the chassis tacplus 2.5.6 generates (ed25519-dalek 3, sha2 0.11) (#9, 1f568da)
- 2026-11-03 looper issue Trace propagation: on_message drops the delivery's trace_id on InsertIfAbsentMailboxEntry, and no test asserts it (#10)
