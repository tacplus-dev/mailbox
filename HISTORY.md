# History: mailbox

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-29 looper issue Write records to Stratum through its write surface: Stratum no longer ingests them from the event bus (#7, 4c04eee)
