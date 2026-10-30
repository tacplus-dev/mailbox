# Proposals: mailbox

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **<title>** (<date>, <issue/run link>): <what and why>
- **Who stamps `spiffe_id` and `store_epoch_ms` on a store write** (2026-10-29, #7): eventbus stamped the publisher's
  `spiffe_id` while `MailboxEntry` was published, and the record store stamped `store_epoch_ms` on ingest. Through
  Stratum's write surface mailbox still leaves both empty; tacplus#163's docs do not say the store-side wrapper
  stamps either. Ask tacplus/stratum; if nobody does, `spiffe_id` (the record `owner`) stays empty on every row.
- **Test `RecordPlaneStore` against a real Stratum** (2026-10-29, #7): the tests drive a fake behind the
  `MailboxEntryStore` seam; the forwarding to the generated client (NOT_FOUND → absent, the request shapes) is
  untested, as tacplus#163 also lists "a runtime gate against a real store" as a follow-up.
- **Mail addressed in no tenant** (2026-10-30, #8): a delivery whose namespace is not `/tenants/<tid>[/...]`
  (`/system`, say) has no tenant's Stratum to be held in, so mailbox drops it (logged, not retried), and a caller in
  no tenant sees an empty mailbox. If messaging can publish such deliveries (service-to-service mail), they need a
  home: a platform store, or a refusal at messaging's send.
- **Prove per-tenant routing on seatrials** (2026-10-30, #8): the routing is tested against the fake; that a held
  row reaches `tac-<deployment>-<tid>`'s Stratum is for fleet-seatrials#3 to show before mail is held there.
- **`UpdateHeldMail` as one `Apply`** (2026-10-29, #7): retrieving a page is one conditional replace per id, so a
  refusal midway leaves the earlier ids retrieved (retrying is idempotent). One `Apply` in canonical key order would
  make the page all or nothing, at the cost of one ABORTED failing the whole page.

## Promoted / declined
- **Which per-tenant Stratum mailbox writes to** (2026-10-29, #7): decided 2026-10-30 in #8 (Jody): the tenant of the
  record itself, its namespace copied from the message, never mailbox's own token tenant.
