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
- **Which per-tenant Stratum mailbox writes to** (2026-10-29, #7): mailbox is one service holding every tenant's mail,
  and Stratum is now per tenant (roadmap decisions/stratum-per-tenant-2026-10-25.md). The generated client dials
  `stratum`; whether that reaches the row's tenant's instance is for fleet-seatrials#3 to prove before mail is held there.
- **`UpdateHeldMail` as one `Apply`** (2026-10-29, #7): retrieving a page is one conditional replace per id, so a
  refusal midway leaves the earlier ids retrieved (retrying is idempotent). One `Apply` in canonical key order would
  make the page all or nothing, at the cost of one ABORTED failing the whole page.

## Promoted / declined
