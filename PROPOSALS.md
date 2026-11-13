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
- **A security checks pipeline, and a README "Security checks" section** (2026-11-06, nightly scan): the build
  runs only `planning-files`, so there is no security report for the nightly scan to read. The scan has to run
  `cargo audit` itself, and nothing checks the image. A workflow like fleet-seatrials' `.github/workflows/security.yml`
  (cargo audit on `Cargo.lock`, an image scan of the distroless runtime) would let the scan read a report and catch
  advisories on every push, not just nightly. The README should name the workflow and say where its report lives.
  Platform context: tacplus-dev/roadmap ROADMAP.md, Later, "Automatic security remediation" (looper#49).
- **ROADMAP "Now": the floor in force** (2026-11-08, #14): Jody asked #14 to update "Now" to the floor in force, but a
  looper commit may not touch ROADMAP.md. For an agent at his request: "Released v0.1.x; tacplus floor 2.6 (2.6.1
  pinned in tacplus.lock). Not yet in seatrials." (now: "Unreleased; tacplus floor 2.4").
- **Move the pin to 2.6.2** (2026-11-08, #14): v2.6.2 was released while #14 ran (Kani harnesses, image rebuilds; no
  change mailbox needs). `tacplus pin` in a commit of its own, when a change wants it. Nightly scan 2026-11-09: v2.6.3
  and v2.6.4 followed on 2026-11-08, so the newest patch of the 2.6 floor is now 2.6.4 (the pin is still 2.6.1).
  Nightly scan 2026-11-10: v2.6.5 (2026-11-09, a cross-test change only) is now the newest; 2.6 is still the latest line.
  None of these patches carries tacplus-dev/tacplus#368 (still open), which #15 waits on.
  Nightly scan 2026-11-11: v2.6.8 (2026-11-11, registry.json layout, tacplus#412) is now the newest; 2.6 is still the
  latest line, and tacplus#368 is still open.
  Nightly scan 2026-11-12: v2.6.10 (2026-11-12) is now the newest, and 2.6 is still the latest line (2.5.22 and
  older are backports). It carries "Rust/TypeScript chassis: no hardcoded floors that admit known advisories"
  (tacplus#338, PR tacplus#472). So a pin to 2.6.10 may let the generated chassis require a fixed rmcp itself, not
  only the relock #16 makes. tacplus#368 is still open.
  Nightly scan 2026-11-13: v2.6.11 (2026-11-13) is now the newest. 2.6 is still the latest line (2.5.23 and older
  are backports).
- **Unmaintained crates in the generated chassis** (2026-11-10, nightly scan): `osv-scanner` on `Cargo.lock` warns on
  `backoff` 0.4.0 (RUSTSEC-2025-0012) and `instant` 0.1.13 (RUSTSEC-2024-0384) via `httpclient`, `rustls-pemfile`
  2.2.0 (RUSTSEC-2025-0134) via `httpclient`, `servicemanager` and `mailbox-model`, and `unsafe-libyaml` 0.2.11
  (RUSTSEC-2024-0449) via `serde_yaml`. None has a fixed version, and every one comes from tacplus-generated code. So
  this is tacplus's to replace (backoff → backon, say; rustls-pki-types' PEM reader), as part of
  tacplus-dev/roadmap `items/tacplus-security-sweep.md`. It does not fail the floor today (no severity, no fix).
- **The scan's security report** (2026-11-10, nightly scan): the build pipeline now runs the security floor on this
  repository (#14), but tonight's scan run directory had no `security-mailbox` report in it. So the scan audited
  `Cargo.lock` itself again (and found #16). Fetching the report into the scan's run directory is looper's to fix.
  Until then, the "security checks pipeline" proposal above is half done.
  Nightly scan 2026-11-11: still no report in the run directory (and README still has no "Security checks" section),
  so the scan audited `Cargo.lock` itself again and found #17 (rustix, RUSTSEC-2026-0486).
  Nightly scan 2026-11-12: the report is now in the run directory (`security/summary.md`, `findings.json`), so that
  half is done. But it is a report of 5541bf4, #17's relock (a child of main 3d1eb65 that is not on main), not of
  main. So the scan ran `osv-scanner` on main's `Cargo.lock` too, and the only difference is rustix 1.1.4 (#17).
  README still has no "Security checks" section.
  Nightly scan 2026-11-12 (second run, 20261112T101503Z): the same report, still of 5541bf4 and not main (f8d3aaa).
  README has no "Trace propagation" section either, though the scan is pointed at both. A short section for each
  (what runs, where its report lives, the `// trace: unrelated: <why>` marker) would make the README match.
- **The scan cannot run `just gates`** (2026-11-11, nightly scan): `just generate` fails in the scan's runner before
  building anything: `tacplus build` needs a Docker daemon and gets `failed to connect to the docker API at
  unix:///var/run/docker.sock`. So tonight nobody checked whether the gates pass on main. The runner needs a
  daemon, or the scan should hand the gates to the build pipeline the way issue runs do (`repo-looper --build`).
  That is looper's to fix.
  Nightly scan 2026-11-12: the same, this time through the broker (`docker` is looper-docker with
  LOOPER_DOCKER_BROKER, looper#68): `docker version` reaches it, but its side has no daemon at
  /var/run/docker.sock either. The gates on main went unchecked again. Related: build jobs do not report
  (tacplus-dev/looper#113, open), which is what keeps #16 from landing.
  Nightly scan 2026-11-13 (20261113T093604Z): the same. `docker info` through the broker gets no daemon at
  /var/run/docker.sock, so the gates on main (c881988) went unchecked for the third night. The security report
  is still of 5541bf4, not main. `osv-scanner` on main's `Cargo.lock` finds only rmcp (#16), rustix (#17) and the
  unmaintained crates above.

## Promoted / declined
- **Which per-tenant Stratum mailbox writes to** (2026-10-29, #7): decided 2026-10-30 in #8 (Jody): the tenant of the
  record itself, its namespace copied from the message, never mailbox's own token tenant.
