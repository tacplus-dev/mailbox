# Local development builds only — not used in CI/CD.

TACPLUS := env_var_or_default("TACPLUS_BIN", "tacplus")

default: generate

generate:
  #!/usr/bin/env bash
  set -euo pipefail
  {{TACPLUS}} build --tag latest

# Every later issue (ports, releases) runs this: fresh generation, then the
# generated-code checks, build and test.
gates: generate
  #!/usr/bin/env bash
  set -euo pipefail
  tracked="$(git ls-files target)"
  test -z "$tracked" || { echo "generated files are tracked:" >&2; echo "$tracked" >&2; exit 1; }
  stale="$(git status --porcelain -- proto/registry.yaml)"
  test -z "$stale" || { echo "tacplus rewrote the registry lock; commit it:" >&2; echo "$stale" >&2; exit 1; }
  cargo build --locked
  cargo test --locked
  echo "gates: ok"
