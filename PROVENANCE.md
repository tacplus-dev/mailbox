# Provenance

Copied **wholesale** on 2026-10-23 from `~/workspace/gitlab/seabeam/schematic/platform/services/mailbox` at
commit `29e900f` (branch `big8-p1-mailbox`), with `proto/services/mailbox.proto`, the common/vservices protos it imports,
and `proto/dependencies.yaml` from `platform/proto` at `57f45fe` on branch `big8-p1-messaging`. Build outputs not copied.

Jody, 2026-10-23: the platform's primitives move to tacplus-dev as ships in the night; the private copy runs
the launch fleet unchanged and new-design changes land here.

Divergences: `kind: service`; headers in tacplus.yaml (Jody Scott, Apache-2.0; LICENSE added); `src: proto`;
`proto/registry.yaml` is a lock of github.com/tacplus-dev/registry.
