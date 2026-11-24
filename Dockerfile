# Runtime base per #2 (hub, 2026-10-27): distroless. Pinned by digest, never a
# floating tag (#25; tests/dockerfile.rs): the tag's digest of 2026-11-24, moved
# by nightly maintenance (tacplus-dev/looper#234).
FROM gcr.io/distroless/cc-debian13:latest@sha256:dac90a25ffe03ebaace90cda886b081e61ab6cd77ac47eda51f7bcc15c320765
COPY target/release/mailbox /usr/local/bin/mailbox
# distroless's nonroot user (65532): the service needs no root (#14).
USER nonroot
ENTRYPOINT ["mailbox"]
