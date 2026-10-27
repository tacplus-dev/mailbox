# Runtime base per #2 (hub, 2026-10-27): distroless.
FROM gcr.io/distroless/cc-debian13
COPY target/release/mailbox /usr/local/bin/mailbox
ENTRYPOINT ["mailbox"]
