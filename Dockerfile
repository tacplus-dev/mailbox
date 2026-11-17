# Runtime base per #2 (hub, 2026-10-27): distroless.
# Pinned by digest (#20, hub 2026-11-17): the image main names is the image that
# gets built. The digest is cc-debian13's `latest` index on 2026-11-17 (debian
# 13.7). When distroless publishes fixes, move it in a commit of its own (the
# `docker-content-digest` of `curl -sI -H 'Accept: application/vnd.oci.image.index.v1+json'
# https://gcr.io/v2/distroless/cc-debian13/manifests/latest`), and move the
# image's entries in security-baseline.json with it: they name this reference.
FROM gcr.io/distroless/cc-debian13@sha256:809d04afa1be033c441f76bb0051878c342fb76d0ea236bc6dc94b655fa0fbd6
COPY target/release/mailbox /usr/local/bin/mailbox
# distroless's nonroot user (65532): the service needs no root (#14).
USER nonroot
ENTRYPOINT ["mailbox"]
