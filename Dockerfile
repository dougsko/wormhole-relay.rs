# syntax=docker/dockerfile:1
#
# wormhole-relay (transit relay), packaged for podman (or docker). Build with:
#
#   podman build --format docker -t wormhole-relay .
#
# (--format docker keeps the HEALTHCHECK; podman's default OCI format drops it)
#
# Run with:
#
#   podman run -d --name wormhole-relay \
#       -p 4001:4001 -v wormhole-relay-data:/data \
#       wormhole-relay [extra flags...]
#
# Extra flags replace the default CMD entirely. To serve WebSocket clients
# instead of (in addition to) raw TCP, pass --websocket and optionally
# --websocket-url:
#
#   podman run -d --name wormhole-relay \
#       -p 4001:4001 -p 4002:4002 -v wormhole-relay-data:/data \
#       wormhole-relay --port tcp:4001:interface=:: \
#       --websocket tcp:4002:interface=:: \
#       --websocket-url ws://localhost:4002/ \
#       --usage-db /data/relay-usage.sqlite

# Cross-compiling on the native build platform means the arm64 image needs
# no QEMU emulation, so builds behave the same on CI and local machines.
FROM --platform=$BUILDPLATFORM docker.io/library/rust:1-alpine AS build
RUN apk add --no-cache musl-dev gcc
WORKDIR /src
# TARGETOS/TARGETARCH are provided automatically by buildx (and by podman
# build --platform); default to linux/amd64 when building without them
ARG TARGETOS TARGETARCH
# map buildx arch names to Rust musl target triples
RUN case "${TARGETARCH:-amd64}" in \
      amd64) echo x86_64-unknown-linux-musl > /rust-target ;; \
      arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac && rustup target add "$(cat /rust-target)"
# dependency layer: build with a dummy main first to cache the dep build
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && \
    echo 'fn main() {}' > src/bin/relay-server.rs && \
    echo 'fn main() {}' > src/bin/relay-stress.rs && \
    echo 'pub fn placeholder() {}' > src/lib.rs && \
    cargo build --release --target "$(cat /rust-target)" && \
    rm -rf src
COPY . .
# cargo's fingerprint uses mtimes; the dummy sources may appear newer than the
# real ones, so force a rebuild of our crate
RUN find src -exec touch {} + && \
    cargo build --release --target "$(cat /rust-target)" && \
    cp "target/$(cat /rust-target)/release/relay-server" /out-relay-server && \
    adduser -D -u 10001 wormhole && \
    mkdir -p /data && \
    chown 10001:10001 /data

FROM docker.io/library/alpine:3.21
LABEL org.opencontainers.image.source="https://github.com/dougsko/wormhole-relay.rs" \
      org.opencontainers.image.description="Rust port of magic-wormhole-transit-relay (transit relay)" \
      org.opencontainers.image.licenses="MIT"
# /etc/passwd and /etc/group bring in the wormhole user (uid 10001); they were
# prepared on the build platform as plain text, so no arm64 binary runs here
COPY --from=build /etc/passwd /etc/group /etc/
COPY --from=build --chown=10001:10001 /data /data
COPY --from=build /out-relay-server /usr/local/bin/relay-server
USER wormhole
VOLUME ["/data"]
EXPOSE 4001
# the relay speaks raw TCP, so probe the port with busybox nc instead of HTTP
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z 127.0.0.1 4001 || exit 1
ENTRYPOINT ["/usr/local/bin/relay-server"]
# default listens on TCP/4001 and records usage to /data (the Python plugin
# only enables the usage DB when --usage-db is passed)
CMD ["--port", "tcp:4001:interface=::", "--usage-db", "/data/relay-usage.sqlite"]
