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

# Build natively for the target platform. Cross-compiling would need an
# aarch64 musl libc cross-toolchain (e.g. for rusqlite's bundled SQLite,
# built via cc); Alpine doesn't ship one. QEMU is registered ahead of time
# in CI, and locally `podman build --platform` works the same way.
FROM docker.io/library/rust:1-alpine AS build
RUN apk add --no-cache musl-dev gcc
WORKDIR /src
# dependency layer: build with a dummy main first to cache the dep build
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && \
    echo 'fn main() {}' > src/bin/relay-server.rs && \
    echo 'fn main() {}' > src/bin/relay-stress.rs && \
    echo 'pub fn placeholder() {}' > src/lib.rs && \
    cargo build --release && \
    rm -rf src
COPY . .
# cargo's fingerprint uses mtimes; the dummy sources may appear newer than the
# real ones, so force a rebuild of our crate
RUN find src -exec touch {} + && \
    cargo build --release && \
    cp target/release/relay-server /out-relay-server && \
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
