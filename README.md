# Wormhole Transit Relay (Rust)

Public home: <https://github.com/dougsko/wormhole-relay.rs>

A Rust reimplementation of the
[Magic Wormhole transit relay](https://github.com/magic-wormhole/magic-wormhole-transit-relay)
server, which the Magic Wormhole protocol uses to establish direct TCP/WebSocket
connections between two peers through a third-party relay.

This repository is a port of the Go implementation
([dougsko/wormhole-relay](https://github.com/dougsko/wormhole-relay)).
The [Go implementation](https://github.com/dougsko/wormhole-relay) was the
primary reference for behavior, and the
[magic-wormhole-protocols](https://github.com/magic-wormhole/magic-wormhole-protocols)
repository (in particular `transit.md`, "Relay Handshake") is the
authoritative source for the wire protocol.

Clients connect, send a handshake containing a shared token, and the relay
pairs up connections that present the same token (with different "side"
tokens, when provided) and glues the two sockets together.

## Protocol

A client connects and sends one of the following handshake lines:

```
please relay <64-hex-char-token>\n            # legacy
please relay <64-hex-char-token> for side <16-char-side>\n
```

If no matching token is pending, the client waits (up to 30 seconds in the
reference implementation) and receives `ok\n` once its partner connects.

If the token line is malformed the server responds with `bad handshake\n` and
closes the connection. If data is received before the partner has been found,
the server responds with `impatient\n` and closes the connection.

Once relaying, bytes are forwarded verbatim between the two sockets.

WebSocket clients carry the same byte stream in binary messages (any
message-to-logical-message alignment is allowed). The first message carries
the handshake; trailing line-ending whitespace in that message is tolerated
(the Python and Rust clients send the protocol-literal `please relay ...\n`
form, which the strict Go implementation would have rejected). Binary and
text messages are relayed verbatim; pings are answered with pongs.
TCP and WebSocket clients can be paired with each other.

## Usage

```sh
git clone https://github.com/dougsko/wormhole-relay.rs
cd wormhole-relay.rs
cargo build --release
./target/release/relay-server [options]
```

Or install the binaries with Cargo:

```sh
cargo install --path .
```

Options — the names, short flags, and defaults match the Python
`twist transitrelay` plugin (twisted `usage.Options`), and the endpoints use
the same `tcp:PORT[:interface=ADDR]` syntax as twisted's endpoint strings
(bare ports and `host:port` are also accepted):

| Flag | Default | Python equivalent | Description |
|------|---------|-------------------|-------------|
| `-p`, `--port` | `tcp:4001:interface=::` | `-p`, `--port` | endpoint to listen on |
| `-w`, `--websocket` | (disabled) | `-w`, `--websocket` | endpoint to listen for WebSocket connections |
| `-u`, `--websocket-url` | derived | `-u`, `--websocket-url` | WebSocket URL (derived from endpoint if not provided) |
| `--blur-usage` | `0` | `--blur-usage` | blur timestamps and data sizes in logs (`0` = off) |
| `--log-fd` | (none) | `--log-fd` | write JSON usage logs to this file descriptor (a number, or a path) |
| `--usage-db` | (none) | `--usage-db` | record usage data (SQLite) |

Notes on parity with the Python plugin:

- `--port` and `--websocket` take the same twisted endpoint strings the
  Python plugin accepts (`tcp:PORT`, `tcp:PORT:interface=ADDR`), not just a
  bare port; the defaults are identical.
- `--log-fd` matches the Python flag's intent (the Python version is strictly
  a file descriptor number; we also accept a file path, a superset).
- `--websocket-url` is accepted for compatibility and derived from the
  `--websocket` endpoint when not given (`ws://localhost:PORT/`), exactly like
  the Python plugin. It is not otherwise used (the Python factory also only
  uses it for that derivation).
- Usage logging: `--log-fd` writes one JSON object per finished connection
  (`started`, `total_time`, `waiting_time`, `total_bytes`, `mood`), and
  `--usage-db` writes the same events into a SQLite database in the reference
  v1 schema (`src/db_schemas/usage-v1.sql`, copied verbatim). The
  `waiting_time` column is `NULL` for connections that were never paired, and
  moods include `happy`, `lonely`, `redundant`, `impatient`, `errory`,
  `jilted`, and `empty` (a connection that closed without ever sending a
  handshake) — the same set the Python version records. In addition, a
  `current` row (`rebooted`, `updated`, `connected`, `waiting`,
  `incomplete_bytes`) is written at startup and every 5 minutes, mirroring
  the reference's `TimerService`.

Notes on parity with the Go implementation (deliberate differences):

- **WebSocket handshakes with a trailing newline are accepted.** The Go
  implementation required the first WebSocket message to be byte-identical
  to `please relay <token>[ for side <side>]` with no trailing newline,
  which rejects the protocol-literal handshake (`...\n`) that the Python and
  Rust clients actually send. We trim trailing line-ending whitespace before
  matching, so both forms work. This is the behavior mandated by
  `magic-wormhole-protocols` (`transit.md`, "Relay Handshake").
- **Endpoint parsing handles IPv6 interfaces correctly.** The Go endpoint
  parser split the remainder of `tcp:PORT:interface=...` on `:`, so any
  `interface=` value containing a colon was silently mangled. This parser
  keeps everything after `interface=`, so `tcp:4001:interface=::1` binds
  `[::1]:4001` and `tcp:4001:interface=::` binds the dual-stack wildcard.
- An empty endpoint host (`tcp:4001`) maps to the dual-stack wildcard
  `[::]:4001` instead of `:4001`, because tokio's binder rejects the empty
  host that Go's `net.Listen` accepts; both are the dual-stack wildcard on
  Linux.
- **Ctrl-C / SIGTERM flush pending usage records** before exit. The Go
  implementation dropped buffered usage events on shutdown.
- WebSocket pings are answered with pongs.
- Usage events are computed on the teardown path and written by a single
  background task fed by a bounded queue (1 024 events), batched in
  transactions of up to 64 events or every 100 ms — the same behavior as the
  Go implementation's async `UsageTracker`.
- The WebSocket server enforces `max_message_size`/`max_frame_size` of
  4 MiB, like the Go implementation's gorilla `ReadLimit`. Oversized
  handshake lines (> 1024 bytes) get `bad handshake\n`, as do malformed
  handshakes.
- Timeouts are identical to the Go implementation: 30 s pre-handshake read,
  30 s write, 5 s WebSocket close-frame write. TCP connections also get
  `TCP_NODELAY`, the accept loop backs off exponentially on repeated
  accept errors (5 ms base, capped at 1 s), and `RLIMIT_NOFILE` is raised
  when possible (soft limit below 10 000).

Everything else is wire-identical to the Go implementation: the handshake
syntax, the pairing rules (empty side matches everything; identical
non-empty sides do not pair with each other; leftover sides of a token get
`partnerConnectionLost`), the moods, the jilted-suppression rule (a `jilted`
event is skipped when the buddy ended `happy`, so each relayed transfer
produces exactly one usage record), the usage database schema (copied
verbatim), and the blur algorithm (`--blur-usage N`).

## Client-side environment variables

Setting a server flag is not enough to make a `wormhole` client talk to it —
the *client* chooses the servers. The Python `wormhole` CLI (and library)
reads these environment variables, which are the same for the Rust and Go
servers, since they speak the same protocols:

- `WORMHOLE_RELAY_URL` (`--relay-url`) — mailbox/rendezvous URL, e.g. `ws://localhost:4000/v1`
- `WORMHOLE_TRANSIT_HELPER` (`--transit-helper`) — `tcp:HOST:PORT` of a transit relay, e.g. `tcp:localhost:4001`

Example, using both Rust servers with a Python client:

```sh
mailbox-server --port 4500 --channel-db /tmp/mb.sqlite &
relay-server --port 4001 --usage-db /tmp/relay-usage.sqlite &
export WORMHOLE_RELAY_URL=ws://localhost:4500/v1
export WORMHOLE_TRANSIT_HELPER=tcp:localhost:4001
wormhole send --no-listen FILE            # add --no-listen on both sides to force the relay path
wormhole receive --no-listen --accept-file --output-file OUT CODE
```

## Running tests

```sh
cargo test               # unit tests (endpoint parsing, blur math) + integration tests
cargo test -- --nocapture

# benchmarks and load generator (see PERFORMANCE.md)
cargo build --release
./target/release/relay-stress --mode throughput --pairs 8 --bytes 16777216
```

The integration suite (`tests/integration.rs`) runs the relay on ephemeral
loopback ports and exercises the full state machine over real TCP and
WebSocket connections: pairing, bidirectional byte flow, `ok\n` handshake,
`bad handshake`, `impatient`, `lonely`, `empty`, `happy`/`jilted` mood
accounting (including the jilted-suppression rule), TCP↔WebSocket cross-transport
pairing, the usage SQLite database (schema creation, `current` row,
version check), and the JSON usage log format.

End-to-end interoperability suite (unmodified Python `magic-wormhole`
client against a real `relay-server` binary with Go `mailbox-server` as
rendezvous; both sides forced through the relay with `--no-listen`;
sha256-verified; relay usage DB checked for the `happy` pairing):

```sh
WORMHOLE_E2E_PYTHON=/path/to/wormhole WORMHOLE_E2E_MAILBOX=/path/to/wormhole-mailbox \
  bash e2e/run_e2e.sh
```

See [e2e/run_e2e.sh](e2e/run_e2e.sh) for the requirements and knobs. The
default `WORMHOLE_E2E_REQUIRE=0` skips (exit 0) when a requirement is
missing; CI sets `WORMHOLE_E2E_REQUIRE=1`.

## Project layout

```
src/endpoint.rs     twisted-style endpoint parsing (tcp:PORT[:interface=ADDR])
src/relay.rs        transit state machine, pairing registry, TCP/WebSocket transports
src/usage.rs        asynchronous usage accounting (JSON log and SQLite backends)
src/usage_db.rs     usage SQLite database creation and schema management
src/db_schemas/     usage-v1.sql, copied verbatim from the reference implementation
src/bin/relay-server.rs  the server binary
src/bin/relay-stress.rs  protocol-level load generator
tests/integration.rs     end-to-end tests of the state machine over loopback
e2e/run_e2e.sh           interoperability test against the Python client
```

## Running in a container (podman)

Build the image and run it:

```sh
podman build --format docker -t wormhole-relay .
podman run -d --name wormhole-relay \
    -p 4001:4001 -v wormhole-relay-data:/data \
    wormhole-relay
```

Multi-architecture images (linux/amd64, linux/arm64) are published to
`ghcr.io/dougsko/wormhole-relay.rs` by CI on pushes to `main` and on version
tags (`v1.2.3` → `1.2.3`, `1.2`).

- `--format docker` keeps the `HEALTHCHECK` (podman's default OCI image
  format drops it; the build still succeeds without it, you just lose the
  health probe). The probe uses busybox `nc -z` because the transit relay
  speaks raw TCP, not HTTP.
- The image is alpine-based and runs as non-root uid 10001. `/data` is a
  volume holding `relay-usage.sqlite` (the default `--usage-db`).
- Extra flags replace the default CMD entirely. To serve WebSocket clients
  as well as raw TCP:

  ```sh
  podman run -d --name wormhole-relay \
      -p 4001:4001 -p 4002:4002 -v wormhole-relay-data:/data \
      wormhole-relay --port tcp:4001:interface=:: \
      --websocket tcp:4002:interface=:: \
      --websocket-url ws://localhost:4002/ \
      --usage-db /data/relay-usage.sqlite
  ```

- `.dockerignore` keeps `.git`, `target/`, local databases (`*.sqlite`), and
  editor noise out of the build context.

## Running with Podman Quadlet

Quadlet files are provided under `packaging/quadlet/` for easy deployment as
systemd services using Podman. They pull the published image and use a
named `wormhole-relay-rs-data` volume, publish raw TCP port 4001, and start
the container on boot via the generated systemd unit.

Rootful installation:

```sh
sudo cp packaging/quadlet/*.container /etc/containers/systemd/
sudo systemctl daemon-reload
sudo systemctl start wormhole-relay.service
```

Rootless installation:

```sh
mkdir -p ~/.config/containers/systemd
cp packaging/quadlet/*.container ~/.config/containers/systemd/
systemctl --user daemon-reload
systemctl --user start wormhole-relay.service
loginctl enable-linger "$USER"   # if it should start before login
```

The generated units have an `[Install]` section, so no separate
`systemctl enable` is needed. Logs are available with:

```sh
sudo journalctl -u wormhole-relay.service -f      # rootful
journalctl --user -u wormhole-relay.service -f    # rootless
```

The default `Exec=` is
`--port tcp:4001:interface=:: --usage-db /data/relay-usage.sqlite`. To also
serve WebSocket transit clients, add `--websocket tcp:4002:interface=::` to
`Exec=`, publish `4002:4002`, and set `--websocket-url` if needed. Ensure
host TCP port 4001 is open in the firewall if accessed over the internet.
Desktop Magic Wormhole clients generally use the raw `tcp:HOST:4001` transit
helper.

## License

MIT — see [LICENSE](LICENSE). This is a Rust port of the MIT-licensed Go
implementation [dougsko/wormhole-relay](https://github.com/dougsko/wormhole-relay),
which is a Go port of the MIT-licensed Python
[magic-wormhole-transit-relay](https://github.com/magic-wormhole/magic-wormhole-transit-relay);
the usage schema under `src/db_schemas/` is derived verbatim from the
reference.
