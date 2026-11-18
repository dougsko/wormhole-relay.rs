#!/bin/bash
# End-to-end interoperability test for the Rust transit relay.
#
# Pushes a real magic-wormhole file transfer (Python client, both sides,
# --no-listen so the direct-connection path is disabled and ALL transit
# traffic flows through this relay) and then checks the relay's usage
# database for the expected `happy` pairing.
#
# Requirements:
#   - Python with `magic-wormhole` installed (provides `wormhole`):
#       pip install magic-wormhole
#   - a mailbox server binary (Go wormhole-mailbox, sibling checkout at
#     ../wormhole-mailbox by default, or set WORMHOLE_E2E_MAILBOX to the
#     binary path, or WORMHOLE_E2E_MAILBOX_SRC to a checkout to build)
#   - this repository's relay-server built (target/release/relay-server by
#     default, or set WORMHOLE_E2E_RELAY to a binary path)
#   - sqlite3 CLI (for the usage database assertion)
#
# Environment:
#   WORMHOLE_E2E_RELAY        path to relay-server binary
#   WORMHOLE_E2E_MAILBOX      path to mailbox-server binary
#   WORMHOLE_E2E_MAILBOX_SRC  path to wormhole-mailbox checkout (go build)
#   WORMHOLE_E2E_PYTHON       path to the Python `wormhole` entry script
#   WORMHOLE_E2E_REQUIRE=1    fail instead of skip when a requirement is missing
#   MAILBOX_PORT / RELAY_PORT ports to use (default: 14500 / 14001)
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)

RELAY_BIN=${WORMHOLE_E2E_RELAY:-"$REPO_ROOT/target/release/relay-server"}
MAILBOX_BIN=${WORMHOLE_E2E_MAILBOX:-}
MAILBOX_SRC=${WORMHOLE_E2E_MAILBOX_SRC:-"$REPO_ROOT/../wormhole-mailbox"}
PYTHON_BIN=${WORMHOLE_E2E_PYTHON:-wormhole}
REQUIRE=${WORMHOLE_E2E_REQUIRE:-0}
MAILBOX_PORT=${MAILBOX_PORT:-14500}
RELAY_PORT=${RELAY_PORT:-14001}

fail_or_skip() {
  if [ "$REQUIRE" = "1" ]; then
    echo "WORMHOLE-E2E FAIL: $*" >&2
    exit 1
  fi
  echo "WORMHOLE-E2E SKIP: $*" >&2
  exit 0
}

# --- resolve requirements -------------------------------------------------
if [ ! -x "$RELAY_BIN" ]; then
  fail_or_skip "relay-server not found at $RELAY_BIN (build with: cargo build --release)"
fi
if [ -z "$MAILBOX_BIN" ]; then
  if [ -d "$MAILBOX_SRC" ]; then
    echo "building mailbox-server from $MAILBOX_SRC"
    (cd "$MAILBOX_SRC" && go build -o "$REPO_ROOT/e2e/.mailbox-server" ./cmd/mailbox-server)
    MAILBOX_BIN="$REPO_ROOT/e2e/.mailbox-server"
  else
    fail_or_skip "wormhole-mailbox checkout not found at $MAILBOX_SRC (set WORMHOLE_E2E_MAILBOX to a binary)"
  fi
fi
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  fail_or_skip "python wormhole CLI not found in PATH (set WORMHOLE_E2E_PYTHON)"
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  fail_or_skip "sqlite3 CLI not found"
fi

# --- start servers ---------------------------------------------------------
WORK=$(mktemp -d)
echo "workdir: $WORK"
MAILBOX_LOG="$WORK/mailbox.log"
RELAY_LOG="$WORK/relay.log"
MAILBOX_PID=""
RELAY_PID=""
cleanup() {
  [ -n "$MAILBOX_PID" ] && kill "$MAILBOX_PID" 2>/dev/null || true
  [ -n "$RELAY_PID" ] && kill "$RELAY_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

"$MAILBOX_BIN" --port "$MAILBOX_PORT" --channel-db "$WORK/mailbox.sqlite" >"$MAILBOX_LOG" 2>&1 &
MAILBOX_PID=$!
"$RELAY_BIN" --port "tcp:$RELAY_PORT" --usage-db "$WORK/relay-usage.sqlite" >"$RELAY_LOG" 2>&1 &
RELAY_PID=$!
sleep 1

export WORMHOLE_RELAY_URL="ws://localhost:$MAILBOX_PORT/v1"
export WORMHOLE_TRANSIT_HELPER="tcp:localhost:$RELAY_PORT"

# --- transfer --------------------------------------------------------------
PAYLOAD="$WORK/payload.bin"
RECEIVED="$WORK/received.bin"
head -c 262144 /dev/urandom > "$PAYLOAD"   # 256 KiB forces a real relayed transfer
sha256sum "$PAYLOAD" | awk '{print $1}' > "$WORK/payload.sha256"

"$PYTHON_BIN" send --no-listen "$PAYLOAD" > "$WORK/sender.log" 2>&1 &
SENDER_PID=$!

# wait for the code to appear in the sender log
CODE=""
for _ in $(seq 1 50); do
  CODE=$(grep -oE '[0-9]+(-[a-z]+){2}' "$WORK/sender.log" | head -1 || true)
  [ -n "$CODE" ] && break
  sleep 0.2
done
if [ -z "$CODE" ]; then
  echo "WORMHOLE-E2E FAIL: no wormhole code appeared in the sender log:" >&2
  cat "$WORK/sender.log" >&2
  exit 1
fi
echo "wormhole code: $CODE"

"$PYTHON_BIN" receive --no-listen --accept-file --output-file "$RECEIVED" "$CODE" > "$WORK/receiver.log" 2>&1
wait "$SENDER_PID"

# --- verify ----------------------------------------------------------------
sha256sum "$RECEIVED" | awk '{print $1}' > "$WORK/received.sha256"
if ! cmp -s "$WORK/payload.sha256" "$WORK/received.sha256"; then
  echo "WORMHOLE-E2E FAIL: payload checksum mismatch" >&2
  exit 1
fi
echo "payload checksum OK"

HAPPY=$(sqlite3 "$WORK/relay-usage.sqlite" "SELECT count(*) FROM usage WHERE result='happy'")
if [ "$HAPPY" -lt 1 ]; then
  echo "WORMHOLE-E2E FAIL: relay usage DB has no 'happy' pairing:" >&2
  sqlite3 "$WORK/relay-usage.sqlite" ".tables" >&2 || true
  exit 1
fi
BYTES=$(sqlite3 "$WORK/relay-usage.sqlite" "SELECT sum(total_bytes) FROM usage WHERE result='happy'")
echo "usage DB: $HAPPY happy pairing(s), ${BYTES} bytes relayed"

kill "$MAILBOX_PID" "$RELAY_PID" 2>/dev/null || true
MAILBOX_PID=""
RELAY_PID=""
echo "WORMHOLE-E2E PASS"
