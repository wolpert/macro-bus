#!/usr/bin/env bash
# Standalone end-to-end demo: start a daemon, subscribe, register, publish,
# and show the message being delivered. No cluster, no TLS.
set -euo pipefail

cd "$(dirname "$0")/.."

SOCK="${TMPDIR:-/tmp}/macro-bus-demo.$$.sock"
DAEMON_LOG="${TMPDIR:-/tmp}/macro-bus-demo.$$.log"

cleanup() {
  [[ -n "${DAEMON_PID:-}" ]] && kill "$DAEMON_PID" 2>/dev/null || true
  [[ -n "${SUB_PID:-}" ]] && kill "$SUB_PID" 2>/dev/null || true
  rm -f "$SOCK"
}
trap cleanup EXIT

echo "==> building"
cargo build --release -q

BUSD=./target/release/macro-busd
CLI=./target/release/macro-bus

echo "==> starting daemon (id=demo, socket=$SOCK)"
"$BUSD" --id demo --socket "$SOCK" >"$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!

# Wait for the socket to appear.
for _ in $(seq 1 50); do [[ -S "$SOCK" ]] && break; sleep 0.1; done

echo "==> registering type 'chat.room' and subscribing"
"$CLI" --socket "$SOCK" register chat.room letmein
"$CLI" --socket "$SOCK" subscribe chat.room &
SUB_PID=$!
sleep 0.5

echo "==> publishing three messages"
printf 'hello from the demo\nsecond line' | "$CLI" --socket "$SOCK" publish chat.room letmein
echo 'a one-liner' | "$CLI" --socket "$SOCK" publish chat.room letmein
"$CLI" --socket "$SOCK" publish chat.room letmein --message 'via --message flag'

sleep 0.5
echo "==> known types:"
"$CLI" --socket "$SOCK" list

echo "==> done (the subscriber output above shows the delivered messages)"
