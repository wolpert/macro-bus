#!/usr/bin/env bash
# Two-node cluster demo on a single machine, over real mutual TLS.
#
# Generates certs, writes two daemon configs, starts daemons d1 and d2, then
# subscribes on d2 and publishes on d1 to show a message crossing the cluster.
set -euo pipefail

cd "$(dirname "$0")/.."

WORK="${TMPDIR:-/tmp}/macro-bus-cluster.$$"
mkdir -p "$WORK"
SOCK1="$WORK/d1.sock"
SOCK2="$WORK/d2.sock"

cleanup() {
  [[ -n "${D1:-}" ]] && kill "$D1" 2>/dev/null || true
  [[ -n "${D2:-}" ]] && kill "$D2" 2>/dev/null || true
  [[ -n "${SUB:-}" ]] && kill "$SUB" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> building"
cargo build --release -q
BUSD=./target/release/macro-busd
CLI=./target/release/macro-bus

echo "==> generating mTLS certs in $WORK/certs"
./scripts/gen-certs.sh "$WORK/certs" d1 d2

cat > "$WORK/d1.toml" <<EOF
[server]
daemon_id = "d1"
socket_path = "$SOCK1"

[cluster]
listen = "127.0.0.1:19440"
reconnect_base_ms = 100
reconnect_max_ms = 1000
[[cluster.peers]]
id = "d2"
addr = "127.0.0.1:19441"

[tls]
cert = "$WORK/certs/d1.crt"
key  = "$WORK/certs/d1.key"
ca   = "$WORK/certs/ca.pem"
EOF

cat > "$WORK/d2.toml" <<EOF
[server]
daemon_id = "d2"
socket_path = "$SOCK2"

[cluster]
listen = "127.0.0.1:19441"
reconnect_base_ms = 100
reconnect_max_ms = 1000
[[cluster.peers]]
id = "d1"
addr = "127.0.0.1:19440"

[tls]
cert = "$WORK/certs/d2.crt"
key  = "$WORK/certs/d2.key"
ca   = "$WORK/certs/ca.pem"
EOF

echo "==> starting daemon d1 and d2"
"$BUSD" --config "$WORK/d1.toml" >"$WORK/d1.log" 2>&1 & D1=$!
"$BUSD" --config "$WORK/d2.toml" >"$WORK/d2.log" 2>&1 & D2=$!

for _ in $(seq 1 50); do [[ -S "$SOCK1" && -S "$SOCK2" ]] && break; sleep 0.1; done

echo "==> registering 'weather.temp' on d1 and subscribing on d2"
"$CLI" --socket "$SOCK1" register weather.temp k
"$CLI" --socket "$SOCK2" subscribe weather.temp & SUB=$!

# Give the TLS peer link and the subscription a moment to come up.
sleep 1.5

echo "==> publishing on d1 (should appear under the d2 subscriber above)"
echo '18.0C on node d1' | "$CLI" --socket "$SOCK1" publish weather.temp k
sleep 1.0

echo "==> types known on d2 (registration propagated across the cluster):"
"$CLI" --socket "$SOCK2" list

echo "==> done"
