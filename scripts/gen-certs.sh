#!/usr/bin/env bash
# Generate self-signed mutual-TLS certificates for a set of macro-bus daemons.
#
# Each daemon gets a cert whose subjectAltName (DNS) is its daemon id; that is
# the name peers verify on dial. A shared CA bundle (the concatenation of all
# node certs) lets every daemon trust every other. This mirrors what the
# integration tests do, using openssl instead of rcgen.
#
# Usage:  scripts/gen-certs.sh OUTDIR ID [ID ...]
# Example: scripts/gen-certs.sh ./certs d1 d2
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 OUTDIR ID [ID ...]" >&2
  exit 2
fi

OUTDIR="$1"; shift
mkdir -p "$OUTDIR"

CA_BUNDLE="$OUTDIR/ca.pem"
: > "$CA_BUNDLE"

for id in "$@"; do
  echo "==> generating cert for '$id'"
  # Explicit extensions: an end-entity (CA:FALSE) self-signed leaf whose SAN is
  # the daemon id. webpki rejects a CA:TRUE cert presented as an end-entity, so
  # we must NOT let openssl's default `req -x509` mark it as a CA.
  conf="$(mktemp)"
  cat > "$conf" <<CFG
[req]
distinguished_name = dn
x509_extensions = ext
prompt = no
[dn]
CN = $id
[ext]
basicConstraints = critical,CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:$id
CFG
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -config "$conf" \
    -keyout "$OUTDIR/$id.key" \
    -out    "$OUTDIR/$id.crt" \
    >/dev/null 2>&1
  rm -f "$conf"
  chmod 600 "$OUTDIR/$id.key"
  cat "$OUTDIR/$id.crt" >> "$CA_BUNDLE"
done

echo "==> wrote per-node cert/key and shared CA bundle to $OUTDIR"
echo "    CA bundle: $CA_BUNDLE"
