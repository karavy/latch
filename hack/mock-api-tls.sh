#!/usr/bin/env bash
# Creates, once, the test certificate of the mock API of the Tilt stack: the Secret mock-api-tls
# (apicall-debug), which the mock serves on its HTTPS port, and the ConfigMap apicall-debug-ca
# (latch-system), which the operator trusts with --apicall-ca-file. The operator sends Secret
# credentials (the portal API key) only over HTTPS, so the mock admin portal needs it. The
# certificate is self-signed, for the local Kind cluster only; the key never leaves the cluster.
set -euo pipefail

host=mock-api.apicall-debug.svc.cluster.local
if ! kubectl -n apicall-debug get secret mock-api-tls >/dev/null 2>&1; then
  dir=$(mktemp -d)
  trap 'rm -r -- "$dir"' EXIT
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout "$dir/tls.key" -out "$dir/tls.crt" \
    -subj "/CN=$host" -addext "subjectAltName=DNS:$host,DNS:mock-api.apicall-debug.svc,DNS:localhost" 2>/dev/null
  kubectl -n apicall-debug create secret tls mock-api-tls --cert="$dir/tls.crt" --key="$dir/tls.key"
  echo "Created the Secret mock-api-tls"
fi
# The CA the operator trusts is the certificate in the Secret, whatever created it.
kubectl -n apicall-debug get secret mock-api-tls -o jsonpath='{.data.tls\.crt}' | base64 -d |
  kubectl -n latch-system create configmap apicall-debug-ca --from-file=ca.crt=/dev/stdin --dry-run=client -o yaml |
  kubectl apply -f - >/dev/null
echo "The ConfigMap apicall-debug-ca holds the certificate of mock-api-tls"
