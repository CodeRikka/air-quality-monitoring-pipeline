#!/usr/bin/env bash
set -euo pipefail

WAIT_TIMEOUT_SECONDS="${CNPG_WAIT_TIMEOUT_SECONDS:-300}"
KUBECTL_WAIT_TIMEOUT="${WAIT_TIMEOUT_SECONDS}s"
START_TIME="${SECONDS}"

microk8s kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.24/releases/cnpg-1.24.2.yaml
microk8s kubectl rollout status deployment/cnpg-controller-manager -n cnpg-system --timeout="${KUBECTL_WAIT_TIMEOUT}"

until [[ -n "$(microk8s kubectl get endpoints cnpg-webhook-service -n cnpg-system -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || true)" ]]; do
  if (( SECONDS - START_TIME >= WAIT_TIMEOUT_SECONDS )); then
    echo "Error: CNPG webhook service did not become ready within ${KUBECTL_WAIT_TIMEOUT}." >&2
    exit 1
  fi
  sleep 5
done
