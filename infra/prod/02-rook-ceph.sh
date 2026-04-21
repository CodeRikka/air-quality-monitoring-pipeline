#!/usr/bin/env bash
set -euo pipefail

WAIT_TIMEOUT_SECONDS="${ROOK_WAIT_TIMEOUT_SECONDS:-600}"
START_TIME="${SECONDS}"

microk8s enable rook-ceph
microk8s kubectl wait --for=condition=Ready pod -n rook-ceph -l app=rook-ceph-operator --timeout="${WAIT_TIMEOUT_SECONDS}s"

until microk8s kubectl get storageclass rook-ceph-block >/dev/null 2>&1; do
  if (( SECONDS - START_TIME >= WAIT_TIMEOUT_SECONDS )); then
    echo "Error: storage class 'rook-ceph-block' did not become available within ${WAIT_TIMEOUT_SECONDS}s." >&2
    exit 1
  fi
  sleep 5
done
