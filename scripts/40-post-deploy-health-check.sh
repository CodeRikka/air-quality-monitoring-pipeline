#!/usr/bin/env bash
set -euo pipefail

KUBECTL_BIN="${KUBECTL_BIN:-microk8s kubectl}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.8.0}"

echo "[1/5] Checking namespaces"
${KUBECTL_BIN} get ns airflow serving storage >/dev/null

echo "[2/5] Waiting for core deployments"
${KUBECTL_BIN} rollout status deployment/aq-api -n serving --timeout="${WAIT_TIMEOUT}"
${KUBECTL_BIN} rollout status deployment/aq-web -n serving --timeout="${WAIT_TIMEOUT}"

echo "[3/5] Checking Airflow API server availability"
${KUBECTL_BIN} get pods -n airflow -l component=api-server

echo "[4/5] Running in-cluster API health checks"
${KUBECTL_BIN} run aq-healthcheck-curl \
  -n serving \
  --image="${CURL_IMAGE}" \
  --restart=Never \
  --rm -i \
  --command -- sh -c "
    set -e
    curl -fsS http://aq-api.serving.svc.cluster.local:8080/healthz >/dev/null
    curl -fsS http://aq-api.serving.svc.cluster.local:8080/latest/health >/dev/null
  "

echo "[5/5] Post-deploy health check passed"
