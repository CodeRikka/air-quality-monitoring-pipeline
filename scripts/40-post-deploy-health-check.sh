#!/usr/bin/env bash
set -euo pipefail

KUBECTL_BIN="${KUBECTL_BIN:-microk8s kubectl}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"

echo "[1/5] Checking namespaces"
${KUBECTL_BIN} get ns airflow serving storage >/dev/null

echo "[2/5] Waiting for core deployments"
${KUBECTL_BIN} rollout status deployment/aq-api -n serving --timeout="${WAIT_TIMEOUT}"
${KUBECTL_BIN} rollout status deployment/aq-web -n serving --timeout="${WAIT_TIMEOUT}"

echo "[3/5] Checking Airflow API server availability"
${KUBECTL_BIN} get pods -n airflow -l component=api-server

echo "[4/5] Running API health checks via service proxy"
${KUBECTL_BIN} get --raw /api/v1/namespaces/serving/services/http:aq-api:8080/proxy/healthz >/dev/null
${KUBECTL_BIN} get --raw /api/v1/namespaces/serving/services/http:aq-api:8080/proxy/latest/health >/dev/null

echo "[5/5] Post-deploy health check passed"
