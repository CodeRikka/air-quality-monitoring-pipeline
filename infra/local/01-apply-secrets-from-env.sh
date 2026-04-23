#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"

if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Error: required env var '${name}' is not set (check ${ENV_FILE})." >&2
    exit 1
  fi
}

require_var POSTGRES_USER
require_var POSTGRES_PASSWORD
require_var MINIO_ACCESS_KEY
require_var MINIO_SECRET_KEY
require_var EPA_AQS_EMAIL
require_var EPA_AQS_KEY
require_var AIRNOW_API_KEY

POSTGRES_SUPERUSER_PASSWORD="${POSTGRES_SUPERUSER_PASSWORD:-${POSTGRES_PASSWORD}}"
GIT_SYNC_USERNAME="${GIT_SYNC_USERNAME:-replace-me-if-private-repo}"
GIT_SYNC_PASSWORD="${GIT_SYNC_PASSWORD:-replace-me-if-private-repo}"
AQS_REQUEST_TIMEOUT_SECONDS="${AQS_REQUEST_TIMEOUT_SECONDS:-60}"
AQS_MIN_REQUEST_INTERVAL_SECONDS="${AQS_MIN_REQUEST_INTERVAL_SECONDS:-5}"
AQS_MAX_RETRIES="${AQS_MAX_RETRIES:-3}"
AQS_RETRY_BACKOFF_FACTOR="${AQS_RETRY_BACKOFF_FACTOR:-1.0}"
AQS_GEOGRAPHY_MODE="${AQS_GEOGRAPHY_MODE:-auto}"
AQS_STATE_TO_COUNTY_SCORE_THRESHOLD="${AQS_STATE_TO_COUNTY_SCORE_THRESHOLD:-8000}"
AQS_BOOTSTRAP_START_YEAR="${AQS_BOOTSTRAP_START_YEAR:-2026}"
AQS_BOOTSTRAP_REGION_IDS="${AQS_BOOTSTRAP_REGION_IDS:-}"
AQS_BOOTSTRAP_PARAMETERS="${AQS_BOOTSTRAP_PARAMETERS:-}"
AQS_SHARD_PARAMS_PER_REQUEST="${AQS_SHARD_PARAMS_PER_REQUEST:-5}"
AQS_BOOTSTRAP_BDATE="${AQS_BOOTSTRAP_BDATE:-}"
AQS_BOOTSTRAP_EDATE="${AQS_BOOTSTRAP_EDATE:-}"
AQS_BOOTSTRAP_WINDOW_GRANULARITY="${AQS_BOOTSTRAP_WINDOW_GRANULARITY:-year}"
AQS_RECONCILE_CBDATE="${AQS_RECONCILE_CBDATE:-}"
AQS_RECONCILE_CEDATE="${AQS_RECONCILE_CEDATE:-}"
AIRNOW_GAP_START_UTC="${AIRNOW_GAP_START_UTC:-}"
AIRNOW_GAP_END_UTC="${AIRNOW_GAP_END_UTC:-}"
AIRNOW_GAP_CHUNK_HOURS="${AIRNOW_GAP_CHUNK_HOURS:-6}"
AIRNOW_GAP_FALLBACK_HOURS="${AIRNOW_GAP_FALLBACK_HOURS:-6}"
AIRNOW_NORMALIZE_HOURLY_MANIFEST_LIMIT="${AIRNOW_NORMALIZE_HOURLY_MANIFEST_LIMIT:-100}"
AIRNOW_NORMALIZE_GAP_MANIFEST_LIMIT="${AIRNOW_NORMALIZE_GAP_MANIFEST_LIMIT:-100}"
LOAD_POSTGRES_BATCH_SIZE="${LOAD_POSTGRES_BATCH_SIZE:-5000}"

microk8s kubectl create secret generic postgres-app-secret \
  -n storage \
  --from-literal=username="${POSTGRES_USER}" \
  --from-literal=password="${POSTGRES_PASSWORD}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl create secret generic postgres-superuser-secret \
  -n storage \
  --from-literal=username=postgres \
  --from-literal=password="${POSTGRES_SUPERUSER_PASSWORD}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl create secret generic air-quality-secrets \
  -n storage \
  --from-literal=MINIO_ROOT_USER="${MINIO_ACCESS_KEY}" \
  --from-literal=MINIO_ROOT_PASSWORD="${MINIO_SECRET_KEY}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl create secret generic air-quality-secrets \
  -n airflow \
  --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  --from-literal=MINIO_ROOT_USER="${MINIO_ACCESS_KEY}" \
  --from-literal=MINIO_ROOT_PASSWORD="${MINIO_SECRET_KEY}" \
  --from-literal=EPA_AQS_EMAIL="${EPA_AQS_EMAIL}" \
  --from-literal=EPA_AQS_KEY="${EPA_AQS_KEY}" \
  --from-literal=AQS_REQUEST_TIMEOUT_SECONDS="${AQS_REQUEST_TIMEOUT_SECONDS}" \
  --from-literal=AQS_MIN_REQUEST_INTERVAL_SECONDS="${AQS_MIN_REQUEST_INTERVAL_SECONDS}" \
  --from-literal=AQS_MAX_RETRIES="${AQS_MAX_RETRIES}" \
  --from-literal=AQS_RETRY_BACKOFF_FACTOR="${AQS_RETRY_BACKOFF_FACTOR}" \
  --from-literal=AQS_GEOGRAPHY_MODE="${AQS_GEOGRAPHY_MODE}" \
  --from-literal=AQS_STATE_TO_COUNTY_SCORE_THRESHOLD="${AQS_STATE_TO_COUNTY_SCORE_THRESHOLD}" \
  --from-literal=AQS_BOOTSTRAP_START_YEAR="${AQS_BOOTSTRAP_START_YEAR}" \
  --from-literal=AQS_BOOTSTRAP_REGION_IDS="${AQS_BOOTSTRAP_REGION_IDS}" \
  --from-literal=AQS_BOOTSTRAP_PARAMETERS="${AQS_BOOTSTRAP_PARAMETERS}" \
  --from-literal=AQS_SHARD_PARAMS_PER_REQUEST="${AQS_SHARD_PARAMS_PER_REQUEST}" \
  --from-literal=AQS_BOOTSTRAP_BDATE="${AQS_BOOTSTRAP_BDATE}" \
  --from-literal=AQS_BOOTSTRAP_EDATE="${AQS_BOOTSTRAP_EDATE}" \
  --from-literal=AQS_BOOTSTRAP_WINDOW_GRANULARITY="${AQS_BOOTSTRAP_WINDOW_GRANULARITY}" \
  --from-literal=AQS_RECONCILE_CBDATE="${AQS_RECONCILE_CBDATE}" \
  --from-literal=AQS_RECONCILE_CEDATE="${AQS_RECONCILE_CEDATE}" \
  --from-literal=AIRNOW_API_KEY="${AIRNOW_API_KEY}" \
  --from-literal=AIRNOW_GAP_START_UTC="${AIRNOW_GAP_START_UTC}" \
  --from-literal=AIRNOW_GAP_END_UTC="${AIRNOW_GAP_END_UTC}" \
  --from-literal=AIRNOW_GAP_CHUNK_HOURS="${AIRNOW_GAP_CHUNK_HOURS}" \
  --from-literal=AIRNOW_GAP_FALLBACK_HOURS="${AIRNOW_GAP_FALLBACK_HOURS}" \
  --from-literal=AIRNOW_NORMALIZE_HOURLY_MANIFEST_LIMIT="${AIRNOW_NORMALIZE_HOURLY_MANIFEST_LIMIT}" \
  --from-literal=AIRNOW_NORMALIZE_GAP_MANIFEST_LIMIT="${AIRNOW_NORMALIZE_GAP_MANIFEST_LIMIT}" \
  --from-literal=LOAD_POSTGRES_BATCH_SIZE="${LOAD_POSTGRES_BATCH_SIZE}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl create secret generic airflow-git-credentials \
  -n airflow \
  --from-literal=GIT_SYNC_USERNAME="${GIT_SYNC_USERNAME}" \
  --from-literal=GIT_SYNC_PASSWORD="${GIT_SYNC_PASSWORD}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl create secret generic air-quality-secrets \
  -n serving \
  --from-literal=POSTGRES_USER="${POSTGRES_USER}" \
  --from-literal=POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

echo "Kubernetes secrets synced from ${ENV_FILE}."
