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

require_var POSTGRES_HOST
require_var POSTGRES_PORT
require_var POSTGRES_DB_AIRQUALITY
require_var POSTGRES_USER
require_var POSTGRES_PASSWORD
require_var MINIO_ENDPOINT
require_var MINIO_ACCESS_KEY
require_var MINIO_SECRET_KEY

POSTGRES_CONN_ID="${AIRFLOW_CONN_POSTGRES_ID:-aq_postgres}"
MINIO_CONN_ID="${AIRFLOW_CONN_MINIO_ID:-aq_minio}"
MINIO_SECURE="${MINIO_SECURE:-false}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
AIRFLOW_WAIT_TIMEOUT="${AIRFLOW_WAIT_TIMEOUT:-300s}"

MINIO_SCHEME="http"
if [[ "${MINIO_SECURE}" == "true" ]]; then
  MINIO_SCHEME="https"
fi
MINIO_ENDPOINT_URL="${MINIO_SCHEME}://${MINIO_ENDPOINT}"

microk8s kubectl rollout status deployment/airflow-scheduler -n airflow --timeout="${AIRFLOW_WAIT_TIMEOUT}"
SCHEDULER_POD="$(microk8s kubectl get pod -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}')"

if [[ -z "${SCHEDULER_POD}" ]]; then
  echo "Error: could not find Airflow scheduler pod in namespace airflow." >&2
  exit 1
fi

if ! microk8s kubectl exec -n airflow "${SCHEDULER_POD}" -c scheduler -- env \
  POSTGRES_CONN_ID="${POSTGRES_CONN_ID}" \
  POSTGRES_HOST="${POSTGRES_HOST}" \
  POSTGRES_PORT="${POSTGRES_PORT}" \
  POSTGRES_DB_AIRQUALITY="${POSTGRES_DB_AIRQUALITY}" \
  POSTGRES_USER="${POSTGRES_USER}" \
  POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  MINIO_CONN_ID="${MINIO_CONN_ID}" \
  MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" \
  MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
  MINIO_ENDPOINT_URL="${MINIO_ENDPOINT_URL}" \
  AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
  MINIO_SECURE="${MINIO_SECURE}" \
  bash -lc '
set -euo pipefail

airflow connections delete "${POSTGRES_CONN_ID}" >/dev/null 2>&1 || true
airflow connections add "${POSTGRES_CONN_ID}" \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST}" \
  --conn-port "${POSTGRES_PORT}" \
  --conn-schema "${POSTGRES_DB_AIRQUALITY}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-extra "{\"sslmode\":\"disable\"}"

VERIFY_TLS=false
if [[ "${MINIO_SECURE}" == "true" ]]; then
  VERIFY_TLS=true
fi

airflow connections delete "${MINIO_CONN_ID}" >/dev/null 2>&1 || true
airflow connections add "${MINIO_CONN_ID}" \
  --conn-type aws \
  --conn-login "${MINIO_ACCESS_KEY}" \
  --conn-password "${MINIO_SECRET_KEY}" \
  --conn-extra "{\"endpoint_url\":\"${MINIO_ENDPOINT_URL}\",\"aws_access_key_id\":\"${MINIO_ACCESS_KEY}\",\"aws_secret_access_key\":\"${MINIO_SECRET_KEY}\",\"region_name\":\"${AWS_DEFAULT_REGION}\",\"verify\":${VERIFY_TLS}}"
'
then
  echo "Warning: failed to configure Airflow connections via kubectl exec; continuing without blocking deployment." >&2
  exit 0
fi

echo "Airflow connections configured: ${POSTGRES_CONN_ID}, ${MINIO_CONN_ID}."
