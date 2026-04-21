#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

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

require_var POSTGRES_PASSWORD
require_var MINIO_ACCESS_KEY
require_var MINIO_SECRET_KEY

STORAGE_NAMESPACE="${STORAGE_NAMESPACE:-storage}"
AIRFLOW_NAMESPACE="${AIRFLOW_NAMESPACE:-airflow}"

POSTGRES_HOST="${POSTGRES_HOST:-postgres-rw.storage.svc.cluster.local}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-admin}"
POSTGRES_DB_AIRQUALITY="${POSTGRES_DB_AIRQUALITY:-airquality}"

POSTGRES_RESET_JOB="${POSTGRES_RESET_JOB:-aq-db-reset}"
MINIO_RESET_JOB="${MINIO_RESET_JOB:-aq-minio-reset}"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-minio.storage.svc.cluster.local:9000}"
MINIO_SECURE="${MINIO_SECURE:-false}"
MINIO_SCHEME="http"
if [[ "${MINIO_SECURE}" == "true" ]]; then
  MINIO_SCHEME="https"
fi

AIRFLOW_SCHEDULER_DEPLOYMENT="${AIRFLOW_SCHEDULER_DEPLOYMENT:-airflow-scheduler}"
AIRFLOW_RESET_METADATA="${AIRFLOW_RESET_METADATA:-true}"
AIRFLOW_ADMIN_USERNAME="${AIRFLOW_ADMIN_USERNAME:-admin}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}"
AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"
AIRFLOW_ADMIN_FIRSTNAME="${AIRFLOW_ADMIN_FIRSTNAME:-admin}"
AIRFLOW_ADMIN_LASTNAME="${AIRFLOW_ADMIN_LASTNAME:-user}"
AIRFLOW_WAIT_TIMEOUT="${AIRFLOW_WAIT_TIMEOUT:-300s}"
DB_RESET_MODE="${DB_RESET_MODE:-rebuild}"

if [[ "${DB_RESET_MODE}" != "rebuild" && "${DB_RESET_MODE}" != "truncate" ]]; then
  echo "Error: DB_RESET_MODE must be 'rebuild' or 'truncate' (got: ${DB_RESET_MODE})." >&2
  exit 1
fi

echo "[1/4] Resetting airquality database objects (mode=${DB_RESET_MODE}) ..."
microk8s kubectl delete job "${POSTGRES_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found

cat <<EOF | microk8s kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${POSTGRES_RESET_JOB}
  namespace: ${STORAGE_NAMESPACE}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: psql
          image: postgres:16
          env:
            - name: PGPASSWORD
              value: "${POSTGRES_PASSWORD}"
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              if [ "${DB_RESET_MODE}" = "rebuild" ]; then
                psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS staging CASCADE; DROP SCHEMA IF EXISTS core CASCADE; DROP SCHEMA IF EXISTS mart CASCADE; DROP SCHEMA IF EXISTS ops CASCADE;"
                psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'staging', 'core', 'mart', 'ops') ORDER BY schema_name;"
              else
                TRUNCATE_SQL="\$(psql -v ON_ERROR_STOP=1 -tA -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "SELECT CASE WHEN COUNT(*) = 0 THEN '' ELSE 'TRUNCATE TABLE ' || string_agg(format('%I.%I', schemaname, tablename), ', ' ORDER BY schemaname, tablename) || ' RESTART IDENTITY CASCADE;' END FROM pg_tables WHERE schemaname IN ('raw', 'staging', 'core', 'ops');")"
                if [ -n "\${TRUNCATE_SQL}" ]; then
                  psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "\${TRUNCATE_SQL}"
                fi
                psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('raw', 'staging', 'core', 'ops') ORDER BY schemaname, tablename;"
              fi
EOF

if ! microk8s kubectl wait --for=condition=complete job/"${POSTGRES_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --timeout=180s; then
  echo "Error: ${POSTGRES_RESET_JOB} failed. Dumping job logs:" >&2
  microk8s kubectl logs -n "${STORAGE_NAMESPACE}" job/"${POSTGRES_RESET_JOB}" --tail=200 || true
  microk8s kubectl describe job -n "${STORAGE_NAMESPACE}" "${POSTGRES_RESET_JOB}" || true
  exit 1
fi
microk8s kubectl logs -n "${STORAGE_NAMESPACE}" job/"${POSTGRES_RESET_JOB}"

if [[ "${DB_RESET_MODE}" == "rebuild" ]]; then
  echo "[1.5/4] Rebuilding latest database schemas/tables/views ..."
  bash "${REPO_ROOT}/scripts/20-init-db.sh" "${ENV_FILE}"
fi

echo "[2/4] Resetting MinIO buckets ..."
microk8s kubectl rollout status -n "${STORAGE_NAMESPACE}" statefulset/minio --timeout=180s
microk8s kubectl delete job "${MINIO_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found

cat <<EOF | microk8s kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${MINIO_RESET_JOB}
  namespace: ${STORAGE_NAMESPACE}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mc
          image: minio/mc:RELEASE.2025-04-16T18-13-26Z
          env:
            - name: MINIO_SCHEME
              value: "${MINIO_SCHEME}"
            - name: MINIO_ENDPOINT
              value: "${MINIO_ENDPOINT}"
            - name: MINIO_ACCESS_KEY
              value: "${MINIO_ACCESS_KEY}"
            - name: MINIO_SECRET_KEY
              value: "${MINIO_SECRET_KEY}"
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              mc alias set local "\${MINIO_SCHEME}://\${MINIO_ENDPOINT}" "\${MINIO_ACCESS_KEY}" "\${MINIO_SECRET_KEY}"
              for bucket in aqs-raw airnow-raw pipeline-artifacts postgres-backups; do
                mc rb --force "local/\${bucket}" >/dev/null 2>&1 || true
                mc mb --ignore-existing "local/\${bucket}"
              done
              mc ls local
EOF

if ! microk8s kubectl wait --for=condition=complete job/"${MINIO_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --timeout=180s; then
  echo "Error: ${MINIO_RESET_JOB} failed. Dumping job logs:" >&2
  microk8s kubectl logs -n "${STORAGE_NAMESPACE}" job/"${MINIO_RESET_JOB}" --tail=200 || true
  microk8s kubectl describe job -n "${STORAGE_NAMESPACE}" "${MINIO_RESET_JOB}" || true
  exit 1
fi
microk8s kubectl logs -n "${STORAGE_NAMESPACE}" job/"${MINIO_RESET_JOB}"

echo "[3/4] Resetting Airflow metadata/history ..."
airflow_metadata_reset_performed="false"
if [[ "${AIRFLOW_RESET_METADATA}" == "true" ]]; then
  if microk8s kubectl get deployment "${AIRFLOW_SCHEDULER_DEPLOYMENT}" -n "${AIRFLOW_NAMESPACE}" >/dev/null 2>&1; then
    microk8s kubectl rollout status deployment/"${AIRFLOW_SCHEDULER_DEPLOYMENT}" -n "${AIRFLOW_NAMESPACE}" --timeout="${AIRFLOW_WAIT_TIMEOUT}"
    if ! microk8s kubectl exec -n "${AIRFLOW_NAMESPACE}" deployment/"${AIRFLOW_SCHEDULER_DEPLOYMENT}" -- env \
      AIRFLOW_ADMIN_USERNAME="${AIRFLOW_ADMIN_USERNAME}" \
      AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD}" \
      AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL}" \
      AIRFLOW_ADMIN_FIRSTNAME="${AIRFLOW_ADMIN_FIRSTNAME}" \
      AIRFLOW_ADMIN_LASTNAME="${AIRFLOW_ADMIN_LASTNAME}" \
      bash -lc '
set -euo pipefail
airflow db reset -y
airflow db migrate
airflow users create \
  --role Admin \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --email "${AIRFLOW_ADMIN_EMAIL}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}"
'
    then
      echo "Warning: Airflow metadata reset failed; database rebuild has completed and can still be used." >&2
    fi

    if [[ -f "${REPO_ROOT}/infra/local/07-configure-airflow-connections.sh" ]]; then
      if ! bash "${REPO_ROOT}/infra/local/07-configure-airflow-connections.sh" "${ENV_FILE}"; then
        echo "Warning: Airflow connection reconfigure script failed; core DB rebuild result remains valid." >&2
      fi
    else
      echo "Warning: skipped Airflow connection reconfigure script (file not found)." >&2
    fi
    airflow_metadata_reset_performed="true"
  else
    echo "Warning: skipped Airflow metadata reset because deployment/${AIRFLOW_SCHEDULER_DEPLOYMENT} was not found in namespace ${AIRFLOW_NAMESPACE}." >&2
  fi
else
  echo "Skipped Airflow metadata reset (AIRFLOW_RESET_METADATA=${AIRFLOW_RESET_METADATA})."
fi

echo "[4/4] Cleaning completed reset jobs ..."
microk8s kubectl delete job "${POSTGRES_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found >/dev/null
microk8s kubectl delete job "${MINIO_RESET_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found >/dev/null

echo "Reset complete. State now matches latest baseline: database rebuilt=${DB_RESET_MODE/rebuild/true}, empty MinIO buckets, airflow metadata reset=${airflow_metadata_reset_performed}."
