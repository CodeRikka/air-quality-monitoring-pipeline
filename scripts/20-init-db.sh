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

NAMESPACE="${STORAGE_NAMESPACE:-storage}"
JOB_NAME="${DB_INIT_JOB_NAME:-aq-db-init}"
CONFIGMAP_NAME="${DB_SQL_CONFIGMAP_NAME:-aq-db-sql}"

POSTGRES_HOST="${POSTGRES_HOST:-postgres-rw.storage.svc.cluster.local}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-admin}"
POSTGRES_DB_AIRQUALITY="${POSTGRES_DB_AIRQUALITY:-airquality}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

require_var POSTGRES_PASSWORD

microk8s kubectl create configmap "${CONFIGMAP_NAME}" \
  -n "${NAMESPACE}" \
  --from-file=sql/001_schemas.sql \
  --from-file=sql/010_dim_tables.sql \
  --from-file=sql/015_staging_tables.sql \
  --from-file=sql/020_core_tables.sql \
  --from-file=sql/030_ops_tables.sql \
  --from-file=sql/040_mart_views.sql \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

microk8s kubectl delete job "${JOB_NAME}" -n "${NAMESPACE}" --ignore-not-found

cat <<EOF | microk8s kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
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
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/001_schemas.sql
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/010_dim_tables.sql
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/015_staging_tables.sql
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/020_core_tables.sql
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/030_ops_tables.sql
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -c "DROP VIEW IF EXISTS mart.v_anomaly_spikes, mart.v_exceedance_events, mart.v_region_coverage, mart.v_seasonal_trend, mart.v_monthly_trend, mart.v_daily_trend, mart.v_latest_station_observation CASCADE"
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB_AIRQUALITY}" -f /sql/040_mart_views.sql
          volumeMounts:
            - name: sql
              mountPath: /sql
      volumes:
        - name: sql
          configMap:
            name: aq-db-sql
EOF

if ! microk8s kubectl wait --for=condition=complete job/"${JOB_NAME}" -n "${NAMESPACE}" --timeout=180s; then
  echo "Error: DB init job failed. Dumping job logs:" >&2
  microk8s kubectl logs -n "${NAMESPACE}" job/"${JOB_NAME}" || true
  exit 1
fi

microk8s kubectl logs -n "${NAMESPACE}" job/"${JOB_NAME}"
