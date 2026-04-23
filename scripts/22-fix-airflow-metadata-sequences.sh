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

KUBECTL_BACKEND_TLS_FLAG="${KUBECTL_BACKEND_TLS_FLAG:---insecure-skip-tls-verify-backend=true}"
STORAGE_NAMESPACE="${STORAGE_NAMESPACE:-storage}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres-rw.storage.svc.cluster.local}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DB_AIRFLOW="${POSTGRES_DB_AIRFLOW:-airflow}"
POSTGRES_METADATA_USER="${POSTGRES_METADATA_USER:-postgres}"
POSTGRES_METADATA_PASSWORD="${POSTGRES_METADATA_PASSWORD:-${POSTGRES_SUPERUSER_PASSWORD:-${POSTGRES_PASSWORD:-}}}"
SEQUENCE_FIX_JOB="${SEQUENCE_FIX_JOB:-airflow-metadata-sequence-fix}"

require_var POSTGRES_METADATA_PASSWORD

microk8s kubectl delete job "${SEQUENCE_FIX_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found >/dev/null

cat <<EOF | microk8s kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${SEQUENCE_FIX_JOB}
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
              value: "${POSTGRES_METADATA_PASSWORD}"
          command: ["/bin/sh", "-c"]
          args:
            - |
              set -e
              psql -v ON_ERROR_STOP=1 -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_METADATA_USER}" -d "${POSTGRES_DB_AIRFLOW}" <<'SQL'
              SELECT CASE
                WHEN to_regclass('public.job') IS NULL THEN NULL
                WHEN pg_get_serial_sequence('public.job', 'id') IS NULL THEN NULL
                ELSE setval(
                  pg_get_serial_sequence('public.job', 'id'),
                  COALESCE((SELECT MAX(id) FROM public.job), 1),
                  true
                )
              END;
              SQL
EOF

if ! microk8s kubectl wait --for=condition=complete job/"${SEQUENCE_FIX_JOB}" -n "${STORAGE_NAMESPACE}" --timeout=180s; then
  echo "Error: ${SEQUENCE_FIX_JOB} failed. Dumping job logs:" >&2
  microk8s kubectl logs ${KUBECTL_BACKEND_TLS_FLAG} -n "${STORAGE_NAMESPACE}" job/"${SEQUENCE_FIX_JOB}" --tail=200 || true
  microk8s kubectl describe job -n "${STORAGE_NAMESPACE}" "${SEQUENCE_FIX_JOB}" || true
  exit 1
fi

microk8s kubectl logs ${KUBECTL_BACKEND_TLS_FLAG} -n "${STORAGE_NAMESPACE}" job/"${SEQUENCE_FIX_JOB}" || true
microk8s kubectl delete job "${SEQUENCE_FIX_JOB}" -n "${STORAGE_NAMESPACE}" --ignore-not-found >/dev/null

echo "Airflow metadata sequences verified."
