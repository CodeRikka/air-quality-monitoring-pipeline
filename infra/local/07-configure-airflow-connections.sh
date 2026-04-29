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
KUBECTL_BACKEND_TLS_FLAG="${KUBECTL_BACKEND_TLS_FLAG:---insecure-skip-tls-verify-backend=true}"
AIRFLOW_CONNECTIONS_JOB="${AIRFLOW_CONNECTIONS_JOB:-airflow-connections-bootstrap}"

MINIO_SCHEME="http"
if [[ "${MINIO_SECURE}" == "true" ]]; then
  MINIO_SCHEME="https"
fi
MINIO_ENDPOINT_URL="${MINIO_SCHEME}://${MINIO_ENDPOINT}"

microk8s kubectl rollout status deployment/airflow-scheduler -n airflow --timeout="${AIRFLOW_WAIT_TIMEOUT}"
AIRFLOW_IMAGE="$(microk8s kubectl get deployment airflow-scheduler -n airflow -o jsonpath='{.spec.template.spec.containers[?(@.name=="scheduler")].image}')"

if [[ -z "${AIRFLOW_IMAGE}" ]]; then
  echo "Error: could not determine Airflow scheduler image in namespace airflow." >&2
  exit 1
fi

microk8s kubectl delete job "${AIRFLOW_CONNECTIONS_JOB}" -n airflow --ignore-not-found >/dev/null

cat <<EOF | microk8s kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${AIRFLOW_CONNECTIONS_JOB}
  namespace: airflow
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: airflow-connections
          image: ${AIRFLOW_IMAGE}
          env:
            - name: AIRFLOW_HOME
              value: /opt/airflow
            - name: AIRFLOW__CORE__FERNET_KEY
              valueFrom:
                secretKeyRef:
                  name: airflow-fernet-key
                  key: fernet-key
            - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
              valueFrom:
                secretKeyRef:
                  name: airflow-metadata
                  key: connection
            - name: AIRFLOW_CONN_AIRFLOW_DB
              valueFrom:
                secretKeyRef:
                  name: airflow-metadata
                  key: connection
            - name: AIRFLOW__API__SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: airflow-api-secret-key
                  key: api-secret-key
            - name: POSTGRES_CONN_ID
              value: "${POSTGRES_CONN_ID}"
            - name: POSTGRES_HOST
              value: "${POSTGRES_HOST}"
            - name: POSTGRES_PORT
              value: "${POSTGRES_PORT}"
            - name: POSTGRES_DB_AIRQUALITY
              value: "${POSTGRES_DB_AIRQUALITY}"
            - name: POSTGRES_USER
              value: "${POSTGRES_USER}"
            - name: POSTGRES_PASSWORD
              value: "${POSTGRES_PASSWORD}"
            - name: MINIO_CONN_ID
              value: "${MINIO_CONN_ID}"
            - name: MINIO_ACCESS_KEY
              value: "${MINIO_ACCESS_KEY}"
            - name: MINIO_SECRET_KEY
              value: "${MINIO_SECRET_KEY}"
            - name: MINIO_ENDPOINT_URL
              value: "${MINIO_ENDPOINT_URL}"
            - name: AWS_DEFAULT_REGION
              value: "${AWS_DEFAULT_REGION}"
            - name: MINIO_SECURE
              value: "${MINIO_SECURE}"
          command: ["/bin/bash", "-lc"]
          args:
            - |
              set -euo pipefail

              airflow connections delete "${POSTGRES_CONN_ID}" >/dev/null 2>&1 || true
              airflow connections add "${POSTGRES_CONN_ID}" \
                --conn-type postgres \
                --conn-host "${POSTGRES_HOST}" \
                --conn-port "${POSTGRES_PORT}" \
                --conn-schema "${POSTGRES_DB_AIRQUALITY}" \
                --conn-login "${POSTGRES_USER}" \
                --conn-password "${POSTGRES_PASSWORD}" \
                --conn-extra '{"sslmode":"disable"}'

              VERIFY_TLS=false
              if [[ "${MINIO_SECURE}" == "true" ]]; then
                VERIFY_TLS=true
              fi

              airflow connections delete "${MINIO_CONN_ID}" >/dev/null 2>&1 || true
              airflow connections add "${MINIO_CONN_ID}" \
                --conn-type aws \
                --conn-login "${MINIO_ACCESS_KEY}" \
                --conn-password "${MINIO_SECRET_KEY}" \
                --conn-extra "{\"endpoint_url\":\"${MINIO_ENDPOINT_URL}\",\"aws_access_key_id\":\"${MINIO_ACCESS_KEY}\",\"aws_secret_access_key\":\"${MINIO_SECRET_KEY}\",\"region_name\":\"${AWS_DEFAULT_REGION}\",\"verify\":\${VERIFY_TLS}}"
EOF

if ! microk8s kubectl wait --for=condition=complete job/"${AIRFLOW_CONNECTIONS_JOB}" -n airflow --timeout="${AIRFLOW_WAIT_TIMEOUT}"; then
  echo "Error: ${AIRFLOW_CONNECTIONS_JOB} failed. Dumping job logs:" >&2
  microk8s kubectl logs ${KUBECTL_BACKEND_TLS_FLAG} -n airflow job/"${AIRFLOW_CONNECTIONS_JOB}" --tail=200 || true
  microk8s kubectl describe job -n airflow "${AIRFLOW_CONNECTIONS_JOB}" || true
  exit 1
fi

microk8s kubectl logs ${KUBECTL_BACKEND_TLS_FLAG} -n airflow job/"${AIRFLOW_CONNECTIONS_JOB}" || true
microk8s kubectl delete job "${AIRFLOW_CONNECTIONS_JOB}" -n airflow --ignore-not-found >/dev/null

echo "Airflow connections configured: ${POSTGRES_CONN_ID}, ${MINIO_CONN_ID}."
