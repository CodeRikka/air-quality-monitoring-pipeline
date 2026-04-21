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

require_var MINIO_ACCESS_KEY
require_var MINIO_SECRET_KEY

MINIO_ENDPOINT="${MINIO_ENDPOINT:-minio.storage.svc.cluster.local:9000}"
MINIO_SECURE="${MINIO_SECURE:-false}"
MINIO_SCHEME="http"
if [[ "${MINIO_SECURE}" == "true" ]]; then
  MINIO_SCHEME="https"
fi

NAMESPACE="storage"
JOB_NAME="aq-minio-bucket-init"

microk8s kubectl rollout status -n "${NAMESPACE}" statefulset/minio --timeout=180s
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
              mc mb --ignore-existing local/aqs-raw
              mc mb --ignore-existing local/airnow-raw
              mc mb --ignore-existing local/pipeline-artifacts
              mc mb --ignore-existing local/postgres-backups
              mc ls local
EOF

microk8s kubectl wait --for=condition=complete job/"${JOB_NAME}" -n "${NAMESPACE}" --timeout=180s
echo "MinIO buckets are ready: aqs-raw, airnow-raw, pipeline-artifacts, postgres-backups."
