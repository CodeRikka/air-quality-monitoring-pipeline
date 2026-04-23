#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-600}"
KUBECTL_WAIT_TIMEOUT="${WAIT_TIMEOUT_SECONDS}s"
export WAIT_TIMEOUT="${KUBECTL_WAIT_TIMEOUT}"
export AIRFLOW_WAIT_TIMEOUT="${AIRFLOW_WAIT_TIMEOUT:-${KUBECTL_WAIT_TIMEOUT}}"

wait_for_service_endpoint() {
  local namespace="$1"
  local service_name="$2"
  local description="$3"
  local start_time="${SECONDS}"
  local endpoint_ip=""

  while true; do
    endpoint_ip="$(microk8s kubectl get endpoints "${service_name}" -n "${namespace}" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || true)"
    if [[ -n "${endpoint_ip}" ]]; then
      return 0
    fi
    if (( SECONDS - start_time >= WAIT_TIMEOUT_SECONDS )); then
      echo "Error: ${description} did not expose a ready endpoint within ${KUBECTL_WAIT_TIMEOUT}." >&2
      exit 1
    fi
    sleep 5
  done
}

microk8s kubectl apply -f infra/base/namespaces.yaml
bash infra/prod/01-apply-secrets-from-env.sh "${ENV_FILE}"

bash infra/prod/02-rook-ceph.sh
bash infra/prod/03-cnpg-operator-install.sh
microk8s kubectl apply -f infra/prod/04-cnpg-cluster.yaml
microk8s kubectl apply -f infra/prod/05-minio-statefulset.yaml

microk8s kubectl wait --for=condition=Ready pod -n storage -l cnpg.io/cluster=postgres --timeout="${KUBECTL_WAIT_TIMEOUT}"
wait_for_service_endpoint "storage" "postgres-rw" "Postgres rw service"

bash scripts/20-init-db.sh "${ENV_FILE}"

microk8s kubectl rollout status -n storage statefulset/minio --timeout="${KUBECTL_WAIT_TIMEOUT}"
wait_for_service_endpoint "storage" "minio" "MinIO service"
bash infra/prod/06-init-minio-buckets.sh "${ENV_FILE}"

microk8s helm3 repo add apache-airflow https://airflow.apache.org --force-update
microk8s helm3 repo update
microk8s helm3 upgrade --install airflow apache-airflow/airflow \
  -n airflow \
  -f infra/prod/07-airflow-values.yaml \
  --timeout 15m0s

bash scripts/22-fix-airflow-metadata-sequences.sh "${ENV_FILE}"
microk8s kubectl rollout status deployment/airflow-api-server -n airflow --timeout="${KUBECTL_WAIT_TIMEOUT}"
microk8s kubectl rollout status deployment/airflow-scheduler -n airflow --timeout="${KUBECTL_WAIT_TIMEOUT}"
bash infra/prod/08-configure-airflow-connections.sh "${ENV_FILE}"

microk8s kubectl apply -f infra/prod/09-api-deployment.yaml
microk8s kubectl apply -f infra/prod/10-web-deployment.yaml

bash scripts/40-post-deploy-health-check.sh
