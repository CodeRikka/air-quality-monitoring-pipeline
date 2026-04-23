#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env}"

microk8s kubectl apply -f infra/base/namespaces.yaml
bash infra/local/01-apply-secrets-from-env.sh "${ENV_FILE}"

bash infra/local/02-cnpg-operator-install.sh
microk8s kubectl apply -f infra/local/03-cnpg-cluster.yaml
microk8s kubectl apply -f infra/local/04-minio-statefulset.yaml
bash infra/local/05-init-minio-buckets.sh

microk8s helm3 repo add apache-airflow https://airflow.apache.org
microk8s helm3 repo update
microk8s helm3 upgrade --install airflow apache-airflow/airflow \
  -n airflow \
  -f infra/local/06-airflow-values.yaml \
  --timeout 15m0s
bash scripts/22-fix-airflow-metadata-sequences.sh "${ENV_FILE}"
bash infra/local/07-configure-airflow-connections.sh "${ENV_FILE}"

microk8s kubectl apply -f infra/local/08-api-deployment.yaml
microk8s kubectl apply -f infra/local/09-web-deployment.yaml
