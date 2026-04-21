#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${1:-ghcr.io/coderikka}"
TAG="${2:-latest}"
NAMESPACE="${3:-serving}"
TMP_TAR="/tmp/aq-serving-images-${TAG}.tar"

API_IMAGE="${REGISTRY}/aq-api:${TAG}"
WEB_IMAGE="${REGISTRY}/aq-web:${TAG}"

echo "[1/5] Building ${API_IMAGE}"
docker build -t "${API_IMAGE}" -f api/Dockerfile api

echo "[2/5] Building ${WEB_IMAGE}"
docker build -t "${WEB_IMAGE}" -f web/Dockerfile web

echo "[3/5] Exporting images to ${TMP_TAR}"
docker save "${API_IMAGE}" "${WEB_IMAGE}" -o "${TMP_TAR}"

echo "[4/5] Importing images into MicroK8s"
microk8s images import "${TMP_TAR}"

echo "[5/5] Applying local serving manifests and restarting deployments"
microk8s kubectl apply -f infra/local/08-api-deployment.yaml
microk8s kubectl apply -f infra/local/09-web-deployment.yaml
microk8s kubectl rollout restart deployment/aq-api -n "${NAMESPACE}"
microk8s kubectl rollout restart deployment/aq-web -n "${NAMESPACE}"
microk8s kubectl rollout status deployment/aq-api -n "${NAMESPACE}" --timeout=120s
microk8s kubectl rollout status deployment/aq-web -n "${NAMESPACE}" --timeout=120s

echo "Done."
echo "Active pods:"
microk8s kubectl get pods -n "${NAMESPACE}" -o wide
