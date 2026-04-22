#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${1:-ghcr.io/coderikka}"
TAG="${2:-latest}"

if docker buildx inspect aq-builder >/dev/null 2>&1; then
  docker buildx use aq-builder
else
  docker buildx create --name aq-builder --use
fi

docker buildx inspect --bootstrap >/dev/null

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${REGISTRY}/aq-airflow:${TAG}" \
  -f airflow/Dockerfile \
  --push \
  airflow

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${REGISTRY}/aq-api:${TAG}" \
  -f api/Dockerfile \
  --push \
  api

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t "${REGISTRY}/aq-web:${TAG}" \
  -f web/Dockerfile \
  --push \
  web
