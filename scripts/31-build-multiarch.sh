#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${1:-ghcr.io/coderikka}"
TAG="${2:-latest}"

docker buildx create --use --name aq-builder || true

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
