#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_EXAMPLE="${REPO_ROOT}/.env.example"
ENV_TARGET="${REPO_ROOT}/.env"
ENV_PREVIOUS=""

# Secret values are never hard-coded in this repository. You can provide them per-run:
#   EPA_AQS_EMAIL=... EPA_AQS_KEY=... AIRNOW_API_KEY=... bash scripts/12-update-local-env.sh

sync_secrets="${SYNC_K8S_SECRETS:-true}"
restart_airflow="${RESTART_AIRFLOW_PODS:-true}"
full_reset_db="${FULL_RESET_DB:-true}"
db_reset_mode="${DB_RESET_MODE:-rebuild}"
reset_airflow_metadata="${RESET_AIRFLOW_METADATA:-true}"
build_and_load_images="${BUILD_AND_LOAD_IMAGES:-true}"
image_load_with_sudo="${IMAGE_LOAD_WITH_SUDO:-false}"
run_post_deploy_healthcheck="${RUN_POST_DEPLOY_HEALTHCHECK:-true}"

if [[ ! -f "${ENV_EXAMPLE}" ]]; then
  echo "Error: ${ENV_EXAMPLE} not found." >&2
  exit 1
fi

get_env_file_value() {
  local env_file="$1"
  local key="$2"

  if [[ -z "${env_file}" || ! -f "${env_file}" ]]; then
    return 1
  fi

  local line
  line="$(grep -m1 "^${key}=" "${env_file}" || true)"
  if [[ -z "${line}" ]]; then
    return 1
  fi

  printf '%s\n' "${line#*=}"
}

resolve_secret_value() {
  local key="$1"
  local runtime_value="${!key:-}"

  if [[ -n "${runtime_value}" ]]; then
    printf '%s\n' "${runtime_value}"
    return 0
  fi

  if [[ -n "${ENV_PREVIOUS}" ]]; then
    get_env_file_value "${ENV_PREVIOUS}" "${key}" || true
  fi
}

if [[ -f "${ENV_TARGET}" ]]; then
  ENV_PREVIOUS="${ENV_TARGET}.bak.$(date +%Y%m%d%H%M%S)"
  cp "${ENV_TARGET}" "${ENV_PREVIOUS}"
fi

cp "${ENV_EXAMPLE}" "${ENV_TARGET}"

set_env_value() {
  local key="$1"
  local value="$2"
  local escaped_value
  escaped_value="$(printf '%s' "${value}" | sed 's/[&|]/\\&/g')"
  if grep -q "^${key}=" "${ENV_TARGET}"; then
    sed -i "s|^${key}=.*|${key}=${escaped_value}|" "${ENV_TARGET}"
  else
    echo "${key}=${value}" >> "${ENV_TARGET}"
  fi
}

EPA_AQS_EMAIL_VALUE="$(resolve_secret_value "EPA_AQS_EMAIL")"
EPA_AQS_KEY_VALUE="$(resolve_secret_value "EPA_AQS_KEY")"
AIRNOW_API_KEY_VALUE="$(resolve_secret_value "AIRNOW_API_KEY")"

if [[ -n "${EPA_AQS_EMAIL_VALUE}" ]]; then
  set_env_value "EPA_AQS_EMAIL" "${EPA_AQS_EMAIL_VALUE}"
fi

if [[ -n "${EPA_AQS_KEY_VALUE}" ]]; then
  set_env_value "EPA_AQS_KEY" "${EPA_AQS_KEY_VALUE}"
fi

if [[ -n "${AIRNOW_API_KEY_VALUE}" ]]; then
  set_env_value "AIRNOW_API_KEY" "${AIRNOW_API_KEY_VALUE}"
fi

if [[ -z "${EPA_AQS_EMAIL_VALUE}" || -z "${EPA_AQS_KEY_VALUE}" || -z "${AIRNOW_API_KEY_VALUE}" ]]; then
  echo "Warning: one or more API credential values were not provided."
  echo "         .env keeps the placeholder value from .env.example for any missing key."
  echo "         Export EPA_AQS_EMAIL, EPA_AQS_KEY, and AIRNOW_API_KEY before rerunning if needed."
fi

echo "[1/7] .env regenerated from .env.example and secret values preserved when available."

if [[ "${sync_secrets}" == "true" ]]; then
  echo "[2/7] Syncing Kubernetes secrets from .env ..."
  bash "${REPO_ROOT}/infra/local/01-apply-secrets-from-env.sh" "${ENV_TARGET}"
else
  echo "[2/7] Skipped secret sync (SYNC_K8S_SECRETS=${sync_secrets})."
fi

if [[ "${full_reset_db}" == "true" ]]; then
  echo "[3/7] Fully resetting and rebuilding database (DB_RESET_MODE=${db_reset_mode}, RESET_AIRFLOW_METADATA=${reset_airflow_metadata}) ..."
  DB_RESET_MODE="${db_reset_mode}" AIRFLOW_RESET_METADATA="${reset_airflow_metadata}" bash "${REPO_ROOT}/scripts/21-reset-db.sh" "${ENV_TARGET}"
else
  echo "[3/7] Re-initializing database objects without full reset ..."
  bash "${REPO_ROOT}/scripts/20-init-db.sh" "${ENV_TARGET}"
fi

if [[ "${build_and_load_images}" == "true" ]]; then
  echo "[4/7] Rebuilding and loading local images ..."
  if [[ "${image_load_with_sudo}" == "true" ]]; then
    sudo bash "${REPO_ROOT}/scripts/30-build-and-load-local-images.sh"
  else
    bash "${REPO_ROOT}/scripts/30-build-and-load-local-images.sh"
  fi
else
  echo "[4/7] Skipped image build/load (BUILD_AND_LOAD_IMAGES=${build_and_load_images})."
fi

if [[ "${restart_airflow}" == "true" ]]; then
  echo "[5/7] Restarting Airflow pods ..."
  microk8s kubectl delete pod -n airflow --all
else
  echo "[5/7] Skipped Airflow pod restart (RESTART_AIRFLOW_PODS=${restart_airflow})."
fi

echo "[6/8] Reconfiguring Airflow hook connections ..."
bash "${REPO_ROOT}/infra/local/07-configure-airflow-connections.sh" "${ENV_TARGET}"

if [[ "${run_post_deploy_healthcheck}" == "true" ]]; then
  echo "[7/8] Running post-deploy health check ..."
  bash "${REPO_ROOT}/scripts/40-post-deploy-health-check.sh"
else
  echo "[7/8] Skipped post-deploy health check (RUN_POST_DEPLOY_HEALTHCHECK=${run_post_deploy_healthcheck})."
fi

echo "[8/8] Done."
echo "Environment update completed."
