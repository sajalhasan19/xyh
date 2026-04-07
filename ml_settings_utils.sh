#!/usr/bin/env bash
set -euo pipefail

ml_settings_lock_dir="${ML_SETTINGS_DIR:-.ml_settings}"

ml_settings_lock_path() {
  local ml_model="$1"
  printf '%s/%s.txt' "${ml_settings_lock_dir}" "${ml_model}"
}

append_training_categories() {
  local settings="$1"
  local training_categories="$2"
  if [[ -n "${training_categories}" ]]; then
    if [[ -z "${settings}" ]]; then
      settings="training_categories=${training_categories}"
    elif [[ "${settings}" != *"training_categories="* ]]; then
      settings="${settings},training_categories=${training_categories}"
    fi
  fi
  printf '%s' "${settings}"
}

resolve_ml_settings() {
  local ml_model="$1"
  local training_categories="$2"
  local require_lock="${3:-false}"
  local allow_write="${4:-true}"

  local settings="${ML_SETTINGS:-}"
  settings="$(append_training_categories "${settings}" "${training_categories}")"

  local lock_path
  lock_path="$(ml_settings_lock_path "${ml_model}")"

  if [[ -f "${lock_path}" ]]; then
    local locked
    locked="$(cat "${lock_path}")"
    if [[ -n "${settings}" && "${settings}" != "${locked}" ]]; then
      echo "ERROR: ML_SETTINGS mismatch for ${ml_model}." >&2
      echo "  env:    ${settings}" >&2
      echo "  locked: ${locked}" >&2
      exit 1
    fi
    settings="${locked}"
  else
    if [[ "${require_lock}" == "true" && -z "${settings}" ]]; then
      echo "ERROR: ML_SETTINGS lock missing for ${ml_model}. Train first or set ML_SETTINGS." >&2
      exit 1
    fi
    if [[ "${allow_write}" == "true" ]]; then
      mkdir -p "${ml_settings_lock_dir}"
      printf '%s' "${settings}" > "${lock_path}"
    fi
  fi

  export ML_SETTINGS="${settings}"
  printf '%s' "${settings}"
}
