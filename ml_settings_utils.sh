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

xyh_pnn_reprs() {
  local ml_model="$1"
  local settings="$2"
  local cond_masses="${3:-}"
  local eval_mass_x="${4:-}"
  local eval_mass_y="${5:-}"

  XYH_REPR_MODEL="${ml_model}" \
  XYH_REPR_SETTINGS="${settings}" \
  XYH_REPR_COND_MASSES="${cond_masses}" \
  XYH_REPR_EVAL_MASS_X="${eval_mass_x}" \
  XYH_REPR_EVAL_MASS_Y="${eval_mass_y}" \
  python - <<'PY'
import os
from collections import OrderedDict

model = os.environ["XYH_REPR_MODEL"]
settings = os.environ.get("XYH_REPR_SETTINGS", "")

if model != "xyh_pnn_parameterized":
    print(f"{model}\t{model}")
    raise SystemExit

import law
from columnflow.tasks.framework.parameters import SettingsParameter
from xyh.ml.xyh_pnn import XYH_PNN

params = OrderedDict()
if settings:
    params.update(dict(SettingsParameter().parse(settings)))

if params.get("conditioning_masses") in (None, "", ()):
    cond_masses = os.environ.get("XYH_REPR_COND_MASSES", "")
    if cond_masses:
        params["conditioning_masses"] = cond_masses

eval_mass_x = os.environ.get("XYH_REPR_EVAL_MASS_X", "")
eval_mass_y = os.environ.get("XYH_REPR_EVAL_MASS_Y", "")
if params.get("eval_mass_x") is None and eval_mass_x:
    params["eval_mass_x"] = eval_mass_x
if params.get("eval_mass_y") is None and eval_mass_y:
    params["eval_mass_y"] = eval_mass_y

for name, value in XYH_PNN.default_parameters.items():
    params.setdefault(name, value)

def format_value(value):
    if isinstance(value, (list, tuple)):
        return "_".join(map(format_value, value))
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value}" if value >= 0.01 else f"{value:.2e}"
    return str(value)

def make_repr(pairs):
    joined = "__".join(f"{name}_{format_value(value)}" for name, value in pairs)
    return f"{model}__{law.util.create_hash(joined)}" if joined else model

all_pairs = sorted(params.items())

mode = str(params.get("background_mass_mode", "random")).strip().lower()
if mode == "fixed" and params.get("background_mass_value") is None:
    eval_only = set()
else:
    eval_only = {"eval_mass_x", "eval_mass_y"}

eval_only |= {
    "shap_enabled",
    "shap_max_events",
    "shap_background_size",
    "shap_kernel_nsamples",
    "shap_plot_top_n",
    "shap_on_validation",
}
training_pairs = [(name, value) for name, value in all_pairs if name not in eval_only]

print(f"{make_repr(all_pairs)}\t{make_repr(training_pairs)}")
PY
}

record_datacard_mass_map() {
  local ml_model="$1"
  local settings="$2"
  local mass_point="$3"
  local signal_process="$4"
  local version="$5"
  local datacard_variable="${6:-}"
  local cond_masses="${7:-}"
  local eval_mass_x="${8:-}"
  local eval_mass_y="${9:-}"

  local reprs ml_model_repr training_model_repr
  reprs="$(xyh_pnn_reprs "${ml_model}" "${settings}" "${cond_masses}" "${eval_mass_x}" "${eval_mass_y}")"
  ml_model_repr="${reprs%%$'\t'*}"
  training_model_repr="${reprs#*$'\t'}"

  local manifest_dir="${DATACARD_MANIFEST_DIR:-datacard_manifests}"
  local manifest="${manifest_dir}/ml_mass_map.tsv"
  mkdir -p "${manifest_dir}"
  if [[ ! -f "${manifest}" ]]; then
    printf 'created_at\tml_dir\ttraining_ml_dir\tmass_point\tsignal_process\tmodel\tversion\tdatacard_variable\n' > "${manifest}"
  fi

  printf '%s\tml__%s\tml__%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date +%Y-%m-%dT%H:%M:%S%z)" \
    "${ml_model_repr}" \
    "${training_model_repr}" \
    "${mass_point}" \
    "${signal_process}" \
    "${ml_model}" \
    "${version}" \
    "${datacard_variable}" >> "${manifest}"

  echo "[+] Recorded datacard mass map: ml__${ml_model_repr} -> ${mass_point}"
}
