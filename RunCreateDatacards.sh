#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/ml_settings_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage: RunCreateDatacards.sh <xyh_binary_x<mass>_y<mass>>

Example:
  RunCreateDatacards.sh xyh_binary_x500_y350
EOF
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage

if [[ ! $ml_model =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
  echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
  exit 1
fi

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
ml_settings="$(resolve_ml_settings "${ml_model}" "${training_categories}" "true" "false")"

signal_process="xyh_sl_${ml_model#xyh_binary_}"

# Ensure the inference model knows which signal mass point to use.
export XYH_SIGNAL_PROCESS="${signal_process}"

law run cf.CreateDatacards \
  --version "${VERSION:-no_unc}" \
  --configs "${CONFIGS:-config_2022post}" \
  --selector "${SELECTOR:-default}" \
  --producers "${PRODUCERS:-default}" \
  --ml-models "${ml_model}" \
  --inference-model "${INFERENCE_MODEL:-xyh_limits}" \
  --workers "${WORKERS:-30}" |& tee dc_mx.log
  #--job-workers "${WORKERS:-30}" --workflow htcondor
