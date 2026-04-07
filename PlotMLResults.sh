#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/ml_settings_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage: PlotMLResults.sh <xyh_binary_x<mass>_y<mass>>

Example:
  PlotMLResults.sh xyh_binary_x500_y350
EOF
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage

if [[ $ml_model =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
  signal_suffix="x${BASH_REMATCH[1]}_y${BASH_REMATCH[2]}"
else
  echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
  exit 1
fi

signal_process="xyh_sl_${signal_suffix}"
signal_dataset="${signal_process}_madgraph"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
ml_settings="$(resolve_ml_settings "${ml_model}" "${training_categories}" "true" "false")"

law run cf.PlotMLResults \
  --version "${VERSION:-ml_unc_bin_x1000_y700_v2}" \
  --ml-model "${ml_model}" \
  ${ml_settings:+--ml-model-settings "${ml_settings}"} \
  --config "${CONFIG:-config_2022post}" \
  --selector "${SELECTOR:-default}" \
  --producers "${PRODUCERS:-default}" \
  --processes "${signal_process},background" \
  --datasets "${signal_dataset},background" \
  --categories "${CATEGORIES:-cat_incl,1lep__3bjets__4jets,1lep__3bjets__5jets,1lep__3bjets__ge6jets,1lep__4bjets__5jets,1lep__ge4bjets__ge6jets}" \
  --plot-function "${PLOT_FUNCTION:-cm}" \
  --workers "${WORKERS:-4}" --skip-ratio
