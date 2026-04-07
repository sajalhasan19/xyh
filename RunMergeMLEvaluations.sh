#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/ml_settings_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage: RunMergeMLEvaluations.sh <xyh_binary_x<mass>_y<mass>>

Example:
  RunMergeMLEvaluations.sh xyh_binary_x500_y350
EOF
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage

if [[ $ml_model =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
  mass_x=${BASH_REMATCH[1]}
  mass_y=${BASH_REMATCH[2]}
else
  echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
  exit 1
fi

version="${VERSION:-ml_v1}"
config="${CONFIG:-config_2022post}"
selector="${SELECTOR:-default}"
producers="${PRODUCERS:-default}"
workers="${WORKERS:-4}"
# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
ml_settings="$(resolve_ml_settings "${ml_model}" "${training_categories}" "true" "false")"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

signal_dataset="xyh_sl_x${mass_x}_y${mass_y}_madgraph"

datasets=(
  # signal
  "${signal_dataset}"
  # backgrounds
  ttz_zqq_1j_amcatnlo
  ttzz_madgraph
  ttww_madgraph
  tt_sl_powheg
  tt_dl_powheg
  tt_fh_powheg
  st_tchannel_t_4f_powheg
  st_tchannel_tbar_4f_powheg
  st_twchannel_t_sl_powheg
  st_twchannel_t_dl_powheg
  st_twchannel_t_fh_powheg
  st_twchannel_tbar_sl_powheg
  st_twchannel_tbar_dl_powheg
  st_twchannel_tbar_fh_powheg
  #dy_m4to10_amcatnlo
  dy_m10to50_amcatnlo
  #dy_m50toinf_amcatnlo
  dy_m50toinf_0j_amcatnlo
  dy_m50toinf_1j_amcatnlo
  dy_m50toinf_2j_amcatnlo
  w_lnu_amcatnlo
  ww_pythia
  wz_pythia
  zz_pythia
)

for dataset in "${datasets[@]}"; do
  echo
  echo "[+] Merging evaluation parquet for dataset: ${dataset}"
  law run cf.MergeMLEvaluation \
    --version "${version}" \
    --ml-model "${ml_model}" \
    ${ml_settings:+--ml-model-settings "${ml_settings}"} \
    --config "${config}" \
    --dataset "${dataset}" \
    --producers "${producers}" \
    --selector "${selector}" \
    --workers "${workers}" \
    --workers 50
done

echo
echo "[+] All merges submitted."
