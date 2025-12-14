#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: RunMLEvaluations.sh <xyh_binary_x<mass>_y<mass>>

Example:
  RunMLEvaluations.sh xyh_binary_x500_y350
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
config="${CONFIG:-config_2022pre}"
selector="${SELECTOR:-default}"
producers="${PRODUCERS:-default}"
workers="${WORKERS:-4}"

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
  echo "[+] Evaluating dataset: ${dataset}"
  law run cf.MLEvaluation \
    --version "${version}" \
    --ml-model "${ml_model}" \
    --config "${config}" \
    --selector "${selector}" \
    --producers "${producers}" \
    --workers "${workers}" \
    --dataset "${dataset}" \
    --workers 30
done

echo
echo "[+] All evaluations submitted."
