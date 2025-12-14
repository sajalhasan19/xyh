#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: PlotNNScore.sh <xyh_binary_x<mass>_y<mass>> [--raw|--logit]

Examples:
  PlotNNScore.sh xyh_binary_x500_y350             # logit (default)
  PlotNNScore.sh xyh_binary_x500_y350 --raw       # raw score
  PlotNNScore.sh xyh_binary_x500_y350 --logit     # explicit logit
EOF
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage

variant="logit"
while [[ $# -ge 2 ]]; do
  case "$2" in
    --raw)
      variant="score"
      shift
      ;;
    --logit)
      variant="logit"
      shift
      ;;
    --variant=*)
      variant="${2#*=}"
      shift
      ;;
    --variant)
      variant="${3:-}"
      [[ -z $variant ]] && usage
      shift 2
      ;;
    *)
      echo "ERROR: unknown option '$2'." >&2
      usage
      ;;
  esac
done

if [[ $ml_model =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
  signal_suffix="x${BASH_REMATCH[1]}_y${BASH_REMATCH[2]}"
else
  echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
  exit 1
fi

signal_process="xyh_sl_${signal_suffix}"
signal_dataset="${signal_process}_madgraph"
process_group="signal_${signal_suffix}"

case "$variant" in
  logit)
    variable="logit_nn_score__${ml_model}"
    ;;
  score|"")
    variable="nn_score__${ml_model}"
    ;;
  *)
    echo "ERROR: unknown variant '${variant}'. Supported: score, logit." >&2
    exit 1
    ;;
esac

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

law run cf.PlotVariables1D \
  --version "${VERSION:-ml_v1}" \
  --configs "${CONFIG:-config_2022pre}" \
  --selector "${SELECTOR:-default}" \
  --producers "${PRODUCERS:-default}" \
  --ml-models "${ml_model}" \
  --processes "${signal_process},background" \
  --datasets "${signal_dataset},background" \
  --categories "${CATEGORIES:-cat_incl,1lep__2bjets__4jets,1lep__2bjets__5jets,1lep__2bjets__6jets,1lep__2bjets__g6jets,1lep__3bjets__4jets,1lep__3bjets__5jets,1lep__3bjets__6jets,1lep__3bjets__g6jets,1lep__4bjets__5jets,1lep__ge4bjets__ge6jets}" \
  --variables "${variable}" \
  --workers "${WORKERS:-30}" \
  --custom-style-config signals \
  --process-settings "xyh,unstack,color=#000000,histtype=step,linewidth=2,linestyle=--,zorder=1000,scale=stack"
  #--remove-output 0,a,y
  #--categories "${CATEGORIES:-1e__2bjets__4jets,1e__2bjets__5jets,1e__3bjets__4jets,1e__3bjets__5jets,1e__3bjets__6jets,1e__4bjets__5jets,1e__4bjets__6jets,1mu__2bjets__4jets,1mu__2bjets__5jets,1mu__3bjets__4jets,1mu__3bjets__5jets,1mu__3bjets__6jets,1mu__4bjets__5jets,1mu__4bjets__6jets}" \
