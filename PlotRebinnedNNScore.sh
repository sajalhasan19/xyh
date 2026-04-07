#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOU'
Usage: PlotRebinnedNNScore.sh [--model <name>] [options]

Options:
  -v, --version <tag>             Defaults to $VERSION or ml_v3_binary_2022post_cat6
  -m, --model <name>              Process only the specified ML model (e.g. xyh_binary_x700_y500)
  -c, --config <name>             Defaults to $CONFIG or config_2022post
  -s, --selector <name>           Defaults to $SELECTOR or default
  -p, --producers <name>          Defaults to $PRODUCERS or default
  -i, --inference-model <name>    Defaults to $INFERENCE_MODEL or xyh_limits
  -w, --workers <n>               Defaults to $WORKERS or 50
  --raw                           Use nn_score__<model> (default)
  --logit                         Use logit_nn_score__<model>
  --variable <name>               Use an explicit variable (overrides --raw/--logit)
  --stacked                       Add stacked background with signal overlay (default)
  --no-stacked                    Disable stacked plots
  --custom-style-config <name>    Passed to law (defaults to $CUSTOM_STYLE_CONFIG)
  -h, --help                      Show this help

Notes:
  - Uses rebinned shapes produced by RunCreateDatacards.sh + Rebin.sh.
  - If you choose --logit/--variable, rebin with the same variable.
EOU
}

version="${VERSION:-ml_v3_binary_2022post_cat6}"
ml_settings="${ML_SETTINGS:-}"
# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
single_model=""
config="${CONFIG:-config_2022post}"
selector="${SELECTOR:-default}"
producers="${PRODUCERS:-default}"
inference_model="${INFERENCE_MODEL:-xyh_limits}"
workers="${WORKERS:-30}"
custom_style_config="${CUSTOM_STYLE_CONFIG:-}"
variant=""
variable_override=""
stacked=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version)         version="${2:-}"; shift 2 ;;
    -m|--model)           single_model="${2:-}"; shift 2 ;;
    -c|--config)          config="${2:-}"; shift 2 ;;
    -s|--selector)        selector="${2:-}"; shift 2 ;;
    -p|--producers)       producers="${2:-}"; shift 2 ;;
    -i|--inference-model) inference_model="${2:-}"; shift 2 ;;
    -w|--workers)         workers="${2:-}"; shift 2 ;;
    --raw)                variant="score"; shift ;;
    --logit)              variant="logit"; shift ;;
    --variable)           variable_override="${2:-}"; shift 2 ;;
    --stacked)            stacked=true; shift ;;
    --no-stacked)         stacked=false; shift ;;
    --custom-style-config) custom_style_config="${2:-}"; shift 2 ;;
    -h|--help)            usage; exit 0 ;;
    -* )
      echo "ERROR: unknown option '$1'" >&2
      usage; exit 1 ;;
    * )
      if [[ -z "${single_model}" ]]; then
        single_model="$1"
        shift
      else
        echo "ERROR: unexpected argument '$1'" >&2
        usage; exit 1
      fi
      ;;
  esac
done

if [[ -n "${version}" ]]; then
  export VERSION="${version}"
  echo "[+] Using VERSION='${VERSION}'"
fi

if [[ -n "${training_categories}" ]]; then
  if [[ -z "${ml_settings}" ]]; then
    ml_settings="training_categories=${training_categories}"
  elif [[ "${ml_settings}" != *"training_categories="* ]]; then
    ml_settings="${ml_settings},training_categories=${training_categories}"
  fi
fi
if [[ -n "${ml_settings}" ]]; then
  export ML_SETTINGS="${ml_settings}"
  echo "[+] Using ML_SETTINGS='${ML_SETTINGS}'"
fi

# Maintain the valid mass points yourself.
models=(
  xyh_binary_x500_y350
  xyh_binary_x550_y350
  xyh_binary_x550_y400
  xyh_binary_x600_y350
  xyh_binary_x600_y400
  xyh_binary_x600_y450
  xyh_binary_x650_y350
  xyh_binary_x650_y400
  xyh_binary_x650_y450
  xyh_binary_x650_y500
  xyh_binary_x700_y350
  xyh_binary_x700_y400
  xyh_binary_x700_y450
  xyh_binary_x700_y500
  xyh_binary_x700_y550
  xyh_binary_x750_y350
  xyh_binary_x750_y400
  xyh_binary_x750_y450
  xyh_binary_x750_y500
  xyh_binary_x750_y550
  xyh_binary_x750_y600
  xyh_binary_x800_y350
  xyh_binary_x800_y400
  xyh_binary_x800_y450
  xyh_binary_x800_y500
  xyh_binary_x800_y550
  xyh_binary_x800_y600
  xyh_binary_x800_y650
  xyh_binary_x850_y350
  xyh_binary_x850_y400
  xyh_binary_x850_y450
  xyh_binary_x850_y500
  xyh_binary_x850_y550
  xyh_binary_x850_y600
  xyh_binary_x850_y650
  xyh_binary_x850_y700
  xyh_binary_x900_y350
  xyh_binary_x900_y400
  xyh_binary_x900_y450
  xyh_binary_x900_y500
  xyh_binary_x900_y550
  xyh_binary_x900_y600
  xyh_binary_x900_y650
  xyh_binary_x900_y700
  xyh_binary_x900_y750
  xyh_binary_x950_y350
  xyh_binary_x950_y400
  xyh_binary_x950_y450
  xyh_binary_x950_y500
  xyh_binary_x950_y550
  xyh_binary_x950_y600
  xyh_binary_x950_y650
  xyh_binary_x950_y700
  xyh_binary_x950_y800
  xyh_binary_x1000_y350
  xyh_binary_x1000_y400
  xyh_binary_x1000_y450
  xyh_binary_x1000_y500
  xyh_binary_x1000_y550
  xyh_binary_x1000_y600
  xyh_binary_x1000_y650
  xyh_binary_x1000_y700
  xyh_binary_x1000_y800
  xyh_binary_x1200_y500
  xyh_binary_x1200_y600
  xyh_binary_x1200_y800
  xyh_binary_x1200_y1000
  xyh_binary_x1400_y600
  # add/remove entries here as needed
)

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

for model in "${models[@]}"; do
  if [[ -n "${single_model}" && "${model}" != "${single_model}" ]]; then
    continue
  fi

  if [[ ! ${model} =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
    echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${model}')." >&2
    exit 1
  fi

  signal_process="xyh_sl_${model#xyh_binary_}"
  export XYH_SIGNAL_PROCESS="${signal_process}"

  if [[ -n "${variable_override}" ]]; then
    export XYH_DATACARD_VARIABLE="${variable_override}"
  elif [[ "${variant}" == "logit" ]]; then
    export XYH_DATACARD_VARIABLE="logit_nn_score__${model}"
  elif [[ "${variant}" == "score" ]]; then
    export XYH_DATACARD_VARIABLE="nn_score__${model}"
  fi

  cmd=(
    law run xyh.PlotShiftedInferencePlots
    --version "${version}"
    --configs "${config}"
    --selector "${selector}"
    --producers "${producers}"
    --ml-models "${model}"
    --inference-model "${inference_model}"
    --workers "${workers}"
    --process-settings "${signal_process},unstack=True,scale=stack"
    --custom-style-config signals --skip-ratio
  )

  if [[ "${stacked}" == true ]]; then
    cmd+=(--stacked)
  fi

  if [[ -n "${custom_style_config}" ]]; then
    cmd+=(--custom-style-config "${custom_style_config}")
  fi

  echo "[+] Running: ${cmd[*]}"
  "${cmd[@]}"
done
