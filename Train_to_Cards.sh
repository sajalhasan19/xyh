#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: Train_to_Cards.sh [--version <tag>] [--model <name>] [--eval-mass-x <x>] [--eval-mass-y <y>] [--eval-masses <x:y,x:y,...>] [<law args>]

Options:
  -v, --version       Override the VERSION environment variable passed to downstream tasks
  -m, --model         Process only the specified ML model
                      (e.g. xyh_binary_x700_y500 or xyh_pnn_parameterized)
      --eval-mass-x   Evaluation mass X for parameterized model chain (default: 1000)
      --eval-mass-y   Evaluation mass Y for parameterized model chain (default: 700)
      --eval-masses   Comma-separated eval mass points for parameterized model chain
                      format: x:y,x:y,...  (example: 650:350,750:500,1000:700)
      --ml-model-settings <settings>
                      ML settings used to identify the trained/evaluated model
  -h, --help          Show this help and exit

By default, binary model mass points are discovered from the XYH 2022 postEE
campaign dataset file. Use `--model` to restrict processing to one model.

Environment knobs:
  DATACARD_VARIABLE   Defaults to logit_nn_score__<model> (transformed score)

Any additional law arguments are forwarded to the downstream tasks
(e.g. --workflow htcondor --job-workers 48 --htcondor-memory 128GB).
EOF
  exit 1
}

version="${VERSION:-ttbb_v2}" #ml_unc_bin_x650_y350_v2 #ml_unc_pnn_v1
config="${CONFIG:-config_2022post}"
configs="${CONFIGS:-${config}}"
ml_settings="${ML_SETTINGS:-hidden_units=512;256;128,dropout=0.2,learning_rate=3e-4,l2_reg=1e-5,batch_size=500,epochs=30,patience=0,validation_split=0.2,feature_set=legacy,reduce_lr_factor=0.5,reduce_lr_patience=3,early_stopping_min_delta=5e-5,reduce_lr_min_delta=5e-5,background_mass_mode=random}"
# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
single_model=""
eval_mass_x="${EVAL_X:-1000}"
eval_mass_y="${EVAL_Y:-700}"
eval_masses=""
parameterized_eval_points=()
extra_law_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      version="$2"
      shift 2
      ;;
    -m|--model)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      single_model="$2"
      shift 2
      ;;
    --eval-mass-x)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      eval_mass_x="$2"
      shift 2
      ;;
    --eval-mass-y)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      eval_mass_y="$2"
      shift 2
      ;;
    --eval-masses)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      eval_masses="$2"
      shift 2
      ;;
    --ml-model-settings)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      ml_settings="$2"
      shift 2
      ;;
    --ml-model-settings=*)
      ml_settings="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      # Allow model as first positional arg for backward compatibility.
      if [[ -z "${single_model}" && "$1" =~ ^xyh_(binary_x[0-9]+_y[0-9]+|.*_parameterized)$ ]]; then
        single_model="$1"
      else
        extra_law_args+=("$1")
      fi
      shift
      ;;
  esac
done

if [[ -n "${eval_masses}" ]]; then
  eval_masses_normalized="${eval_masses//,/ }"
  for pair in ${eval_masses_normalized}; do
    if [[ "${pair}" =~ ^([0-9]+):([0-9]+)$ ]]; then
      parameterized_eval_points+=("${BASH_REMATCH[1]} ${BASH_REMATCH[2]}")
    else
      echo "ERROR: invalid --eval-masses entry '${pair}'." >&2
      echo "  Expected format: x:y,x:y,... (example: 650:350,750:500)" >&2
      exit 1
    fi
  done
  if [[ ${#parameterized_eval_points[@]} -eq 0 ]]; then
    echo "ERROR: --eval-masses was provided but no valid mass points were parsed." >&2
    exit 1
  fi
else
  parameterized_eval_points=("${eval_mass_x} ${eval_mass_y}")
fi

if [[ -n "${version}" ]]; then
  export VERSION="${version}"
  echo "[+] Using VERSION='${VERSION}'"
fi

export CONFIG="${config}"
export CONFIGS="${configs}"
echo "[+] Using CONFIG='${CONFIG}'"
echo "[+] Using CONFIGS='${CONFIGS}'"

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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
xyh_campaign_file="${script_dir}/modules/cmsdb/cmsdb/campaigns/run3_2022_postEE_nano_v12/xyh.py"

if [[ ! -f "${xyh_campaign_file}" ]]; then
  echo "ERROR: XYH campaign file not found at '${xyh_campaign_file}'." >&2
  exit 1
fi

mapfile -t default_models < <(
  python - "${xyh_campaign_file}" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
points = sorted({tuple(map(int, match)) for match in re.findall(r'name="xyh_sl_x(\d+)_y(\d+)_madgraph"', text)})
for mx, my in points:
    print(f"xyh_binary_x{mx}_y{my}")
PY
)

if [[ ${#default_models[@]} -eq 0 ]]; then
  echo "ERROR: no XYH binary mass points were discovered in '${xyh_campaign_file}'." >&2
  exit 1
fi

echo "[+] Discovered ${#default_models[@]} XYH binary mass points from campaign file"

if [[ -n "${single_model}" ]]; then
  models=("${single_model}")
else
  models=("${default_models[@]}")
fi

for model in "${models[@]}"; do
  model_for_variable="${model%%,*}"
  datacard_variable="${DATACARD_VARIABLE:-logit_nn_score__${model_for_variable}}"
  if [[ "${model}" =~ ^xyh_binary_(x[0-9]+_y[0-9]+)$ ]]; then
    export XYH_SIGNAL_PROCESS="xyh_sl_${BASH_REMATCH[1]}"
    export XYH_DATACARD_VARIABLE="${datacard_variable}"
    echo "[+] Using XYH_SIGNAL_PROCESS='${XYH_SIGNAL_PROCESS}'"
    echo "[+] Using XYH_DATACARD_VARIABLE='${XYH_DATACARD_VARIABLE}'"
    echo "[+] Processing ${model}"
    # ./TrainAllModels.sh "${model}" &&
    # ./RunMLEvaluations.sh "${model}" &&
    # ./RunMergeMLEvaluations.sh "${model}" &&
    ./PlotMLResults.sh "${model}" #&&
    # ./PlotNNScore.sh "${model}" --raw #&&
    # ./PlotNNScore.sh "${model}" --logit &&
    # ./RunCreateDatacards.sh "${model}" &&
    # ./Rebin.sh "${model}" &&
    # ./PlotRebinnedNNScore.sh --model "${model}"
  elif [[ "${model}" =~ ^xyh_.*_parameterized$ ]]; then
    for eval_point in "${parameterized_eval_points[@]}"; do
      read -r eval_x eval_y <<< "${eval_point}"
      echo "[+] Processing parameterized model ${model} at eval mass (${eval_x}, ${eval_y})"
      # MODEL="${model}" ./TrainPNN.sh "${eval_x}" "${eval_y}" "${extra_law_args[@]}" &&
      MODEL="${model}" ./RunMLEvalParameterized.sh "${eval_x}" "${eval_y}" "${extra_law_args[@]}" &&
      MODEL="${model}" ./PlotNNScore.sh "${model}" --raw --eval-mass-x "${eval_x}" --eval-mass-y "${eval_y}" &&
      MODEL="${model}" ./PlotNNScore.sh "${model}" --logit --eval-mass-x "${eval_x}" --eval-mass-y "${eval_y}" &&
      MODEL="${model}" DATACARD_VARIABLE="${datacard_variable}" ./RunCreateDatacardsParameterized.sh "${eval_x}" "${eval_y}" "${extra_law_args[@]}" &&
      MODEL="${model}" DATACARD_VARIABLE="${datacard_variable}" ./RunRebinParameterized.sh "${eval_x}" "${eval_y}" "${extra_law_args[@]}" &&
      MODEL="${model}" DATACARD_VARIABLE="${datacard_variable}" ./PlotRebinnedNNScoreParameterized.sh "${eval_x}" "${eval_y}" "${extra_law_args[@]}"
    done
  else
    echo "ERROR: Unsupported ML model '${model}'." >&2
    echo "  Expected either 'xyh_binary_x<mass>_y<mass>' or 'xyh_*_parameterized'." >&2
    exit 1
  fi


done
