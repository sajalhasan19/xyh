#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MODEL_ROOT="${MODEL_ROOT:-/data/dust/user/hasansye/xyh/data/cf_store/analysis_xyh/cf.MLTraining/config_2022post/calib__default/sel__default/red__cf_default/prod__default/ml__xyh_pnn_parameterized__9b7679b83e/ml_unc_pnn_v5}"
SIGNAL_DATASET="${SIGNAL_DATASET:-xyh_sl_x1000_y700_madgraph}"
EVAL_X="${EVAL_X:-1000}"
EVAL_Y="${EVAL_Y:-700}"
FEATURE_SET="${FEATURE_SET:-legacy}"
TOTAL_FOLDS="${TOTAL_FOLDS:-4}"
FOLDS="${FOLDS:-0 1 2 3}"
MAX_EVENTS="${MAX_EVENTS:-1000}"
BACKGROUND_SIZE="${BACKGROUND_SIZE:-200}"
KERNEL_NSAMPLES="${KERNEL_NSAMPLES:-auto}"
SEED="${SEED:-}"
PLOT="${PLOT:-1}"
PYTHON_BIN="${PYTHON_BIN:-python}"

usage() {
  cat <<'EOF'
Usage:
  RunPNNShapPlots.sh

Environment overrides:
  MODEL_ROOT        Parent directory containing mlmodel_f0of4 ... mlmodel_f3of4
  SIGNAL_DATASET    Signal dataset name under cf.MergeMLEvents
  EVAL_X, EVAL_Y    Conditioning mass used for the SHAP evaluation
  FEATURE_SET       Training feature set (legacy, curated, all)
  TOTAL_FOLDS       Number of folds, default 4
  FOLDS             Space-separated fold indices, default "0 1 2 3"
  MAX_EVENTS        Max combined events to explain
  BACKGROUND_SIZE   Background sample size for KernelExplainer
  KERNEL_NSAMPLES   Value passed to shap.KernelExplainer.shap_values(nsamples=...)
  SEED              Optional seed passed to compute_feature_impacts.py
  PLOT              If 1, also create SHAP summary plots
  PYTHON_BIN        Python executable to use
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -d $MODEL_ROOT ]]; then
  echo "ERROR: MODEL_ROOT does not exist: ${MODEL_ROOT}" >&2
  exit 1
fi

if [[ $MODEL_ROOT != *"/cf.MLTraining/"* ]]; then
  echo "ERROR: MODEL_ROOT must point inside a cf.MLTraining store path." >&2
  exit 1
fi

store_prefix="${MODEL_ROOT%%/cf.MLTraining/*}"
relative_tail="${MODEL_ROOT#${store_prefix}/cf.MLTraining/}"
config_name="${relative_tail%%/*}"
rest_path="${relative_tail#*/}"
merge_config_root="${store_prefix}/cf.MergeMLEvents/${config_name}"

if [[ ! -d $merge_config_root ]]; then
  echo "ERROR: Could not find matching MergeMLEvents root: ${merge_config_root}" >&2
  exit 1
fi

for fold in $FOLDS; do
  model_path="${MODEL_ROOT}/mlmodel_f${fold}of${TOTAL_FOLDS}"
  signal_file="${merge_config_root}/${SIGNAL_DATASET}/${rest_path}/mlevents_f${fold}of${TOTAL_FOLDS}.parquet"

  if [[ ! -d $model_path ]]; then
    echo "ERROR: Missing fold model path: ${model_path}" >&2
    exit 1
  fi
  if [[ ! -f $signal_file ]]; then
    echo "ERROR: Missing signal mlevents file for fold ${fold}: ${signal_file}" >&2
    exit 1
  fi

  background_files=()
  while IFS= read -r path; do
    [[ $path == "$signal_file" ]] && continue
    background_files+=("$path")
  done < <(find "$merge_config_root" -path "*/${rest_path}/mlevents_f${fold}of${TOTAL_FOLDS}.parquet" -type f | sort)

  if [[ ${#background_files[@]} -eq 0 ]]; then
    echo "ERROR: No background files found for fold ${fold} under ${merge_config_root}" >&2
    exit 1
  fi

  cmd=(
    "$PYTHON_BIN" "${script_dir}/compute_feature_impacts.py"
    --model-path "$model_path"
    --model-type pnn
    --feature-set "$FEATURE_SET"
    --eval-mass-x "$EVAL_X"
    --eval-mass-y "$EVAL_Y"
    --max-events "$MAX_EVENTS"
    --background-size "$BACKGROUND_SIZE"
    --kernel-nsamples "$KERNEL_NSAMPLES"
    --signal-events "$signal_file"
    --background-events "${background_files[@]}"
  )
  if [[ -n $SEED ]]; then
    cmd+=(--seed "$SEED")
  fi

  echo "[fold ${fold}] model=${model_path}"
  echo "[fold ${fold}] signal=${signal_file}"
  echo "[fold ${fold}] backgrounds=${#background_files[@]}"
  "${cmd[@]}"

  json_path="${MODEL_ROOT}/mlmodel_f${fold}of${TOTAL_FOLDS}_shap_values.json"
  if [[ $PLOT == "1" ]]; then
    bash "${script_dir}/PlotShapSummary.sh" "$json_path"
  fi
done

echo "Completed SHAP runs for folds: ${FOLDS}"
