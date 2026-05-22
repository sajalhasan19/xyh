#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [eval_mass_x] [eval_mass_y] [<law args>]" >&2
  echo "Defaults: 1000 700" >&2
  exit 1
}

EVAL_X="1000"
EVAL_Y="700"
if [[ $# -ge 2 && $1 =~ ^[0-9]+$ && $2 =~ ^[0-9]+$ ]]; then
  EVAL_X="$1"
  EVAL_Y="$2"
  shift 2
fi
[[ -n $EVAL_X && -n $EVAL_Y ]] || usage
EXTRA_LAW_ARGS=("$@")

USE_LOCAL_SCHEDULER=true
for arg in "${EXTRA_LAW_ARGS[@]}"; do
  case "$arg" in
    --workflow|--workflow=*)
      USE_LOCAL_SCHEDULER=false
      ;;
  esac
done

SCHEDULER_ARGS=()
if [[ "$USE_LOCAL_SCHEDULER" == true ]]; then
  SCHEDULER_ARGS+=(--local-scheduler)
fi

DEFAULT_PNN_SETTINGS="hidden_units=512;512;256;128,dropout=0.3,learning_rate=5e-4,batch_size=500,epochs=30,patience=8,validation_split=0.2,feature_set=legacy,reduce_lr_factor=0.5,reduce_lr_patience=3,early_stopping_min_delta=1e-4,reduce_lr_min_delta=1e-4,background_mass_mode=random"

COND_MASSES="${COND_MASSES:-}"
if [[ -z $COND_MASSES ]]; then
  COND_MASSES=$(python - <<'PY'
import os, re, importlib
from pathlib import Path
from xyh.inference.signals import XYH_SIGNAL_PROCESSES

max_x = os.environ.get("MAX_COND_MASS_X")
max_x = int(max_x) if max_x else None
config_name = os.environ.get("CONFIG", "config_2022post").split(",")[0]
cfg_mod = importlib.import_module("xyh.config.analysis_xyh")
if not hasattr(cfg_mod, config_name):
    raise SystemExit(f"Config '{config_name}' not found in xyh.config.analysis_xyh")
config_inst = getattr(cfg_mod, config_name)

def within_limit(mx: int) -> bool:
    return max_x is None or mx <= max_x

def normalize_pair(raw: str):
    numbers = re.findall(r"-?\d+(?:\.\d+)?", raw)
    if len(numbers) >= 2:
        return (int(float(numbers[0])), int(float(numbers[1])))
    return None

def datasets_for_process(config_inst, process_name: str):
    datasets = []
    try:
        process_inst = config_inst.get_process(process_name)
    except Exception:
        process_inst = None
    for dataset in getattr(config_inst, "datasets", []):
        ds_process = getattr(dataset, "process", None)
        if ds_process is None:
            if dataset.name == process_name or dataset.name.startswith(process_name + "_"):
                datasets.append(dataset)
            continue
        if process_inst and ds_process is process_inst:
            datasets.append(dataset)
            continue
        if getattr(ds_process, "name", None) == process_name:
            datasets.append(dataset)
            continue
        has_parent = getattr(ds_process, "has_parent_process", None)
        if callable(has_parent) and has_parent(process_name):
            datasets.append(dataset)
            continue
        if dataset.name == process_name or dataset.name.startswith(process_name + "_"):
            datasets.append(dataset)
    return datasets

masses = set()
for process_name in XYH_SIGNAL_PROCESSES:
    pair = normalize_pair(process_name)
    if pair is None or not within_limit(pair[0]):
        continue
    if datasets_for_process(config_inst, process_name):
        masses.add(f"x{pair[0]}_y{pair[1]}")

if not masses:
    for ds in getattr(config_inst, "datasets", []):
        m = re.search(r"xyh_sl_x(\d+)_y(\d+)", getattr(ds, "name", ""))
        if m and within_limit(int(m.group(1))):
            masses.add(f"x{m.group(1)}_y{m.group(2)}")

if not masses:
    campaigns_dir = Path("modules/cmsdb/cmsdb/campaigns")
    for xyh_file in campaigns_dir.rglob("xyh.py"):
        text = xyh_file.read_text()
        for m in re.finditer(r"xyh_sl_x(\d+)_y(\d+)_madgraph", text):
            if within_limit(int(m.group(1))):
                masses.add(f"x{m.group(1)}_y{m.group(2)}")

print(";".join(sorted(masses)))
PY
)
  if [[ -z $COND_MASSES ]]; then
    echo "ERROR: No conditioning masses found in config '${CONFIG:-config_2022post}' with MAX_COND_MASS_X='${MAX_COND_MASS_X:-}'." >&2
    exit 1
  fi
fi
MODEL="${MODEL:-xyh_pnn_parameterized}"
VERSION="${VERSION:-ml_v1_pnn_2022post_1000_700_m1000_cat5}"
CONFIG="${CONFIG:-config_2022post}"
SELECTOR="${SELECTOR:-default}"
PRODUCERS="${PRODUCERS:-default}"
WORKERS="${WORKERS:-56}"
PLOT_FUNCTION="${PLOT_FUNCTION:-roc}"

ML_SETTINGS="${ML_SETTINGS:-$DEFAULT_PNN_SETTINGS}"
SIGNAL_DATASET="xyh_sl_x${EVAL_X}_y${EVAL_Y}_madgraph"

append_ml_setting() {
  local key="$1"
  local value="$2"
  if [[ $ML_SETTINGS == *"${key}="* ]]; then
    return
  fi
  if [[ -z $ML_SETTINGS ]]; then
    ML_SETTINGS="${key}=${value}"
  else
    ML_SETTINGS="${ML_SETTINGS},${key}=${value}"
  fi
}

if [[ -z $ML_SETTINGS || $ML_SETTINGS != *"conditioning_masses="* ]]; then
  append_ml_setting "conditioning_masses" "$COND_MASSES"
fi
append_ml_setting "background_mass_mode" "random"
append_ml_setting "eval_mass_x" "$EVAL_X"
append_ml_setting "eval_mass_y" "$EVAL_Y"

echo "Using ML_SETTINGS: $ML_SETTINGS"
echo "Using COND_MASSES: $COND_MASSES"

DATASETS=(
  "$SIGNAL_DATASET"
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
  dy_m10to50_amcatnlo
  dy_m50toinf_0j_amcatnlo
  dy_m50toinf_1j_amcatnlo
  dy_m50toinf_2j_amcatnlo
  w_lnu_amcatnlo
  ww_pythia
  wz_pythia
  zz_pythia
)

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' not found. Please source setup.sh first." >&2
  exit 1
fi

# echo "[1/3] Evaluating ML model '${MODEL}' at m=(${EVAL_X},${EVAL_Y})"
# for ds in "${DATASETS[@]}"; do
#   echo "  - $ds"
#   law run cf.MLEvaluation "${SCHEDULER_ARGS[@]}" \
#     --version "$VERSION" --config "$CONFIG" --selector "$SELECTOR" --producers "$PRODUCERS" \
#     --ml-model "$MODEL" --ml-model-settings "$ML_SETTINGS" \
#     --dataset "$ds" --workers "$WORKERS" \
#     "${EXTRA_LAW_ARGS[@]}"
# done

# echo "[2/3] Merging evaluation outputs"
# for ds in "${DATASETS[@]}"; do
#   echo "  - $ds"
#   law run cf.MergeMLEvaluation "${SCHEDULER_ARGS[@]}" \
#     --version "$VERSION" --config "$CONFIG" --selector "$SELECTOR" --producers "$PRODUCERS" \
#     --ml-model "$MODEL" --ml-model-settings "$ML_SETTINGS" \
#     --dataset "$ds" --workers "$WORKERS" \
#     "${EXTRA_LAW_ARGS[@]}"
# done

echo "[3/3] Plotting ML results (${PLOT_FUNCTION})"
law run cf.PlotMLResults "${SCHEDULER_ARGS[@]}" \
  --version "$VERSION" --config "$CONFIG" --selector "$SELECTOR" --producers "$PRODUCERS" \
  --ml-model "$MODEL" --ml-model-settings "$ML_SETTINGS" \
  --processes "xyh_sl_x${EVAL_X}_y${EVAL_Y},background" \
  --datasets "${SIGNAL_DATASET},background" \
  --categories "${CATEGORIES:-cat_incl,1lep__3bjets__4jets,1lep__3bjets__5jets,1lep__3bjets__ge6jets,1lep__4bjets__5jets,1lep__ge4bjets__ge6jets}" \
  --plot-function "$PLOT_FUNCTION" \
  --workers "$WORKERS" \
  "${EXTRA_LAW_ARGS[@]}"

echo "Done."
