#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/ml_settings_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage: PlotNNScore.sh <xyh_binary_x<mass>_y<mass> | xyh_binary_parameterized | xyh_pnn_parameterized> [--raw|--logit] [--eval-mass-x <mx> --eval-mass-y <my>] [<law args>]

Examples:
  PlotNNScore.sh xyh_binary_x500_y350             # logit (default)
  PlotNNScore.sh xyh_binary_x500_y350 --raw       # raw score
  PlotNNScore.sh xyh_binary_x500_y350 --logit     # explicit logit
  PlotNNScore.sh xyh_binary_parameterized --raw   # needs eval masses (defaults 1000/700 or EVAL_MASS_X/Y)
  PlotNNScore.sh xyh_pnn_parameterized --raw      # needs eval masses (defaults 1000/700 or EVAL_MASS_X/Y)

Parameterized models require conditioning masses. Provide either:
  - ML_SETTINGS="conditioning_masses=...,eval_mass_x=...,eval_mass_y=..."
  - COND_MASSES="x900_y600;..." (script will add eval masses)
For xyh_pnn_parameterized, ML_SETTINGS defaults to the TrainPNN.sh settings when unset.
EOF
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage
shift

eval_mass_x="${EVAL_MASS_X:-1000}"
eval_mass_y="${EVAL_MASS_Y:-700}"
COND_MASSES="${COND_MASSES:-}"
ML_SETTINGS="${ML_SETTINGS:-}"
# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
DEFAULT_PNN_SETTINGS="hidden_units=512;512;256;128,dropout=0.3,learning_rate=5e-4,batch_size=500,epochs=30,patience=8,validation_split=0.2,feature_set=legacy,reduce_lr_factor=0.5,reduce_lr_patience=3,early_stopping_min_delta=1e-4,reduce_lr_min_delta=1e-4,background_mass_mode=random"

variant="logit"
EXTRA_LAW_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --eval-mass-x)
      eval_mass_x="${2:-}"
      [[ -z $eval_mass_x ]] && usage
      shift 2
      ;;
    --eval-mass-x=*)
      eval_mass_x="${1#*=}"
      shift
      ;;
    --eval-mass-y)
      eval_mass_y="${2:-}"
      [[ -z $eval_mass_y ]] && usage
      shift 2
      ;;
    --eval-mass-y=*)
      eval_mass_y="${1#*=}"
      shift
      ;;
    --raw)
      variant="score"
      shift
      ;;
    --logit)
      variant="logit"
      shift
      ;;
    --variant=*)
      variant="${1#*=}"
      shift
      ;;
    --variant)
      variant="${2:-}"
      [[ -z $variant ]] && usage
      shift 2
      ;;
    --ml-model-settings)
      ML_SETTINGS="${2:-}"
      [[ -z $ML_SETTINGS ]] && usage
      shift 2
      ;;
    --ml-model-settings=*)
      ML_SETTINGS="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      EXTRA_LAW_ARGS+=("$1")
      shift
      ;;
  esac
done

case "$ml_model" in
  xyh_binary_parameterized|xyh_pnn_parameterized)
    if [[ -z $eval_mass_x || -z $eval_mass_y ]]; then
      echo "ERROR: eval masses must be set for parameterized model (use --eval-mass-x/--eval-mass-y or EVAL_MASS_X/EVAL_MASS_Y)." >&2
      exit 1
    fi
    signal_suffix="x${eval_mass_x}_y${eval_mass_y}"
    is_parameterized=true
    ;;
  xyh_binary_x*_y*)
    if [[ $ml_model =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
      signal_suffix="x${BASH_REMATCH[1]}_y${BASH_REMATCH[2]}"
    else
      echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
      exit 1
    fi
    is_parameterized=false
    ;;
  *)
    echo "ERROR: unsupported ML model '${ml_model}'." >&2
    exit 1
    ;;
esac

if [[ ${is_parameterized:-false} == true && $ml_model == "xyh_pnn_parameterized" && -z $ML_SETTINGS ]]; then
  ML_SETTINGS="${DEFAULT_PNN_SETTINGS}"
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

build_cond_masses() {
  python - <<'PY'
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
}

if [[ ${is_parameterized:-false} == true ]]; then
  training_categories=""
  if [[ -z $ML_SETTINGS || $ML_SETTINGS != *"conditioning_masses="* ]]; then
    if [[ -z $COND_MASSES ]]; then
      COND_MASSES="$(build_cond_masses)"
    fi
    if [[ -z $COND_MASSES ]]; then
      echo "ERROR: No conditioning masses found. Set COND_MASSES or ML_SETTINGS with conditioning_masses=... ." >&2
      exit 1
    fi
    append_ml_setting "conditioning_masses" "$COND_MASSES"
  fi
  append_ml_setting "eval_mass_x" "$eval_mass_x"
  append_ml_setting "eval_mass_y" "$eval_mass_y"
fi

ml_settings="$(resolve_ml_settings "${ml_model}" "${training_categories}" "true" "false")"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

law run cf.PlotVariables1D \
  --version "${VERSION:-ml_v1}" \
  --configs "${CONFIG:-config_2022post}" \
  --selector "${SELECTOR:-default}" \
  --producers "${PRODUCERS:-default}" \
  --ml-models "${ml_model}" \
  --processes "${signal_process},background" \
  --datasets "${signal_dataset},background" \
  --categories "${CATEGORIES:-cat_incl,1lep__3bjets__4jets,1lep__3bjets__5jets,1lep__3bjets__ge6jets,1lep__4bjets__5jets,1lep__ge4bjets__ge6jets}" \
  --variables "${variable}" \
  --workers "${WORKERS:-56}" \
  --custom-style-config signals \
  --process-settings "xyh,unstack,color=#000000,histtype=step,linewidth=2,linestyle=--,zorder=1000,scale=stack" --skip-ratio \
  "${EXTRA_LAW_ARGS[@]}"
  #--remove-output 0,a,y
  #--categories "${CATEGORIES:-1e__3bjets__4jets,1e__3bjets__5jets,1e__3bjets__6jets,1e__4bjets__5jets,1e__4bjets__6jets,1mu__3bjets__4jets,1mu__3bjets__5jets,1mu__3bjets__6jets,1mu__4bjets__5jets,1mu__4bjets__6jets}" \
