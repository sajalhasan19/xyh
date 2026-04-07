#!/usr/bin/env bash
set -euo pipefail

MODEL="${MODEL:-xyh_pnn_parameterized}"
VERSION="${VERSION:-ml_v1_pnn_2022post_1000_700_m1000_cat5}"
CONFIG="${CONFIG:-config_2022post}"
SELECTOR="${SELECTOR:-default}"
CALIBRATORS="${CALIBRATORS:-default}"
PRODUCERS="${PRODUCERS:-default}"
REDUCER="${REDUCER:-cf_default}"
WORKERS="${WORKERS:-30}"
ML_SETTINGS="${ML_SETTINGS:-hidden_units=512;256;128,dropout=0.3,learning_rate=3e-4,l2_reg=1e-5,batch_size=500,epochs=30,patience=5,validation_split=0.2,feature_set=legacy,reduce_lr_factor=0.5,reduce_lr_patience=3,early_stopping_min_delta=5e-5,reduce_lr_min_delta=5e-5,background_mass_mode=random}"
EVAL_X="${EVAL_X:-1000}"
EVAL_Y="${EVAL_Y:-700}"
MAX_COND_MASS_X="${MAX_COND_MASS_X:-1000}"
COND_MASSES="${COND_MASSES:-}"
SUPPRESS_MISSING_MASS_WARN="${SUPPRESS_MISSING_MASS_WARN:-1}"

usage() {
  cat <<'EOF'
Usage:
  TrainPNN.sh [eval_mass_x eval_mass_y] [--ml-model-settings 'conditioning_masses=...,...'] [<law args>]

Options (optional):
  --ml-model <name>
  --version <version>
  --config <config>
  --selector <selector>
  --calibrators <calibrators>
  --reducer <reducer>
  --producers <producers>
  --workers <n>
  --ml-model-settings <settings>

You can also set defaults via env vars:
  MODEL, VERSION, CONFIG, SELECTOR, CALIBRATORS, REDUCER, PRODUCERS, WORKERS
  ML_SETTINGS, COND_MASSES, MAX_COND_MASS_X, EVAL_X, EVAL_Y, SUPPRESS_MISSING_MASS_WARN
EOF
}

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ml-model) MODEL="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --selector) SELECTOR="$2"; shift 2 ;;
    --calibrators) CALIBRATORS="$2"; shift 2 ;;
    --reducer) REDUCER="$2"; shift 2 ;;
    --producers) PRODUCERS="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --ml-model-settings) ML_SETTINGS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) EXTRA_ARGS+=("$1"); shift ;;
  esac
done

if [[ ${#EXTRA_ARGS[@]} -ge 1 && ${EXTRA_ARGS[0]} =~ ^[0-9]+$ ]]; then
  if [[ ${#EXTRA_ARGS[@]} -ge 2 && ${EXTRA_ARGS[1]} =~ ^[0-9]+$ ]]; then
    EVAL_X="${EXTRA_ARGS[0]}"
    EVAL_Y="${EXTRA_ARGS[1]}"
    EXTRA_ARGS=("${EXTRA_ARGS[@]:2}")
  else
    echo "ERROR: When using positional masses, pass both eval_mass_x and eval_mass_y." >&2
    exit 1
  fi
fi

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

if [[ -z "$ML_SETTINGS" || "$ML_SETTINGS" != *"conditioning_masses="* ]]; then
  if [[ -z "$COND_MASSES" ]]; then
    COND_MASSES="$(build_cond_masses)"
  fi
  if [[ -z "$COND_MASSES" ]]; then
    echo "ERROR: No conditioning masses found for config '${CONFIG}' with MAX_COND_MASS_X='${MAX_COND_MASS_X}'." >&2
    exit 1
  fi

  EXTRA_SETTINGS=()
  if [[ "$ML_SETTINGS" != *"background_mass_mode="* ]]; then
    EXTRA_SETTINGS+=("background_mass_mode=random")
  fi
  if [[ "$ML_SETTINGS" != *"eval_mass_x="* ]]; then
    EXTRA_SETTINGS+=("eval_mass_x=${EVAL_X}")
  fi
  if [[ "$ML_SETTINGS" != *"eval_mass_y="* ]]; then
    EXTRA_SETTINGS+=("eval_mass_y=${EVAL_Y}")
  fi

  if [[ -z "$ML_SETTINGS" ]]; then
    ML_SETTINGS="conditioning_masses=${COND_MASSES}"
  elif [[ "$ML_SETTINGS" != *"conditioning_masses="* ]]; then
    ML_SETTINGS="${ML_SETTINGS},conditioning_masses=${COND_MASSES}"
  fi
  if [[ ${#EXTRA_SETTINGS[@]} -gt 0 ]]; then
    ML_SETTINGS="${ML_SETTINGS},$(IFS=,; echo "${EXTRA_SETTINGS[*]}")"
  fi
fi

echo "Conditioning masses: ${COND_MASSES}"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' not found. Source setup.sh first." >&2
  exit 1
fi

echo "[Train] ${MODEL} (version=${VERSION})"
if [[ "${SUPPRESS_MISSING_MASS_WARN}" == "1" ]]; then
  law run cf.MLTraining --local-scheduler \
    --config "$CONFIG" --ml-model "$MODEL" --version "$VERSION" \
    --selector "$SELECTOR" --calibrators "$CALIBRATORS" \
    --reducer "$REDUCER" --producers "$PRODUCERS" \
    --workers "$WORKERS" --ml-model-settings "$ML_SETTINGS" \
    "${EXTRA_ARGS[@]}" 2>&1 | sed '/Skipping missing signal masses in training:/d'
else
  law run cf.MLTraining --local-scheduler \
    --config "$CONFIG" --ml-model "$MODEL" --version "$VERSION" \
    --selector "$SELECTOR" --calibrators "$CALIBRATORS" \
    --reducer "$REDUCER" --producers "$PRODUCERS" \
    --workers "$WORKERS" --ml-model-settings "$ML_SETTINGS" \
    "${EXTRA_ARGS[@]}"
fi
