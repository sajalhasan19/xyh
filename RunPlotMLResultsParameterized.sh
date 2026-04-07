#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [eval_mass_x] [eval_mass_y]" >&2
  echo "Defaults: 1000 700" >&2
  exit 1
}

EVAL_X="${1:-1000}"
EVAL_Y="${2:-700}"
[[ -n $EVAL_X && -n $EVAL_Y ]] || usage

COND_MASSES="${COND_MASSES:-}"
if [[ -z $COND_MASSES ]]; then
  COND_MASSES=$(python - <<'PY'
import os, re, importlib
from pathlib import Path
max_x = os.environ.get("MAX_COND_MASS_X")
max_x = int(max_x) if max_x else None
config_name = os.environ.get("CONFIG", "config_2022pre")
cfg_mod = importlib.import_module("xyh.config.analysis_xyh")
if not hasattr(cfg_mod, config_name):
    raise SystemExit(f"Config '{config_name}' not found in xyh.config.analysis_xyh")
config_inst = getattr(cfg_mod, config_name)

def within_limit(mx: int) -> bool:
    return max_x is None or mx <= max_x

masses = set()
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

# 3) last resort: use all known signal processes (filtered) if still empty
if not masses:
    from xyh.inference.signals import XYH_SIGNAL_PROCESSES
    for name in XYH_SIGNAL_PROCESSES:
        m = re.search(r"x(\d+)_y(\d+)", name)
        if m and within_limit(int(m.group(1))):
            masses.add(f"x{m.group(1)}_y{m.group(2)}")

print(";".join(sorted(masses)))
PY
)
  if [[ -z $COND_MASSES ]]; then
    echo "ERROR: No conditioning masses found in config '${CONFIG:-config_2022pre}' with MAX_COND_MASS_X='${MAX_COND_MASS_X:-}'." >&2
    exit 1
  fi
fi
MODEL="${MODEL:-xyh_binary_parameterized}"
VERSION="${VERSION:-ml_unc_pnn_v15}"
CONFIG="${CONFIG:-config_2022pre}"
SELECTOR="${SELECTOR:-default}"
PRODUCERS="${PRODUCERS:-default}"
WORKERS="${WORKERS:-30}"
PLOT_FUNCTION="${PLOT_FUNCTION:-roc}"  # options: roc, cm

ML_SETTINGS="${ML_SETTINGS:-conditioning_masses=${COND_MASSES},background_mass_mode=duplicate,eval_mass_x=${EVAL_X},eval_mass_y=${EVAL_Y}}"
SIGNAL_DATASET="xyh_sl_x${EVAL_X}_y${EVAL_Y}_madgraph"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' not found. Please source setup.sh first." >&2
  exit 1
fi

echo "[Plot] Plotting ML results (${PLOT_FUNCTION}) for '${MODEL}' at m=(${EVAL_X},${EVAL_Y})"
law run cf.PlotMLResults --local-scheduler \
  --version "$VERSION" --config "$CONFIG" --selector "$SELECTOR" --producers "$PRODUCERS" \
  --ml-model "$MODEL" --ml-model-settings "$ML_SETTINGS" \
  --processes "xyh_sl_x${EVAL_X}_y${EVAL_Y},background" \
  --datasets "${SIGNAL_DATASET},background" \
  --categories "${CATEGORIES:-cat_incl,1lep__3bjets__4jets,1lep__3bjets__5jets,1lep__3bjets__ge6jets,1lep__4bjets__5jets,1lep__ge4bjets__ge6jets}" \
  --plot-function "$PLOT_FUNCTION" \
  --workers "$WORKERS"

echo "Done."
