#!/usr/bin/env bash
set -euo pipefail

# Create histograms for one or many XYH signal hypotheses, assigning a
# mass-point specific version name that ends with "pre_ml_v1" (configurable via
# HIST_VERSION_SUFFIX).
#
# Usage:
#   CreateHistograms.sh [--max-x <value>] [signal_process ...]
# Examples:
#   CreateHistograms.sh xyh_sl_x1000_y350
#   CreateHistograms.sh --max-x 1500         # run for all signals with mX <= 1500
#   CreateHistograms.sh sig1 sig2 sig3       # run for the listed signals

max_x=2000
declare -a requested_signals=()

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source the analysis setup before running this script." >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-x)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --max-x requires a value." >&2
        exit 1
      fi
      max_x="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [--max-x <value>] [signal_process ...]" >&2
      exit 0
      ;;
    *)
      requested_signals+=("$1")
      shift
      ;;
  esac
done

if [[ ${#requested_signals[@]} -eq 0 ]]; then
  export XYH_MAX_MASS="${max_x}"
  mapfile -t requested_signals < <(
    python - <<'PY'
import os
import re
from xyh.inference.signals import XYH_SIGNAL_PROCESSES

max_x = int(os.environ.get("XYH_MAX_MASS", "2000"))
pattern = re.compile(r"xyh_sl_x(\d+)_y(\d+)")
for name in XYH_SIGNAL_PROCESSES:
    match = pattern.fullmatch(name)
    if not match:
        continue
    if int(match.group(1)) <= max_x:
        print(name)
PY
  )
fi

if [[ ${#requested_signals[@]} -eq 0 ]]; then
  echo "ERROR: No signal processes found to process." >&2
  exit 1
fi

analysis="xyh.config.analysis_xyh.analysis_xyh"
config="config_2022pre"
selector="default"
calibrators="default"
reducer="cf_default"
producers="default"
hist_producer="cf_default"
base_suffix="${HIST_VERSION_SUFFIX:-pre_ml_v1}"

for signal_process in "${requested_signals[@]}"; do
  if [[ ! $signal_process =~ ^xyh_sl_x([0-9]+)_y([0-9]+)$ ]]; then
    echo "ERROR: signal process '${signal_process}' does not match pattern 'xyh_sl_x<mass>_y<mass>'." >&2
    continue
  fi

  mass_x="${BASH_REMATCH[1]}"
  mass_y="${BASH_REMATCH[2]}"
  signal_dataset="${signal_process}_madgraph"
  version="${signal_process}_${base_suffix}"

  echo "[+] Creating histograms for signal '${signal_process}' (mX=${mass_x}, mY=${mass_y})"
  echo "    -> dataset           : ${signal_dataset}"
  echo "    -> histogram version : ${version}"

  law run cf.CreateHistograms \
    --analysis "${analysis}" \
    --config "${config}" \
    --dataset "${signal_dataset}" \
    --selector "${selector}" \
    --calibrators "${calibrators}" \
    --reducer "${reducer}" \
    --producers "${producers}" \
    --hist-producer "${hist_producer}" \
    --version "${version}" \
    --local-scheduler

  store_dir="data/cf_store/analysis_xyh/cf.CreateHistograms/${config}/${signal_dataset}/nominal/calib__${calibrators}/sel__${selector}/red__${reducer}/prod__${producers}/hist__${hist_producer}/${version}"

  if [[ -d "${store_dir}" ]]; then
    echo "[+] Histograms stored under: ${store_dir}"
  else
    echo "[i] Expected histograms location (created on first successful run): ${store_dir}"
  fi

  echo "[+] Done for ${signal_process}."
done

echo "[+] All requested histograms created."
