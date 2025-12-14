#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: PlotMLLosses.sh <path/to/mlmodel_fXofY_training_history.json>" >&2
  exit 1
}

history_path=${1:-}
[[ -n $history_path ]] || usage
if [[ ! -f $history_path ]]; then
  echo "ERROR: training history file not found: ${history_path}" >&2
  exit 1
fi

if [[ $history_path =~ x([0-9]+)_y([0-9]+) ]]; then
  mass_x=${BASH_REMATCH[1]}
  mass_y=${BASH_REMATCH[2]}
else
  echo "ERROR: failed to extract mass point from path: ${history_path}" >&2
  exit 1
fi

if [[ $history_path =~ mlmodel_f([0-9]+)of([0-9]+) ]]; then
  fold=${BASH_REMATCH[1]}
else
  echo "ERROR: failed to extract fold from filename: ${history_path}" >&2
  exit 1
fi

if [[ $history_path =~ /(ml_v[0-9a-zA-Z_]+)/ ]]; then
  version=${BASH_REMATCH[1]}
else
  echo "ERROR: failed to extract version from path: ${history_path}" >&2
  exit 1
fi

output_dir="/data/dust/user/hasansye/xyh/loss_plots"
mkdir -p "$output_dir"
output_path="${output_dir}/ml_loss_${version}_x${mass_x}_y${mass_y}_fold${fold}.png"

export HIST_PATH="$history_path"
export OUTPUT_PATH="$output_path"

python <<'PY'
import json
import os
import pathlib
import matplotlib.pyplot as plt

history_path = pathlib.Path(os.environ["HIST_PATH"])
output_path = pathlib.Path(os.environ["OUTPUT_PATH"]).expanduser()
history = json.loads(history_path.read_text())
loss = history["metrics"]["loss"]
val_loss = history["metrics"]["val_loss"]

plt.plot(history["epoch"], loss, label="train")
plt.plot(history["epoch"], val_loss, label="val")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.legend()
plt.savefig(output_path, dpi=150, bbox_inches="tight")
print(f"Saved plot to {output_path}")
PY
