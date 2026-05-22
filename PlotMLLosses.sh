#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: PlotMLLosses.sh <path/to/training_history.json|model_history.pkl>" >&2
  exit 1
}

history_path=${1:-}
[[ -n $history_path ]] || usage
if [[ ! -f $history_path ]]; then
  echo "ERROR: training history file not found: ${history_path}" >&2
  exit 1
fi

mass_x=""
mass_y=""
if [[ $history_path =~ x([0-9]+)_y([0-9]+) ]]; then
  mass_x=${BASH_REMATCH[1]}
  mass_y=${BASH_REMATCH[2]}
fi

fold=""
if [[ $history_path =~ mlmodel_f([0-9]+)of([0-9]+) ]]; then
  fold=${BASH_REMATCH[1]}
fi

version="unknown"
if [[ $history_path =~ /([^/]+)/mlmodel_f[0-9]+of[0-9]+/ ]]; then
  version=${BASH_REMATCH[1]}
elif [[ $history_path =~ /([^/]+)/[^/]*training_history[^/]*$ ]]; then
  version=${BASH_REMATCH[1]}
elif [[ $history_path =~ /([^/]+)/[^/]*model_history[^/]*$ ]]; then
  version=${BASH_REMATCH[1]}
fi

output_dir="/data/dust/user/hasansye/xyh/loss_plots/${version}"
mkdir -p "$output_dir"

export HIST_PATH="$history_path"
export OUTPUT_DIR="$output_dir"
export VERSION="$version"
export MASS_X="$mass_x"
export MASS_Y="$mass_y"
export FOLD="$fold"

python <<'PY'
import json
import os
import pathlib
import pickle
import matplotlib.pyplot as plt

history_path = pathlib.Path(os.environ["HIST_PATH"])
output_dir = pathlib.Path(os.environ["OUTPUT_DIR"]).expanduser()
version = os.environ.get("VERSION", "unknown")
mass_x = os.environ.get("MASS_X", "")
mass_y = os.environ.get("MASS_Y", "")
fold = os.environ.get("FOLD", "")

parts = ["ml_loss", version]
if mass_x and mass_y:
    parts.append(f"x{mass_x}_y{mass_y}")
if fold:
    parts.append(f"fold{fold}")
filename = "_".join(parts) + ".png"
output_path = output_dir / filename

if history_path.suffix == ".pkl":
    with history_path.open("rb") as f:
        history = pickle.load(f)
else:
    history = json.loads(history_path.read_text())

if "metrics" in history:
    loss = history["metrics"]["loss"]
    val_loss = history["metrics"]["val_loss"]
    epochs = history.get("epoch", list(range(1, len(loss) + 1)))
else:
    loss = history["loss"]
    val_loss = history["val_loss"]
    epochs = list(range(1, len(loss) + 1))

plt.plot(epochs, loss, label="train")
plt.plot(epochs, val_loss, label="val")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.legend()
title_parts = [version]
if mass_x and mass_y:
    title_parts.append(f"mX={mass_x}, mY={mass_y}")
if fold:
    title_parts.append(f"fold {fold}")
plt.title(" - ".join(title_parts))
plt.savefig(output_path, dpi=150, bbox_inches="tight")
print(f"Saved plot to {output_path}")
PY
