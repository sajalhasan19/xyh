#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: PlotVariableImpacts.sh <path/to/feature_impacts.json>" >&2
  exit 1
}

impact_path=${1:-}
[[ -n $impact_path ]] || usage
if [[ ! -f $impact_path ]]; then
  echo "ERROR: feature-impact file not found: ${impact_path}" >&2
  exit 1
fi

output_dir="${OUTPUT_DIR:-/data/dust/user/hasansye/xyh/impact_plots}"
mkdir -p "$output_dir"

export IMPACT_PATH="$impact_path"
export OUTPUT_DIR="$output_dir"
export TOP_N="${TOP_N:-25}"

python <<'PY'
import json
import os
import pathlib

import matplotlib.pyplot as plt

impact_path = pathlib.Path(os.environ["IMPACT_PATH"])
output_dir = pathlib.Path(os.environ["OUTPUT_DIR"]).expanduser()
top_n = max(int(os.environ.get("TOP_N", "25")), 1)

payload = json.loads(impact_path.read_text())
impacts = list(payload.get("impacts", []))
if not impacts:
    raise SystemExit(f"No impacts found in {impact_path}")

impacts.sort(key=lambda item: float(item.get("loss_delta", 0.0)), reverse=True)
selected = impacts[:top_n]
selected.reverse()

labels = [str(item["feature"]) for item in selected]
values = [float(item.get("loss_delta", 0.0)) for item in selected]

fig_height = max(4.5, 0.35 * len(labels) + 1.8)
fig, ax = plt.subplots(figsize=(10.5, fig_height))
bars = ax.barh(labels, values, color="#0f766e", alpha=0.9)
ax.set_xlabel("Increase in weighted BCE after permutation")
ax.set_ylabel("Feature")
ax.set_title(f"Variable impact ({payload.get('source', 'post_training')})")
ax.grid(axis="x", alpha=0.25)

for bar, value in zip(bars, values):
    ax.text(
        bar.get_width(),
        bar.get_y() + bar.get_height() / 2.0,
        f" {value:.4f}",
        va="center",
        ha="left",
        fontsize=9,
    )

output_path = output_dir / (impact_path.stem + ".png")
fig.tight_layout()
fig.savefig(output_path, dpi=160, bbox_inches="tight")
plt.close(fig)
print(f"Saved plot to {output_path}")
PY
