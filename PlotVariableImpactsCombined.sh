#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  PlotVariableImpactsCombined.sh <feature_impacts.json> [more feature_impacts.json ...]
EOF
  exit 1
}

[[ $# -ge 1 ]] || usage

impact_paths=()
for path in "$@"; do
  if [[ ! -f $path ]]; then
    echo "ERROR: feature-impact file not found: ${path}" >&2
    exit 1
  fi
  impact_paths+=("$path")
done

output_dir="${OUTPUT_DIR:-/data/dust/user/hasansye/xyh/impact_plots}"
mkdir -p "$output_dir"

joined_paths=""
for path in "${impact_paths[@]}"; do
  if [[ -n $joined_paths ]]; then
    joined_paths+=$'\n'
  fi
  joined_paths+="$path"
done

export IMPACT_PATHS="$joined_paths"
export OUTPUT_DIR="$output_dir"
export TOP_N="${TOP_N:-25}"

python <<'PY'
import json
import os
import pathlib
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np

paths = [
    pathlib.Path(line)
    for line in os.environ["IMPACT_PATHS"].splitlines()
    if line.strip()
]
output_dir = pathlib.Path(os.environ["OUTPUT_DIR"]).expanduser()
top_n = max(int(os.environ.get("TOP_N", "25")), 1)

per_feature = defaultdict(list)
sources = set()

for path in paths:
    payload = json.loads(path.read_text())
    sources.add(str(payload.get("source", "unknown")))
    for item in payload.get("impacts", []):
        per_feature[str(item["feature"])].append(float(item.get("loss_delta", 0.0)))

if not per_feature:
    raise SystemExit("No impacts found in the provided files.")

summary = []
for feature, values in per_feature.items():
    values_arr = np.array(values, dtype=float)
    summary.append({
        "feature": feature,
        "folds_present": int(len(values)),
        "loss_delta_mean": float(np.mean(values_arr)),
        "loss_delta_std": float(np.std(values_arr)),
    })

summary.sort(key=lambda item: item["loss_delta_mean"], reverse=True)
selected = summary[:top_n]
selected.reverse()

labels = [item["feature"] for item in selected]
values = [item["loss_delta_mean"] for item in selected]
errors = [item["loss_delta_std"] for item in selected]

fig_height = max(4.5, 0.35 * len(labels) + 1.8)
fig, ax = plt.subplots(figsize=(11.0, fig_height))
bars = ax.barh(
    labels,
    values,
    xerr=errors,
    color="#1d4ed8",
    alpha=0.88,
    error_kw={"elinewidth": 1.2, "ecolor": "#334155", "capsize": 3},
)
ax.set_xlabel("Mean increase in weighted BCE after permutation")
ax.set_ylabel("Feature")
ax.set_title(f"Variable impact across files ({', '.join(sorted(sources))})")
ax.grid(axis="x", alpha=0.25)

for bar, value, error in zip(bars, values, errors):
    ax.text(
        bar.get_width() + error,
        bar.get_y() + bar.get_height() / 2.0,
        f" {value:.4f} +/- {error:.4f}",
        va="center",
        ha="left",
        fontsize=9,
    )

base_name = "variable_impact_combined"
output_path = output_dir / f"{base_name}.png"
summary_path = output_dir / f"{base_name}.json"

fig.tight_layout()
fig.savefig(output_path, dpi=160, bbox_inches="tight")
plt.close(fig)

summary_payload = {
    "files": [str(path) for path in paths],
    "n_files": len(paths),
    "source": sorted(sources),
    "impacts": summary,
}
summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True))

print(f"Saved combined plot to {output_path}")
print(f"Saved combined summary to {summary_path}")
PY
