#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: PlotShapSummary.sh <path/to/shap_values.json>" >&2
  exit 1
}

shap_path=${1:-}
[[ -n $shap_path ]] || usage
if [[ ! -f $shap_path ]]; then
  echo "ERROR: SHAP file not found: ${shap_path}" >&2
  exit 1
fi

version="unknown"
if [[ $shap_path =~ /([^/]+)/mlmodel_f[0-9]+of[0-9]+_shap_values\.json$ ]]; then
  version=${BASH_REMATCH[1]}
fi

base_output_dir="${OUTPUT_DIR:-/data/dust/user/hasansye/xyh/shap_plots}"
if [[ "$(basename "$base_output_dir")" == "$version" ]]; then
  output_dir="$base_output_dir"
else
  output_dir="${base_output_dir}/${version}"
fi
mkdir -p "$output_dir"

export SHAP_PATH="$shap_path"
export OUTPUT_DIR="$output_dir"
export TOP_N="${TOP_N:-20}"
export VERSION="$version"

python <<'PY'
import json
import os
import pathlib

import matplotlib.pyplot as plt
import numpy as np

shap_path = pathlib.Path(os.environ["SHAP_PATH"])
output_dir = pathlib.Path(os.environ["OUTPUT_DIR"]).expanduser()
top_n = max(int(os.environ.get("TOP_N", "20")), 1)
version = os.environ.get("VERSION", "unknown")

payload = json.loads(shap_path.read_text())
if payload.get("method") != "shap":
    raise SystemExit(f"Input file is not a SHAP payload: {shap_path}")

feature_names = list(payload.get("feature_names", []))
feature_values = np.asarray(payload.get("feature_values", []), dtype=float)
shap_values = np.asarray(payload.get("shap_values", []), dtype=float)
impacts = list(payload.get("impacts", []))

if not feature_names or feature_values.size == 0 or shap_values.size == 0:
    raise SystemExit(f"Missing feature_values or shap_values in {shap_path}")
if feature_values.shape != shap_values.shape:
    raise SystemExit(
        f"feature_values shape {feature_values.shape} does not match shap_values shape {shap_values.shape}"
    )

if impacts:
    ranking = [str(item["feature"]) for item in impacts]
else:
    mean_abs = np.mean(np.abs(shap_values), axis=0)
    ranking = [feature_names[idx] for idx in np.argsort(mean_abs)[::-1]]

selected = ranking[:top_n]
selected_indices = [feature_names.index(name) for name in selected]

bar_labels = list(reversed(selected))
bar_values = [
    float(np.mean(np.abs(shap_values[:, feature_names.index(name)])))
    for name in bar_labels
]

fig_height = max(4.5, 0.35 * len(bar_labels) + 1.6)
fig, ax = plt.subplots(figsize=(10.5, fig_height))
bars = ax.barh(bar_labels, bar_values, color="#b45309", alpha=0.9)
ax.set_xlabel("mean(|SHAP value|)")
ax.set_ylabel("Feature")
ax.set_title(f"SHAP feature importance - {version}")
ax.grid(axis="x", alpha=0.25)

for bar, value in zip(bars, bar_values):
    ax.text(
        bar.get_width(),
        bar.get_y() + bar.get_height() / 2.0,
        f" {value:.4f}",
        va="center",
        ha="left",
        fontsize=9,
    )

bar_path = output_dir / f"{shap_path.stem}_bar.png"
bar_pdf_path = output_dir / f"{shap_path.stem}_bar.pdf"
fig.tight_layout()
fig.savefig(bar_path, dpi=160, bbox_inches="tight")
fig.savefig(bar_pdf_path, bbox_inches="tight")
plt.close(fig)

fig_height = max(5.5, 0.42 * len(selected) + 1.8)
fig, ax = plt.subplots(figsize=(11.5, fig_height))
rng = np.random.default_rng(12345)
for row_idx, feature_idx in enumerate(reversed(selected_indices)):
    values = shap_values[:, feature_idx]
    colors = feature_values[:, feature_idx]
    y_base = np.full(values.shape, row_idx, dtype=float)
    jitter = rng.uniform(-0.28, 0.28, size=values.shape)
    scatter = ax.scatter(
        values,
        y_base + jitter,
        c=colors,
        cmap="coolwarm",
        s=14,
        alpha=0.65,
        edgecolors="none",
        rasterized=True,
    )

ax.axvline(0.0, color="#475569", linewidth=1.0, alpha=0.7)
ax.set_yticks(np.arange(len(selected)))
ax.set_yticklabels(list(reversed(selected)))
ax.set_xlabel("SHAP value")
ax.set_ylabel("Feature")
ax.set_title(f"SHAP summary - {version}")
ax.grid(axis="x", alpha=0.2)
cbar = fig.colorbar(scatter, ax=ax, pad=0.02)
cbar.set_label("Feature value")

summary_path = output_dir / f"{shap_path.stem}_summary.png"
summary_pdf_path = output_dir / f"{shap_path.stem}_summary.pdf"
fig.tight_layout()
fig.savefig(summary_path, dpi=180, bbox_inches="tight")
fig.savefig(summary_pdf_path, bbox_inches="tight")
plt.close(fig)

print(f"Saved SHAP bar plot to {bar_path}")
print(f"Saved SHAP bar plot to {bar_pdf_path}")
print(f"Saved SHAP summary plot to {summary_path}")
print(f"Saved SHAP summary plot to {summary_pdf_path}")
PY
