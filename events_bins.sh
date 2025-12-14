#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: events_bins.sh <histogram_pickle> [process] [shift] [category_list]

Example:
  events_bins.sh analysis_data/.../hist__var_logit_nn_score__xyh_binary_x800_y500.pickle background nominal 1mu__2bjets__4jets
  events_bins.sh analysis_data/.../hist__var_logit_nn_score__xyh_binary_x800_y500.pickle background nominal "catA,catB"

Shows the per-bin yields for a logit NN-score histogram both summed over categories
and split per category for the requested process/shift. Defaults: process=background,
shift=nominal. Toggle sections via environment variables:
  SHOW_SUMMED=0 ./events_bins.sh ...
  SHOW_PER_CAT=0 ./events_bins.sh ...
Override or repeat categories via the optional 4th argument (comma-separated) or by
setting CATEGORY_FILTER="cat1,cat2". When histogram pickles contain multiple histograms,
set HIST_KEY to the desired entry name, e.g. HIST_KEY=logit_nn_score__xyh_binary_x600_y350.
EOF
}

hist_path=${1:-}
if [[ -z ${hist_path} ]]; then
  usage
  exit 1
fi

process=${2:-background}
shift_name=${3:-nominal}
category_filter_arg=${4:-}
show_summed=${SHOW_SUMMED:-1}
show_per_cat=${SHOW_PER_CAT:-1}
category_filter=${CATEGORY_FILTER:-$category_filter_arg}

python - <<'PY' \
  "$hist_path" \
  "$process" \
  "$shift_name" \
  "$show_summed" \
  "$show_per_cat" \
  "$category_filter"
import os
import pickle
import sys
from pathlib import Path
from hist import tag

hist_path = Path(sys.argv[1])
process = sys.argv[2]
shift_name = sys.argv[3]
show_summed = bool(int(sys.argv[4]))
show_per_cat = bool(int(sys.argv[5]))
category_filter = sys.argv[6]
hist_key = os.environ.get("HIST_KEY", "").strip()

with hist_path.open("rb") as f:
    hist_obj = pickle.load(f)

if isinstance(hist_obj, dict):
    if hist_key:
        if hist_key not in hist_obj:
            available = ", ".join(sorted(hist_obj))
            raise SystemExit(f"Histogram '{hist_key}' not found in {hist_path}. Available: {available}")
        hist_obj = hist_obj[hist_key]
    elif len(hist_obj) == 1:
        hist_obj = next(iter(hist_obj.values()))
    else:
        available = ", ".join(sorted(hist_obj))
        raise SystemExit(
            "Histogram file contains multiple entries. Set HIST_KEY=<name> to choose one. "
            f"Available: {available}"
        )

axes = {ax.name: ax for ax in hist_obj.axes}
required = {"category", "process", "shift"}
missing = required - axes.keys()
if missing:
    raise SystemExit(f"Histogram is missing axes: {', '.join(sorted(missing))}")

def axis_values(axis):
    identifiers = getattr(axis, "identifiers", None)
    if identifiers is not None:
        return identifiers
    if hasattr(axis, "size") and hasattr(axis, "value"):
        return [axis.value(i) for i in range(axis.size)]
    return None

def ensure_identifier(axis_name, value):
    values = axis_values(axes[axis_name])
    if values is None:
        return
    if value not in values:
        available = ", ".join(str(v) for v in values)
        raise SystemExit(f"Unknown {axis_name} '{value}'. Available: {available}")

ensure_identifier("process", process)
ensure_identifier("shift", shift_name)

def dump(view, label):
    edges = view.axes[-1].edges
    counts = view.values()
    errs = view.variances() ** 0.5
    for low, high, val, err in zip(edges[:-1], edges[1:], counts, errs):
        print(f"{label:>20} {process:>12} [{low:5.2f},{high:5.2f}): {val:.3f} ± {err:.3f}")
    print()

# summed over all categories
if show_summed:
    view_all = hist_obj[{ "category": tag.sum, "process": process, "shift": shift_name }]
    dump(view_all, "all_categories")

# per category breakdown
if show_per_cat:
    cats = [c.strip() for c in category_filter.split(",") if c.strip()] if category_filter else axes["category"].identifiers
    unknown = [c for c in cats if c not in axes["category"].identifiers]
    if unknown:
        raise SystemExit(f"Unknown categories requested: {', '.join(unknown)}")
    for cat in cats:
        view = hist_obj[{ "category": cat, "process": process, "shift": shift_name }]
        dump(view, cat)
PY
