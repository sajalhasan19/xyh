#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  RunExistingDatacardImpacts.sh <mass_x> <mass_y> [version]
  RunExistingDatacardImpacts.sh all [version]

Examples:
  ./RunExistingDatacardImpacts.sh 1000 700 ttbb_v30
  ./RunExistingDatacardImpacts.sh all ttbb_v30

Environment overrides:
  VERSION          Default: ttbb_v30
  CONFIG           Default: config_2022post
  HIST_PRODUCER    Default: all_weights
  INFERENCE_MODEL  Default: xyh_limits
  ML_MODEL         Default: xyh_pnn_parameterized
  MASS             Default: 125
  POI              Default: r
  POI_RANGE        Default: -50,50
  USE_ASIMOV       Default: true
  EXPECT_SIGNAL    Default: 1 when USE_ASIMOV=true. Set empty to disable.
EOF
  exit 1
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${XYH_SETUP:-}" && -f "${script_dir}/setup.sh" ]]; then
  # Source the analysis environment so Combine tools are available when possible.
  # If the caller already sourced setup.sh, this block is skipped.
  # shellcheck source=/dev/null
  source "${script_dir}/setup.sh"
fi

for tool in text2workspace.py combineTool.py plotImpacts.py; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: '${tool}' not found. Please source setup.sh / Combine environment first." >&2
    exit 1
  fi
done

VERSION="${VERSION:-ttbb_v30}"
CONFIG="${CONFIG:-config_2022post}"
HIST_PRODUCER="${HIST_PRODUCER:-all_weights}"
INFERENCE_MODEL="${INFERENCE_MODEL:-xyh_limits}"
ML_MODEL="${ML_MODEL:-xyh_pnn_parameterized}"
MASS="${MASS:-125}"
POI="${POI:-r}"
POI_RANGE="${POI_RANGE:--50,50}"
USE_ASIMOV="${USE_ASIMOV:-true}"
EXPECT_SIGNAL="${EXPECT_SIGNAL-1}"

if [[ $# -lt 1 ]]; then
  usage
fi

if [[ $# -ge 3 ]]; then
  VERSION="$3"
elif [[ $# -eq 2 && "$1" == "all" ]]; then
  VERSION="$2"
fi

base_dir="${script_dir}/data/cf_store/analysis_xyh/xyh.ModifyDatacardsFlatRebin/${CONFIG}/calib__default/sel__default/red__cf_default/prod__default/hist__${HIST_PRODUCER}/inf__${INFERENCE_MODEL}"

if [[ ! -d "$base_dir" ]]; then
  echo "ERROR: base datacard directory not found: ${base_dir}" >&2
  exit 1
fi

mass_repr="${MASS//./p}"
workspace_name="workspace_m${mass_repr}.root"
impact_suffix=""

asimov_args=()
case "${USE_ASIMOV,,}" in
  true|1|yes|y)
    asimov_args=(-t -1)
    if [[ -n "$EXPECT_SIGNAL" ]]; then
      asimov_args+=(--expectSignal "$EXPECT_SIGNAL")
      impact_suffix="_expectSignal${EXPECT_SIGNAL//./p}"
    fi
    ;;
  false|0|no|n)
    ;;
  *)
    echo "ERROR: USE_ASIMOV must be true or false, got '${USE_ASIMOV}'." >&2
    exit 1
    ;;
esac

impacts_json="impacts_m${mass_repr}${impact_suffix}.json"
impacts_base="impacts_m${mass_repr}${impact_suffix}"

find_card_dir() {
  local mass_x="$1"
  local mass_y="$2"
  local signal="xyh_sl_x${mass_x}_y${mass_y}"
  local matches=()
  local card

  while IFS= read -r card; do
    [[ "$card" == *"/ml__${ML_MODEL}__"*"/${VERSION}/datacard_combined.txt" ]] || continue
    if grep -q "$signal" "$card"; then
      matches+=("$(dirname "$card")")
    fi
  done < <(find "$base_dir" -type f -path "*/${VERSION}/datacard_combined.txt" | sort)

  if [[ ${#matches[@]} -eq 0 ]]; then
    echo "ERROR: no datacard found for ${signal} and version '${VERSION}' under:" >&2
    echo "  ${base_dir}" >&2
    return 1
  fi
  if [[ ${#matches[@]} -gt 1 ]]; then
    echo "ERROR: multiple datacards found for ${signal} and version '${VERSION}':" >&2
    printf '  %s\n' "${matches[@]}" >&2
    return 1
  fi

  printf '%s\n' "${matches[0]}"
}

run_impacts() {
  local mass_x="$1"
  local mass_y="$2"
  local card_dir
  card_dir="$(find_card_dir "$mass_x" "$mass_y")"

  echo
  echo "[+] Running impacts for mX=${mass_x}, mY=${mass_y}, version=${VERSION}"
  echo "[+] Datacard directory: ${card_dir}"

  (
    cd "$card_dir"

    text2workspace.py datacard_combined.txt -m "$MASS" -o "$workspace_name"

    combineTool.py -M Impacts \
      -d "$workspace_name" \
      -m "$MASS" \
      --robustFit 1 \
      --cminDefaultMinimizerStrategy 0 \
      --setParameterRanges "${POI}=${POI_RANGE}" \
      --X-rtd MINIMIZER_no_analytic \
      --doInitialFit \
      "${asimov_args[@]}" \
      -v 3

    combineTool.py -M Impacts \
      -d "$workspace_name" \
      -m "$MASS" \
      --robustFit 1 \
      --cminDefaultMinimizerStrategy 0 \
      --setParameterRanges "${POI}=${POI_RANGE}" \
      --X-rtd MINIMIZER_no_analytic \
      --doFits \
      "${asimov_args[@]}"

    combineTool.py -M Impacts \
      -d "$workspace_name" \
      -m "$MASS" \
      --robustFit 1 \
      --cminDefaultMinimizerStrategy 0 \
      --setParameterRanges "${POI}=${POI_RANGE}" \
      --X-rtd MINIMIZER_no_analytic \
      -o "$impacts_json" \
      "${asimov_args[@]}"

    plotImpacts.py -i "$impacts_json" -o "$impacts_base"
  )

  echo "[+] Wrote ${card_dir}/${impacts_json}"
  echo "[+] Wrote ${card_dir}/${impacts_base}.pdf"
}

discover_points() {
  local card signal
  while IFS= read -r card; do
    [[ "$card" == *"/ml__${ML_MODEL}__"*"/${VERSION}/datacard_combined.txt" ]] || continue
    signal="$(grep -o 'xyh_sl_x[0-9]\+_y[0-9]\+' "$card" | head -1 || true)"
    [[ -n "$signal" ]] || continue
    printf '%s\n' "$signal" | sed -E 's/xyh_sl_x([0-9]+)_y([0-9]+)/\1 \2/'
  done < <(find "$base_dir" -type f -path "*/${VERSION}/datacard_combined.txt" | sort) | sort -n -k1,1 -k2,2
}

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
elif [[ "$1" == "all" ]]; then
  mapfile -t points < <(discover_points)
  if [[ ${#points[@]} -eq 0 ]]; then
    echo "ERROR: no mass points found for version '${VERSION}' under ${base_dir}" >&2
    exit 1
  fi
  for point in "${points[@]}"; do
    read -r mass_x mass_y <<<"$point"
    run_impacts "$mass_x" "$mass_y"
  done
elif [[ $# -ge 2 ]]; then
  run_impacts "$1" "$2"
else
  usage
fi
