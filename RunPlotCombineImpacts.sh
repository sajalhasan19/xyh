#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: RunPlotCombineImpacts.sh [mass] [<law args>]
Defaults: mass=125

Environment knobs:
  VERSION          (default: ml_unc_pnn_x1000_y700_v5)
  CONFIGS          (default: config_2022post)
  CALIBRATORS      (default: default)
  SELECTOR         (default: default)
  REDUCER          (default: cf_default)
  PRODUCERS        (default: default)
  ML_MODELS        (default: xyh_pnn_parameterized)
  HIST_PRODUCER    (default: all_weights)
  INFERENCE_MODEL  (default: xyh_limits)
  USE_ASIMOV       (default: True)
  POI              (default: r)
  POI_RANGE        (default: -50,50)

Any additional arguments are forwarded to 'law run xyh.PlotCombineImpacts'.
EOF
  exit 1
}

MASS="125"
if [[ $# -ge 1 ]]; then
  case "$1" in
    -h|--help)
      usage
      ;;
    *)
      MASS="$1"
      shift
      ;;
  esac
fi

EXTRA_LAW_ARGS=("$@")

USE_LOCAL_SCHEDULER=true
for arg in "${EXTRA_LAW_ARGS[@]}"; do
  case "$arg" in
    --workflow|--workflow=*|--local-scheduler)
      USE_LOCAL_SCHEDULER=false
      ;;
  esac
done

SCHEDULER_ARGS=()
if [[ "$USE_LOCAL_SCHEDULER" == true ]]; then
  SCHEDULER_ARGS+=(--local-scheduler)
fi

VERSION="${VERSION:-ml_unc_pnn_x1000_y700_v5}"
CONFIGS="${CONFIGS:-config_2022post}"
CALIBRATORS="${CALIBRATORS:-default}"
SELECTOR="${SELECTOR:-default}"
REDUCER="${REDUCER:-cf_default}"
PRODUCERS="${PRODUCERS:-default}"
ML_MODELS="${ML_MODELS:-xyh_pnn_parameterized}"
HIST_PRODUCER="${HIST_PRODUCER:-all_weights}"
INFERENCE_MODEL="${INFERENCE_MODEL:-xyh_limits}"
USE_ASIMOV="${USE_ASIMOV:-True}"
POI="${POI:-r}"
POI_RANGE="${POI_RANGE:--50,50}"

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh first." >&2
  exit 1
fi

echo "Using VERSION: $VERSION"
echo "Using CONFIGS: $CONFIGS"
echo "Using CALIBRATORS: $CALIBRATORS"
echo "Using SELECTOR: $SELECTOR"
echo "Using REDUCER: $REDUCER"
echo "Using PRODUCERS: $PRODUCERS"
echo "Using ML_MODELS: $ML_MODELS"
echo "Using HIST_PRODUCER: $HIST_PRODUCER"
echo "Using INFERENCE_MODEL: $INFERENCE_MODEL"
echo "Using MASS: $MASS"
echo "Using USE_ASIMOV: $USE_ASIMOV"
echo "Using POI: $POI"
echo "Using POI_RANGE: $POI_RANGE"

law run xyh.PlotCombineImpacts "${SCHEDULER_ARGS[@]}" \
  --version "$VERSION" \
  --configs "$CONFIGS" \
  --calibrators "$CALIBRATORS" \
  --selector "$SELECTOR" \
  --reducer "$REDUCER" \
  --producers "$PRODUCERS" \
  --ml-models "$ML_MODELS" \
  --hist-producer "$HIST_PRODUCER" \
  --inference-model "$INFERENCE_MODEL" \
  --mass "$MASS" \
  --use-asimov "$USE_ASIMOV" \
  --poi "$POI" \
  --poi-range "$POI_RANGE" \
  "${EXTRA_LAW_ARGS[@]}"

echo "Done."
