#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: Train_to_Cards.sh [--version <tag>] [--model <name>]

Options:
  -v, --version   Override the VERSION environment variable passed to downstream tasks
  -m, --model     Process only the specified ML model (e.g. xyh_binary_x700_y500)
  -h, --help      Show this help and exit

The list of trained mass points is maintained inside the script. Edit the
"models" array below if you need a different set.
EOF
  exit 1
}

version="${VERSION:-ml_v1}"
single_model=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      version="$2"
      shift 2
      ;;
    -m|--model)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: missing argument for '$1'." >&2
        usage
      fi
      single_model="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "ERROR: unknown option '$1'." >&2
      usage
      ;;
  esac
done

if [[ -n "${version}" ]]; then
  export VERSION="${version}"
  echo "[+] Using VERSION='${VERSION}'"
fi

# Maintain the valid mass points yourself.
models=(
  xyh_binary_x500_y350
  xyh_binary_x550_y350
  xyh_binary_x550_y400
  xyh_binary_x600_y350
  xyh_binary_x600_y400
  xyh_binary_x600_y450
  xyh_binary_x650_y350
  xyh_binary_x650_y400
  xyh_binary_x650_y450
  xyh_binary_x650_y500
  xyh_binary_x700_y350
  xyh_binary_x700_y400
  xyh_binary_x700_y450
  xyh_binary_x700_y500
  xyh_binary_x700_y550
  xyh_binary_x750_y350
  xyh_binary_x750_y400
  xyh_binary_x750_y450
  xyh_binary_x750_y500
  xyh_binary_x750_y550
  xyh_binary_x750_y600
  xyh_binary_x800_y350
  xyh_binary_x800_y400
  xyh_binary_x800_y450
  xyh_binary_x800_y500
  xyh_binary_x800_y550
  xyh_binary_x800_y600
  xyh_binary_x800_y650
  xyh_binary_x850_y350
  xyh_binary_x850_y400
  xyh_binary_x850_y450
  xyh_binary_x850_y500
  xyh_binary_x850_y550
  xyh_binary_x850_y600
  xyh_binary_x850_y650
  xyh_binary_x850_y700
  xyh_binary_x900_y350
  xyh_binary_x900_y400
  xyh_binary_x900_y450
  xyh_binary_x900_y500
  xyh_binary_x900_y550
  xyh_binary_x900_y600
  xyh_binary_x900_y650
  xyh_binary_x900_y700
  xyh_binary_x900_y750
  xyh_binary_x950_y350
  xyh_binary_x950_y400
  xyh_binary_x950_y450
  xyh_binary_x950_y500
  xyh_binary_x950_y550
  xyh_binary_x950_y600
  xyh_binary_x950_y650
  xyh_binary_x950_y700
  xyh_binary_x950_y800
  xyh_binary_x1000_y350
  xyh_binary_x1000_y400
  xyh_binary_x1000_y450
  xyh_binary_x1000_y500
  xyh_binary_x1000_y550
  xyh_binary_x1000_y600
  xyh_binary_x1000_y650
  xyh_binary_x1000_y700
  xyh_binary_x1000_y800
  xyh_binary_x1200_y500
  xyh_binary_x1200_y600
  xyh_binary_x1200_y800
  xyh_binary_x1200_y1000
  xyh_binary_x1400_y600
  # add/remove entries here as needed
)

for model in "${models[@]}"; do
  if [[ -n "${single_model}" && "${model}" != "${single_model}" ]]; then
    continue
  fi

  if [[ "${model}" =~ ^xyh_binary_(x[0-9]+_y[0-9]+)$ ]]; then
    export XYH_SIGNAL_PROCESS="xyh_sl_${BASH_REMATCH[1]}"
    echo "[+] Using XYH_SIGNAL_PROCESS='${XYH_SIGNAL_PROCESS}'"
  else
    echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${model}')." >&2
    exit 1
  fi

  echo "[+] Processing ${model}"
  ./TrainAllModels.sh "${model}" &&
  ./RunMLEvaluations.sh "${model}" &&
  ./RunMergeMLEvaluations.sh "${model}" &&
  ./PlotMLResults.sh "${model}" &&
  ./PlotNNScore.sh "${model}" --raw &&
  ./PlotNNScore.sh "${model}" --logit &&
  ./RunCreateDatacards.sh "${model}" &&
  ./Rebin.sh "${model}"
done
