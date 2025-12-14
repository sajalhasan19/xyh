#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOU'
Usage: Rebin.sh <xyh_binary_x<mass>_y<mass>> [options]

Options:
  -v, --version <tag>             Defaults to $VERSION or ml_v5
  -c, --config <name>             Defaults to $CONFIG or config_2022post
  -s, --selector <name>           Defaults to $SELECTOR or default
  -p, --producers <name>          Defaults to $PRODUCERS or default
  -i, --inference-model <name>    Defaults to $INFERENCE_MODEL or xyh_limits
  -w, --workers <n>               Defaults to $WORKERS or 50
  -b, --bins-per-category <map>   e.g. "catA=3,catB=4"; passed through unchanged
  -h, --help                      Show this help

Environment variables with the same names override the defaults.
EOU
}

ml_model=""
version="${VERSION:-ml_v1}"
config="${CONFIG:-config_2022pre}"
selector="${SELECTOR:-default}"
producers="${PRODUCERS:-default}"
inference_model="${INFERENCE_MODEL:-xyh_limits}"
workers="${WORKERS:-30}"
bins_per_category="${BINS_PER_CATEGORY:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -v|--version)         version="${2:-}"; shift 2 ;;
    -c|--config)          config="${2:-}"; shift 2 ;;
    -s|--selector)        selector="${2:-}"; shift 2 ;;
    -p|--producers)       producers="${2:-}"; shift 2 ;;
    -i|--inference-model) inference_model="${2:-}"; shift 2 ;;
    -w|--workers)         workers="${2:-}"; shift 2 ;;
    -b|--bins-per-category) bins_per_category="${2:-}"; shift 2 ;;
    -h|--help)            usage; exit 0 ;;
    -*)
      echo "ERROR: unknown option '$1'" >&2
      usage; exit 1 ;;
    *)
      if [[ -z "${ml_model}" ]]; then
        ml_model="$1"
        shift
      else
        echo "ERROR: unexpected argument '$1'" >&2
        usage; exit 1
      fi
      ;;
  esac
done

if [[ -z "${ml_model}" ]]; then
  usage; exit 1
fi

if [[ ! ${ml_model} =~ ^xyh_binary_x([0-9]+)_y([0-9]+)$ ]]; then
  echo "ERROR: ML model must look like 'xyh_binary_x<mass>_y<mass>' (got '${ml_model}')." >&2
  exit 1
fi

if ! command -v law >/dev/null 2>&1; then
  echo "ERROR: 'law' command not found. Please source setup.sh before running this script." >&2
  exit 1
fi

cmd=(
  law run xyh.ModifyDatacardsFlatRebin
  --version "${version}"
  --configs "${config}"
  --selector "${selector}"
  --producers "${producers}"
  --ml-models "${ml_model}"
  --inference-model "${inference_model}"
  --workers "${workers}"
)

if [[ -n "${bins_per_category}" ]]; then
  cmd+=(--bins-per-category "${bins_per_category}")
fi

echo "[+] Running: ${cmd[*]}"
"${cmd[@]}"
