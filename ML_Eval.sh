#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <ml_model>" >&2
  exit 1
}

ml_model=${1:-}
[[ -z $ml_model ]] && usage

# derive mass point from model name xyh_binary_x###_y###
if [[ $ml_model =~ xyh_binary_(x[0-9]+)_ (y[0-9]+) ]]; then
  :
fi

# more data
