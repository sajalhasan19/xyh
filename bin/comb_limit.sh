#!/usr/bin/env bash
set -euo pipefail

# Combine per-category datacards, build the workspace, and run blind limits.
# Usage:
#   comb_limit.sh <datacard_dir> [mH=125] [tag=RunIII] [output_dir=combine_outputs/<tag>]
#   comb_limit.sh                     # iterate over all mass-point directories under the default base dir

# Ensure CMSSW-provided Python packages (numpy/pandas/etc.) are used instead of user site packages.
export PYTHONNOUSERSITE=1

default_base_dir="/data/dust/user/hasansye/xyh/data/cf_store/analysis_xyh/cf.CreateDatacards/config_2022pre/calib__default/sel__default/red__cf_default/prod__default/hist__cf_default/inf__xyh_limits"
datacard_dir_pattern="rebin_ml_v1_cmall_raw"

if [[ $# -eq 0 ]]; then
  base_dir="${COMB_LIMIT_BASE_DIR:-${default_base_dir}}"
  if [[ ! -d "${base_dir}" ]]; then
    echo "ERROR: default base directory '${base_dir}' does not exist." >&2
    exit 1
  fi

  shopt -s nullglob
  mapfile -t masspoint_dirs < <(printf '%s\n' "${base_dir}"/${datacard_dir_pattern})
  shopt -u nullglob

  if [[ ${#masspoint_dirs[@]} -eq 0 ]]; then
    echo "ERROR: no datacard directories matching pattern '${datacard_dir_pattern}' found under '${base_dir}'." >&2
    exit 1
  fi

  script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

  echo "[+] Found ${#masspoint_dirs[@]} datacard directories under '${base_dir}'"
  for dir in "${masspoint_dirs[@]}"; do
    if [[ ! -d "${dir}" ]]; then
      continue
    fi
    echo "[+] Processing $(basename "${dir}")"
    "${script_path}" "${dir}"
  done
  echo "[+] All mass points processed."
  exit 0
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <datacard_dir> [mH=125] [tag=RunIII] [output_dir=combine_outputs/<tag>]" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
original_input="$1"
use_single_card=0
single_card=""

if [[ -f "${original_input}" ]]; then
  card_dir="$(cd "$(dirname "${original_input}")" && pwd)"
  single_card="$(basename "${original_input}")"
  use_single_card=1
elif [[ -d "${original_input}" ]]; then
  card_dir="$(cd "${original_input}" && pwd)"
else
  base_dir="${COMB_LIMIT_BASE_DIR:-${default_base_dir}}"
  alt_path="${base_dir}/${original_input}"
  if [[ -d "${alt_path}" ]]; then
    card_dir="$(cd "${alt_path}" && pwd)"
  elif [[ -f "${alt_path}" ]]; then
    card_dir="$(cd "$(dirname "${alt_path}")" && pwd)"
    single_card="$(basename "${alt_path}")"
    use_single_card=1
  else
    echo "ERROR: datacard directory or file '${original_input}' (or '${alt_path}') does not exist." >&2
    exit 1
  fi
fi
mass="${2:-125}"
tag="${3:-RunIII}"
default_output_dir="/data/dust/user/hasansye/xyh/combine_outputs/${tag}_config_2022post__rebinned_ml_unc_pnn_v15"
output_dir="${4:-${default_output_dir}}"

mkdir -p "${output_dir}"

card_basename="$(basename "${card_dir}")"
mass_point="mH${mass}"
if [[ "${card_dir}" =~ x([0-9]+)_y([0-9]+) ]]; then
  # Use the xNNN_yMMM pattern from anywhere in the path if available.
  mass_point="${BASH_REMATCH[0]}"
fi

# Derive a category label to include in output names.
category_label="$(basename "${card_dir}")"
if [[ ${use_single_card} -eq 1 ]]; then
  if [[ "${single_card}" == datacard__cat_*.txt ]]; then
    category_label="${single_card#datacard__}"
    category_label="${category_label%.txt}"
  elif [[ "${single_card}" == card_*__cfg_*__cat_*.txt ]]; then
    category_label="${single_card#card_*__cfg_*__}"
    category_label="${category_label%.txt}"
  fi
fi

datacard_cards=()
rebinned_cards=()

if [[ ${use_single_card} -eq 1 ]]; then
  if [[ "${single_card}" == datacard__cat_*.txt ]]; then
    datacard_cards=("${single_card}")
  elif [[ "${single_card}" == card_*__cfg_*__cat_*.txt ]]; then
    rebinned_cards=("${single_card}")
  else
    echo "ERROR: file '${original_input}' is not a recognized datacard (expected 'datacard__cat_*.txt' or 'card_*__cfg_*__cat_*.txt')." >&2
    exit 1
  fi
  for card in "${datacard_cards[@]}" "${rebinned_cards[@]}"; do
    if [[ -n "${card}" && ! -f "${card_dir}/${card}" ]]; then
      echo "ERROR: expected datacard file '${card_dir}/${card}' not found." >&2
      exit 1
    fi
  done
else
  # build the list of per-category cards from the directory
  shopt -s nullglob
  mapfile -t datacard_cards < <(cd "${card_dir}" && printf '%s\n' datacard__cat_*.txt)
  mapfile -t rebinned_cards < <(cd "${card_dir}" && printf '%s\n' card_*__cfg_*.txt)
  shopt -u nullglob

  # drop empty entries which occur when no files matched the glob
  clean_cards() {
    local -n _target=$1
    local cleaned=()
    local item
    for item in "${_target[@]}"; do
      [[ -z "${item}" ]] && continue
      cleaned+=("${item}")
    done
    _target=("${cleaned[@]}")
  }
  clean_cards datacard_cards
  clean_cards rebinned_cards
fi

if [[ ${#datacard_cards[@]} -eq 0 && ${#rebinned_cards[@]} -eq 0 ]]; then
  echo "ERROR: found no 'datacard__cat_*.txt' or 'card_*__cfg_*__cat_*.txt' files under '${card_dir}'." >&2
  exit 1
fi

combine_cmd=(combineCards.py)
total_cards=0
for card in "${datacard_cards[@]}"; do
  label="${card#datacard__}"
  label="${label%.txt}"
  card_path="${card_dir}/${card}"
  combine_cmd+=("${label}=${card_path}")
  ((++total_cards))
done

for card in "${rebinned_cards[@]}"; do
  label="${card#card_*__cfg_*__}"
  label="${label%.txt}"
  card_path="${card_dir}/${card}"
  combine_cmd+=("${label}=${card_path}")
  ((++total_cards))
done

combined_card="${card_dir}/datacard_combined.txt"
echo "[+] Combining ${total_cards} datacards into '${combined_card}'"
"${combine_cmd[@]}" > "${combined_card}"

workspace="${card_dir}/workspace_m${mass}.root"
workspace_name="$(basename "${workspace}")"
echo "[+] Creating workspace '${workspace}'"
text2workspace.py "${combined_card}" -m "${mass}" -o "${workspace}"

log_path="${output_dir}/AsymptoticLimits_${category_label}_${mass_point}_${tag}.log"
echo "[+] Running blind limits with mH=${mass}, tag='${tag}' (log: ${log_path})"
(
  cd "${card_dir}"
  combine -M AsymptoticLimits "${workspace_name}" --run blind -m "${mass}" -n "${tag}"
    #--freezeParameters allConstrainedNuisances,tt_norm
) | tee "${log_path}"

combine_root="${card_dir}/higgsCombine${tag}.AsymptoticLimits.mH${mass}.root"
if [[ -f "${combine_root}" ]]; then
  echo "[+] Combine ROOT output left in '${combine_root}'"
else
  echo "[!] WARNING: expected Combine output '${combine_root}' not found." >&2
fi

echo "[+] Done."
