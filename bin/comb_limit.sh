#!/usr/bin/env bash
set -euo pipefail

# Combine per-category datacards, build the workspace, and run blind limits.
# Usage:
#   comb_limit.sh <datacard_dir> <mass_x> <mass_y> [tag=RunIII] [mH=125] [output_dir]
# Example:
#   ./comb_limit.sh /path/to/ttbb_v30 1000 700 RunIII

export PYTHONNOUSERSITE=1

default_base_dir="/data/dust/user/hasansye/xyh/data/cf_store/analysis_xyh/cf.CreateDatacards/config_2022pre/calib__default/sel__default/red__cf_default/prod__default/hist__cf_default/inf__xyh_limits"
datacard_dir_pattern="ttbb_v30"

if [[ $# -eq 0 ]]; then
  base_dir="${COMB_LIMIT_BASE_DIR:-${default_base_dir}}"

  if [[ ! -d "${base_dir}" ]]; then
    echo "ERROR: default base directory '${base_dir}' does not exist." >&2
    exit 1
  fi

  shopt -s nullglob
  mapfile -t masspoint_dirs < <(find "${base_dir}" -maxdepth 1 -type d -name "${datacard_dir_pattern}" -print)
  shopt -u nullglob

  if [[ ${#masspoint_dirs[@]} -eq 0 ]]; then
    echo "ERROR: no datacard directories matching '${datacard_dir_pattern}' found under '${base_dir}'." >&2
    exit 1
  fi

  script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

  echo "[+] Found ${#masspoint_dirs[@]} datacard directories under '${base_dir}'"

  for dir in "${masspoint_dirs[@]}"; do
    echo "[+] Processing $(basename "${dir}")"
    "${script_path}" "${dir}"
  done

  echo "[+] All mass points processed."
  exit 0
fi

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <datacard_dir> <mass_x> <mass_y> [tag=RunIII] [mH=125] [output_dir]" >&2
  exit 1
fi

original_input="$1"
mass_x="$2"
mass_y="$3"
tag="${4:-RunIII}"
mass="${5:-125}"

mass_point="x${mass_x}_y${mass_y}"
combine_tag="${tag}_${mass_point}"

default_output_dir="/data/dust/user/hasansye/xyh/combine_outputs/${tag}_config_2022post__ttbb_v30(2)"
output_dir="${6:-${default_output_dir}}"

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
    echo "ERROR: datacard directory or file '${original_input}' does not exist." >&2
    echo "Also checked '${alt_path}'." >&2
    exit 1
  fi
fi

mkdir -p "${output_dir}"

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
    echo "ERROR: file '${original_input}' is not a recognized datacard." >&2
    echo "Expected 'datacard__cat_*.txt' or 'card_*__cfg_*__cat_*.txt'." >&2
    exit 1
  fi
else
  mapfile -t datacard_cards < <(
    find "${card_dir}" -maxdepth 1 -type f -name "datacard__cat_*.txt" -printf "%f\n" | sort
  )

  mapfile -t rebinned_cards < <(
    find "${card_dir}" -maxdepth 1 -type f -name "card_*__cfg_*.txt" -printf "%f\n" | sort
  )
fi

echo "[DEBUG] datacard_cards:"
printf '  %s\n' "${datacard_cards[@]:-}"

echo "[DEBUG] rebinned_cards:"
printf '  %s\n' "${rebinned_cards[@]:-}"

if [[ ${#datacard_cards[@]} -eq 0 && ${#rebinned_cards[@]} -eq 0 ]]; then
  echo "ERROR: found no valid datacard files under '${card_dir}'." >&2
  echo "Expected files like:" >&2
  echo "  datacard__cat_*.txt" >&2
  echo "  card_*__cfg_*.txt" >&2
  exit 1
fi

combine_cmd=(combineCards.py)
total_cards=0

for card in "${datacard_cards[@]}"; do
  card_path="${card_dir}/${card}"

  if [[ ! -f "${card_path}" ]]; then
    echo "[!] Skipping non-file datacard candidate: ${card_path}" >&2
    continue
  fi

  label="${card#datacard__}"
  label="${label%.txt}"

  combine_cmd+=("${label}=${card_path}")
  ((++total_cards))
done

for card in "${rebinned_cards[@]}"; do
  card_path="${card_dir}/${card}"

  if [[ ! -f "${card_path}" ]]; then
    echo "[!] Skipping non-file datacard candidate: ${card_path}" >&2
    continue
  fi

  label="${card#card_*__cfg_*__}"
  label="${label%.txt}"

  combine_cmd+=("${label}=${card_path}")
  ((++total_cards))
done

if [[ ${total_cards} -eq 0 ]]; then
  echo "ERROR: no valid datacard files found under '${card_dir}'." >&2
  exit 1
fi

combined_card="${card_dir}/datacard_combined.txt"

echo "[+] Combining ${total_cards} datacards into '${combined_card}'"
"${combine_cmd[@]}" > "${combined_card}"

workspace="${card_dir}/workspace_m${mass}.root"
workspace_name="$(basename "${workspace}")"

echo "[+] Creating workspace '${workspace}'"
text2workspace.py "${combined_card}" -m "${mass}" -o "${workspace}"

log_path="${output_dir}/AsymptoticLimits_${category_label}_${mass_point}_${tag}.log"

echo "[+] Running blind limits"
echo "    mH          = ${mass}"
echo "    mass point  = ${mass_point}"
echo "    combine tag = ${combine_tag}"
echo "    log         = ${log_path}"

(
  cd "${card_dir}"
  combine -M AsymptoticLimits "${workspace_name}" \
    --run blind \
    -m "${mass}" \
    -n "${combine_tag}"
) | tee "${log_path}"

combine_root="${card_dir}/higgsCombine${combine_tag}.AsymptoticLimits.mH${mass}.root"

if [[ -f "${combine_root}" ]]; then
  echo "[+] Combine ROOT output left in '${combine_root}'"
else
  echo "[!] WARNING: expected Combine output '${combine_root}' not found." >&2
fi

echo "[+] Done."