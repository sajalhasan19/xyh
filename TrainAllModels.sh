#!/usr/bin/env bash
set -euo pipefail

# Run cf.MLTraining for all available XYH binary models (up to x <= 2000).
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${script_dir}/ml_settings_utils.sh"

config="${CONFIG:-config_2022post}"
selector="default"
calibrator="default"
reducer="cf_default"
producers="default"
version="${VERSION:-ml_v1_binary_2022post}"
# training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
training_categories="${TRAINING_CATEGORIES:-1lep__3bjets__4jets;1lep__3bjets__5jets;1lep__3bjets__ge6jets;1lep__4bjets__5jets;1lep__ge4bjets__ge6jets}"
readarray -t available_models < <(python - <<'PY'
from xyh.inference.signals import XYH_SIGNAL_PROCESSES

models = []
for name in XYH_SIGNAL_PROCESSES:
    stem = name.removeprefix("xyh_sl_")
    mass_x = int(stem.split("_")[0][1:])
    # Allow trainings up to 2 TeV as announced in the script header.
    if mass_x > 2000:
        continue
    models.append(f"xyh_binary_{stem}")

for model in sorted(set(models)):
    print(model)
PY
)

declare -a models=()

if (($# > 0)); then
    declare -A seen=()
    for pattern in "$@"; do
        matched=0
        for candidate in "${available_models[@]}"; do
            if [[ "${candidate}" == ${pattern} ]]; then
                if [[ -z ${seen["${candidate}"]+x} ]]; then
                    models+=("${candidate}")
                    seen["${candidate}"]=1
                fi
                matched=1
            fi
        done
        if ((matched == 0)); then
            echo "[-] No models match pattern: ${pattern}" >&2
            exit 1
        fi
    done
else
    models=("${available_models[@]}")
fi

echo "[+] Will train ${#models[@]} model(s) (version=${version})"

for model in "${models[@]}"; do
    echo
    echo ">>> Training model: ${model}"
    ml_settings="$(resolve_ml_settings "${model}" "${training_categories}" "false" "true")"
    law run cf.MLTraining \
        --config "${config}" \
        --ml-model "${model}" \
        --version "${version}" \
        --selector "${selector}" \
        --calibrators "${calibrator}" \
        --reducer "${reducer}" \
        --producers "${producers}" \
        --workers 40 \
        ${ml_settings:+--ml-model-settings "${ml_settings}"}
done
echo
echo "[+] All trainings submitted."
