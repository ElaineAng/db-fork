#!/usr/bin/env bash
# Run one macrobenchmark scenario on a backend.
#
# Usage:
#   ./scripts/run_macrobench.sh [--mini] [--outdir DIR] [--max-runtime-sec N] [--measure-storage] \
#       [--branch-intensity X] [--data-intensity Y] \
#       <scenario> <backend> [scale_factor]
#
# Arguments:
#   scenario      rl_env | context_mgmt | multi_agent | dev_agent | ops_agent | data_agent
#   backend       dolt | dolt_mysql | seekdb | matrixone | neon | xata | file_copy
#   scale_factor  W warehouses for the generated CH-benCHmark data (default: the config's)
#
# The base config is macrobench/configs/<scenario>[_mini].textproto; run_id,
# backend and scale_factor are patched into a temporary copy.
#
# Examples:
#   ./scripts/run_macrobench.sh --mini rl_env dolt
#   ./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 data_agent dolt 5

set -euo pipefail

MINI=false
MEASURE_STORAGE=false
OUTDIR="run_stats/"
BRANCH_INTENSITY=""
DATA_INTENSITY=""
MAX_RUNTIME_SEC=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mini)            MINI=true; shift ;;
        --measure-storage) MEASURE_STORAGE=true; shift ;;
        --outdir)          OUTDIR="$2"; shift 2 ;;
        --branch-intensity) BRANCH_INTENSITY="$2"; shift 2 ;;
        --data-intensity)   DATA_INTENSITY="$2"; shift 2 ;;
        --max-runtime-sec) MAX_RUNTIME_SEC="$2"; shift 2 ;;
        *)                 break ;;
    esac
done

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 [--mini] [--outdir DIR] [--max-runtime-sec N] [--measure-storage] [--branch-intensity X] [--data-intensity Y] <scenario> <backend> [scale_factor]"
    echo "  scenario:  rl_env | context_mgmt | multi_agent | dev_agent | ops_agent | data_agent"
    echo "  backend:   dolt | dolt_mysql | seekdb | matrixone | neon | xata | file_copy"
    exit 1
fi

SCENARIO="$1"
BACKEND="$2"
SCALE="${3:-}"

VALID_SCENARIOS="rl_env context_mgmt multi_agent dev_agent ops_agent data_agent"
if ! echo "$VALID_SCENARIOS" | grep -qw "$SCENARIO"; then
    echo "Error: invalid scenario '$SCENARIO' (one of: $VALID_SCENARIOS)"
    exit 1
fi
VALID_BACKENDS="dolt dolt_mysql seekdb matrixone neon xata file_copy"
if ! echo "$VALID_BACKENDS" | grep -qw "$BACKEND"; then
    echo "Error: invalid backend '$BACKEND' (one of: $VALID_BACKENDS)"
    exit 1
fi

BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')
if $MINI; then
    SUFFIX="_mini"
else
    SUFFIX=""
fi
RUN_ID="macro_${SCENARIO}${SUFFIX}_${BACKEND}${SCALE:+_w$SCALE}${BRANCH_INTENSITY:+_b$BRANCH_INTENSITY}${DATA_INTENSITY:+_d$DATA_INTENSITY}"
BASE_CONFIG="macrobench/configs/${SCENARIO}${SUFFIX}.textproto"
if [[ ! -f "$BASE_CONFIG" ]]; then
    echo "Error: base config not found: $BASE_CONFIG"
    exit 1
fi

TMP_CONFIG=$(mktemp /tmp/macrobench_$$_XXXXXX)
trap 'rm -f "$TMP_CONFIG"' EXIT

SED_ARGS=(-e "s|^run_id:.*|run_id: \"${RUN_ID}\"|" -e "s|^backend:.*|backend: ${BACKEND_UPPER}|")
if [[ -n "$SCALE" ]]; then
    SED_ARGS+=(-e "s|scale_factor: [0-9]*|scale_factor: ${SCALE}|")
fi
sed "${SED_ARGS[@]}" "$BASE_CONFIG" > "$TMP_CONFIG"
# Intensity multipliers are inserted as the first lines of the workload block.
if [[ -n "$BRANCH_INTENSITY" || -n "$DATA_INTENSITY" ]]; then
    INTENSITY_LINES=""
    [[ -n "$BRANCH_INTENSITY" ]] && INTENSITY_LINES+="  branch_intensity: ${BRANCH_INTENSITY}\n"
    [[ -n "$DATA_INTENSITY" ]] && INTENSITY_LINES+="  data_intensity: ${DATA_INTENSITY}\n"
    sed -i -e "s|^workload {|workload {\n${INTENSITY_LINES%\\n}|" "$TMP_CONFIG"
fi

echo "=== Macrobench Run ==="
echo "  Scenario:  $SCENARIO${SUFFIX}"
echo "  Backend:   $BACKEND_UPPER"
echo "  Scale:     ${SCALE:-config default}"
echo "  Intensity: branch x${BRANCH_INTENSITY:-1} data x${DATA_INTENSITY:-1}"
echo "  Run ID:    $RUN_ID"
echo "  Output:    $OUTDIR"
echo "  Timeout:   ${MAX_RUNTIME_SEC}s (0 = no limit)"
echo "  Config:    $TMP_CONFIG (patched from $BASE_CONFIG)"
echo "======================"

EXTRA_FLAGS=()
if $MEASURE_STORAGE; then
    EXTRA_FLAGS+=(--measure-storage)
fi

PYTHONUNBUFFERED=1 uv run python -m macrobench.runner \
    --config "$TMP_CONFIG" \
    --outdir "$OUTDIR" \
    --max-runtime-sec "$MAX_RUNTIME_SEC" \
    ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"}
