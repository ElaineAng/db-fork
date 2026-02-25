#!/bin/bash
# Experiment 1: Branch Creation Storage Overhead (Varying Shape)
# Date: 2026-02-08
#
# Measures marginal storage cost per branch creation across topologies.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/bench_lib.sh"

# load_env
# eval "$("$REPO_ROOT/db_setup/setup_pg_volume.sh")"

SEED=42
SQL_DUMP="$REPO_ROOT/db_setup/tpcc_schema.sql"
# RUN_DOLT="${RUN_DOLT:-1}"
# RUN_FILE_COPY="${RUN_FILE_COPY:-1}"
# RUN_NEON="${RUN_NEON:-0}"
RUN_XATA="${RUN_XATA:-1}"
# NEON_MAX_BRANCHES="${NEON_MAX_BRANCHES:-8}"
XATA_MAX_BRANCHES="${XATA_MAX_BRANCHES:-16}"
SHAPES_CSV="${SHAPES_CSV:-spine,bushy,fan_out}"
NUM_BRANCHES_CSV="${NUM_BRANCHES_CSV:-1,2,4,8,16,32,64,128,256,512,1024}"

parse_shapes_csv() {
    local csv="${1// /}"
    IFS=',' read -r -a SHAPE_LIST <<< "$csv"
    if [ "${#SHAPE_LIST[@]}" -eq 0 ] || [ -z "${SHAPE_LIST[0]}" ]; then
        echo "Error: SHAPES_CSV is empty."
        exit 1
    fi
    for shape in "${SHAPE_LIST[@]}"; do
        case "$shape" in
            spine|bushy|fan_out) ;;
            *)
                echo "Error: invalid shape '$shape' in SHAPES_CSV."
                exit 1
                ;;
        esac
    done
}

parse_num_branches_csv() {
    local csv="${1// /}"
    local parsed=()
    local entry
    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: NUM_BRANCHES_CSV is empty."
        exit 1
    fi
    for entry in "${parsed[@]}"; do
        if ! [[ "$entry" =~ ^[0-9]+$ ]]; then
            echo "Error: invalid branch count '$entry' in NUM_BRANCHES_CSV."
            exit 1
        fi
        if [ "$entry" -le 0 ]; then
            echo "Error: branch count must be > 0, got '$entry'."
            exit 1
        fi
    done
    NUM_BRANCHES_LIST=("${parsed[@]}")
}

parse_shapes_csv "$SHAPES_CSV"
parse_num_branches_csv "$NUM_BRANCHES_CSV"

echo "============================================="
echo "Experiment 1: Branch Creation Storage (Shapes)"
echo "============================================="

# if [ "$RUN_DOLT" = "1" ]; then
#     for SHAPE in spine bushy fan_out; do
#         run_branch_sweep dolt "$SQL_DUMP" "$SHAPE" "$SEED" 1024 true BRANCH
#     done
# fi

# if [ "$RUN_FILE_COPY" = "1" ]; then
#     for SHAPE in spine bushy fan_out; do
#         run_branch_sweep file_copy "$SQL_DUMP" "$SHAPE" "$SEED" 1024 true BRANCH
#     done
# fi

# if [ "$RUN_NEON" = "1" ]; then
#     for SHAPE in spine bushy fan_out; do
#         run_branch_sweep neon "$SQL_DUMP" "$SHAPE" "$SEED" "$NEON_MAX_BRANCHES" true BRANCH
#     done
# fi

if [ "$RUN_XATA" = "1" ]; then
    for SHAPE in "${SHAPE_LIST[@]}"; do
        run_branch_sweep xata "$SQL_DUMP" "$SHAPE" "$SEED" "$XATA_MAX_BRANCHES" true BRANCH
    done
else
    echo "Xata skipped (set RUN_XATA=1 to run Xata)."
fi

echo ""
echo "Experiment 1 complete."
echo "Results in ${RUN_STATS_DIR:-/tmp/run_stats}/"

# Teardown (stop PG, detach volume)
# "$REPO_ROOT/db_setup/teardown_pg_volume.sh"
