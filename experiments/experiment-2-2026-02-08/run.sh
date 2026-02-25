#!/bin/bash
# Experiment 2: Per-Operation Storage Overhead
# Date: 2026-02-08
#
# Exp 2a: UPDATE + RANGE_UPDATE (fixed range_size=20, all topologies)
# Exp 2b: RANGE_UPDATE with varying range_size (spine only)
#
# Backends: Xata only (other backends commented out for concurrent run).

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
NUM_BRANCHES_CSV="${NUM_BRANCHES_CSV:-}"
EXP2_ENABLE_2A="${EXP2_ENABLE_2A:-1}"
EXP2_ENABLE_2B="${EXP2_ENABLE_2B:-1}"
EXP2_SHAPES_CSV="${EXP2_SHAPES_CSV:-spine,bushy,fan_out}"
EXP2A_OPS_CSV="${EXP2A_OPS_CSV:-UPDATE,RANGE_UPDATE}"
EXP2B_RANGES_CSV="${EXP2B_RANGES_CSV:-1,10,50,100}"

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

parse_shapes_csv() {
    local csv="${1// /}"
    local parsed=()
    local entry
    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: EXP2_SHAPES_CSV is empty."
        exit 1
    fi
    for entry in "${parsed[@]}"; do
        case "$entry" in
            spine|bushy|fan_out) ;;
            *)
                echo "Error: invalid shape '$entry' in EXP2_SHAPES_CSV."
                exit 1
                ;;
        esac
    done
    EXP2_SHAPE_LIST=("${parsed[@]}")
}

parse_ops_csv() {
    local csv="${1// /}"
    local parsed=()
    local entry
    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: EXP2A_OPS_CSV is empty."
        exit 1
    fi
    for entry in "${parsed[@]}"; do
        case "$entry" in
            UPDATE|RANGE_UPDATE) ;;
            *)
                echo "Error: invalid op '$entry' in EXP2A_OPS_CSV."
                exit 1
                ;;
        esac
    done
    EXP2A_OPS=("${parsed[@]}")
}

parse_ranges_csv() {
    local csv="${1// /}"
    local parsed=()
    local entry
    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: EXP2B_RANGES_CSV is empty."
        exit 1
    fi
    for entry in "${parsed[@]}"; do
        if ! [[ "$entry" =~ ^[0-9]+$ ]]; then
            echo "Error: invalid range '$entry' in EXP2B_RANGES_CSV."
            exit 1
        fi
        if [ "$entry" -le 0 ]; then
            echo "Error: range must be > 0, got '$entry'."
            exit 1
        fi
    done
    EXP2B_RANGE_LIST=("${parsed[@]}")
}

if [ -n "$NUM_BRANCHES_CSV" ]; then
    parse_num_branches_csv "$NUM_BRANCHES_CSV"
fi

echo "============================================="
echo "Experiment 2: Per-Operation Storage Overhead"
echo "============================================="

if [ "$EXP2_ENABLE_2A" != "0" ] && [ "$EXP2_ENABLE_2A" != "1" ]; then
    echo "Error: EXP2_ENABLE_2A must be 0 or 1."
    exit 1
fi
if [ "$EXP2_ENABLE_2B" != "0" ] && [ "$EXP2_ENABLE_2B" != "1" ]; then
    echo "Error: EXP2_ENABLE_2B must be 0 or 1."
    exit 1
fi
if [ "$EXP2_ENABLE_2A" = "0" ] && [ "$EXP2_ENABLE_2B" = "0" ]; then
    echo "Error: both EXP2_ENABLE_2A and EXP2_ENABLE_2B are 0."
    exit 1
fi

if [ "$EXP2_ENABLE_2A" = "1" ]; then
    parse_shapes_csv "$EXP2_SHAPES_CSV"
    parse_ops_csv "$EXP2A_OPS_CSV"
fi
if [ "$EXP2_ENABLE_2B" = "1" ]; then
    parse_ranges_csv "$EXP2B_RANGES_CSV"
fi

if [ "$EXP2_ENABLE_2A" = "1" ]; then
    echo ""
    echo "--- Exp 2a: UPDATE + RANGE_UPDATE ---"

    for SHAPE in "${EXP2_SHAPE_LIST[@]}"; do
        # if [ "$RUN_DOLT" = "1" ]; then
        #     run_branch_sweep dolt "$SQL_DUMP" "$SHAPE" "$SEED" 1024 true UPDATE RANGE_UPDATE
        # fi
        # if [ "$RUN_FILE_COPY" = "1" ]; then
        #     run_branch_sweep file_copy "$SQL_DUMP" "$SHAPE" "$SEED" 1024 true UPDATE RANGE_UPDATE
        # fi
        # if [ "$RUN_NEON" = "1" ]; then
        #     run_branch_sweep neon "$SQL_DUMP" "$SHAPE" "$SEED" "$NEON_MAX_BRANCHES" true UPDATE RANGE_UPDATE
        # fi
        if [ "$RUN_XATA" = "1" ]; then
            run_branch_sweep xata "$SQL_DUMP" "$SHAPE" "$SEED" "$XATA_MAX_BRANCHES" true "${EXP2A_OPS[@]}"
        fi
    done
fi

if [ "$EXP2_ENABLE_2B" = "1" ]; then
    # --- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---
    echo ""
    echo "--- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---"

    for RANGE in "${EXP2B_RANGE_LIST[@]}"; do
        echo "--- range_size=$RANGE ---"
        RANGE_SIZE=$RANGE
        # if [ "$RUN_DOLT" = "1" ]; then
        #     run_branch_sweep dolt "$SQL_DUMP" spine "$SEED" 1024 true RANGE_UPDATE
        # fi
        # if [ "$RUN_FILE_COPY" = "1" ]; then
        #     run_branch_sweep file_copy "$SQL_DUMP" spine "$SEED" 1024 true RANGE_UPDATE
        # fi
        # if [ "$RUN_NEON" = "1" ]; then
        #     run_branch_sweep neon "$SQL_DUMP" spine "$SEED" "$NEON_MAX_BRANCHES" true RANGE_UPDATE
        # fi
        if [ "$RUN_XATA" = "1" ]; then
            run_branch_sweep xata "$SQL_DUMP" spine "$SEED" "$XATA_MAX_BRANCHES" true RANGE_UPDATE
        fi
    done
fi

echo ""
echo "============================================="
echo "Experiment 2 complete."
echo "Results in ${RUN_STATS_DIR:-/tmp/run_stats}/"
echo "============================================="

# Teardown (stop PG, detach volume)
# "$REPO_ROOT/db_setup/teardown_pg_volume.sh"
