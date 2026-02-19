#!/bin/bash
# Experiment 3: Operation Throughput Under Branching
# Date: 2026-02-19
#
# Exp 3a: Branch creation throughput (T concurrent threads, 30s)
# Exp 3b: CRUD throughput under branching (N branches, N threads, 30s)
#
# Backends: Dolt, file_copy (Neon optional via ENABLE_NEON=1).

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/bench_lib.sh"

SEED="${SEED:-42}"
DURATION_SECONDS="${DURATION_SECONDS:-30}"
SQL_DUMP="${SQL_DUMP:-$REPO_ROOT/db_setup/tpcc_schema.sql}"
ENABLE_NEON="${ENABLE_NEON:-0}"   # Set to 1 to include Neon
THREAD_LIST=(1 2 4 8 16 32 64 128)
THREAD_LIST_NEON=(1 2 4 8)
CRUD_OPS_CSV="READ,UPDATE,RANGE_READ,RANGE_UPDATE"

if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 not found in PATH."
    exit 1
fi

if [ ! -f "$SQL_DUMP" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP"
    exit 1
fi

run_throughput_mode() {
    local backend="$1"
    local shape="$2"
    local exp_mode="$3"        # branch | crud
    local ops_csv="$4"
    local max_threads="$5"
    shift 5
    local thread_list=("$@")

    local backend_upper
    backend_upper=$(echo "$backend" | tr '[:lower:]' '[:upper:]')
    local shape_upper
    shape_upper=$(echo "$shape" | tr '[:lower:]' '[:upper:]')

    local sql_basename
    sql_basename=$(basename "$SQL_DUMP" .sql)
    local sql_prefix="${sql_basename:0:4}"

    local temp_config
    temp_config=$(mktemp "/tmp/${backend}_${exp_mode}_throughput.XXXXXX.textproto")

    for num_threads in "${thread_list[@]}"; do
        if [ "$num_threads" -gt "$max_threads" ]; then
            continue
        fi

        local setup_branches=0
        if [ "$exp_mode" = "crud" ]; then
            setup_branches="$num_threads"
        fi

        local run_id="exp3_${backend}_${shape}_${num_threads}t_${exp_mode}_${sql_prefix}"
        echo "Running: $run_id"

        generate_throughput_textproto \
            "$temp_config" \
            "$backend_upper" \
            "$shape_upper" \
            "$num_threads" \
            "$DURATION_SECONDS" \
            "$SQL_DUMP" \
            "$run_id" \
            "$ops_csv" \
            "$setup_branches"

        python3 -m microbench.runner --config "$temp_config" --seed "$SEED" --no-progress

        rm -rf "${DOLT_DATA_DIR:-/tmp/doltgres_data/databases}/.dolt_dropped_databases"/* || true
    done

    rm -f "$temp_config"
}

load_env
eval "$("$REPO_ROOT/db_setup/setup_pg_volume.sh")"
trap "$REPO_ROOT/db_setup/teardown_pg_volume.sh" EXIT

echo "============================================="
echo "Experiment 3: Operation Throughput Under Branching"
echo "============================================="
echo "seed=$SEED duration=${DURATION_SECONDS}s"
echo "sql_dump=$SQL_DUMP"

for SHAPE in spine bushy fan_out; do
    echo "--- Dolt / shape=$SHAPE ---"
    run_throughput_mode dolt "$SHAPE" branch "BRANCH" 128 "${THREAD_LIST[@]}"
    run_throughput_mode dolt "$SHAPE" crud "$CRUD_OPS_CSV" 128 "${THREAD_LIST[@]}"
done

for SHAPE in spine bushy fan_out; do
    echo "--- file_copy / shape=$SHAPE ---"
    run_throughput_mode file_copy "$SHAPE" branch "BRANCH" 128 "${THREAD_LIST[@]}"
    run_throughput_mode file_copy "$SHAPE" crud "$CRUD_OPS_CSV" 128 "${THREAD_LIST[@]}"
done

if [ "$ENABLE_NEON" = "1" ]; then
    for SHAPE in spine bushy fan_out; do
        echo "--- Neon / shape=$SHAPE ---"
        run_throughput_mode neon "$SHAPE" branch "BRANCH" 8 "${THREAD_LIST_NEON[@]}"
        run_throughput_mode neon "$SHAPE" crud "$CRUD_OPS_CSV" 8 "${THREAD_LIST_NEON[@]}"
    done
else
    echo "Neon skipped (set ENABLE_NEON=1 to run Neon with threads 1,2,4,8)."
fi

echo "============================================="
echo "Experiment 3 complete."
echo "Results in /tmp/run_stats/"
echo "============================================="
