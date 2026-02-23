#!/bin/bash
# Experiment 3: throughput matrix with non-blocking per-point execution.

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/bench_lib.sh"

SEED="${SEED:-42}"
DURATION_SECONDS="${DURATION_SECONDS:-30}"
SQL_DUMP="${SQL_DUMP:-$REPO_ROOT/db_setup/tpcc_schema.sql}"
ENABLE_NEON="${ENABLE_NEON:-0}"
RUN_STATS_DIR="${RUN_STATS_DIR:-/tmp/run_stats}"
FRESH_PG_VOLUME="${FRESH_PG_VOLUME:-1}"
PG_VOLUME_PORT="${PG_VOLUME_PORT:-5432}"
RUN_DOLT="${RUN_DOLT:-1}"
RUN_FILE_COPY="${RUN_FILE_COPY:-1}"
RUN_NEON="${RUN_NEON:-$ENABLE_NEON}"
RESULTS_ROOT="$SCRIPT_DIR/results"
DATA_DIR="$RESULTS_ROOT/data"
LOG_DIR="$RESULTS_ROOT/logs"
MANIFEST_PATH="$RESULTS_ROOT/run_manifest.csv"
MANIFEST_MODE="${MANIFEST_MODE:-overwrite}"
RUNNER_TIMEOUT_SECONDS="${RUNNER_TIMEOUT_SECONDS:-1200}"

THREAD_LIST_DEFAULT_CSV="${THREAD_LIST_DEFAULT_CSV:-1,2,4,8,16,32,64,128,256,512,1024}"
THREAD_LIST_DOLT_CSV="${THREAD_LIST_DOLT_CSV:-$THREAD_LIST_DEFAULT_CSV}"
THREAD_LIST_FILE_COPY_CSV="${THREAD_LIST_FILE_COPY_CSV:-$THREAD_LIST_DEFAULT_CSV}"
THREAD_LIST_NEON_CSV="${THREAD_LIST_NEON_CSV:-1,2,4,8,16}"
SHAPES_CSV="${SHAPES_CSV:-spine,bushy,fan_out}"
MODES_CSV="${MODES_CSV:-branch,crud}"
CRUD_OPS_CSV="READ,UPDATE,RANGE_READ,RANGE_UPDATE"
SLOW_LATENCY_MULTIPLIER="10.0"

export RUN_STATS_DIR

if ! command -v python3 >/dev/null 2>&1; then
    echo "Error: python3 not found in PATH."
    exit 1
fi

if [ ! -f "$SQL_DUMP" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP"
    exit 1
fi

if ! [[ "$RUNNER_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "Error: RUNNER_TIMEOUT_SECONDS must be a non-negative integer."
    exit 1
fi

mkdir -p "$RUN_STATS_DIR" "$DATA_DIR" "$LOG_DIR"

SQL_BASENAME="$(basename "$SQL_DUMP" .sql)"
SQL_PREFIX="${SQL_BASENAME:0:4}"

parse_thread_list() {
    local csv_raw="$1"
    local csv="${csv_raw// /}"
    local parsed=()
    local entry

    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: empty thread list: $csv_raw" >&2
        exit 1
    fi

    for entry in "${parsed[@]}"; do
        if ! [[ "$entry" =~ ^[0-9]+$ ]]; then
            echo "Error: invalid thread count '$entry' in '$csv_raw'" >&2
            exit 1
        fi
        if [ "$entry" -le 0 ]; then
            echo "Error: thread count must be > 0, got '$entry'" >&2
            exit 1
        fi
    done

    echo "${parsed[@]}"
}

parse_enum_list() {
    local csv_raw="$1"
    local allowed_regex="$2"
    local csv="${csv_raw// /}"
    local parsed=()
    local entry

    IFS=',' read -r -a parsed <<< "$csv"
    if [ "${#parsed[@]}" -eq 0 ] || [ -z "${parsed[0]}" ]; then
        echo "Error: empty list: $csv_raw" >&2
        exit 1
    fi

    for entry in "${parsed[@]}"; do
        if ! [[ "$entry" =~ $allowed_regex ]]; then
            echo "Error: invalid value '$entry' in '$csv_raw'" >&2
            exit 1
        fi
    done

    echo "${parsed[@]}"
}

THREAD_LIST_DOLT=($(parse_thread_list "$THREAD_LIST_DOLT_CSV"))
THREAD_LIST_FILE_COPY=($(parse_thread_list "$THREAD_LIST_FILE_COPY_CSV"))
THREAD_LIST_NEON=($(parse_thread_list "$THREAD_LIST_NEON_CSV"))
SHAPE_LIST=($(parse_enum_list "$SHAPES_CSV" "^(spine|bushy|fan_out)$"))
MODE_LIST=($(parse_enum_list "$MODES_CSV" "^(branch|crud)$"))

write_manifest_header() {
    if [ "$MANIFEST_MODE" = "append" ] && [ -f "$MANIFEST_PATH" ]; then
        return
    fi
    cat > "$MANIFEST_PATH" <<'CSV'
run_id,backend,shape,mode,threads,runner_exit_code,parquet_present,setup_parquet_present,summary_present,status,attempted_ops,successful_ops,failed_exception_ops,failed_slow_ops,success_rate,top_failure_category,top_failure_reason
CSV
}

build_run_id() {
    local backend="$1"
    local shape="$2"
    local threads="$3"
    local mode="$4"
    echo "exp3_${backend}_${shape}_${threads}t_${mode}_${SQL_PREFIX}"
}

collect_run_artifacts() {
    local run_id="$1"
    local filename
    for filename in "${run_id}.parquet" "${run_id}_setup.parquet" "${run_id}_summary.json"; do
        if [ -f "$RUN_STATS_DIR/$filename" ]; then
            mv -f "$RUN_STATS_DIR/$filename" "$DATA_DIR/$filename"
        fi
    done
}

append_manifest_row() {
    local run_id="$1"
    local backend="$2"
    local shape="$3"
    local mode="$4"
    local threads="$5"
    local exit_code="$6"
    local parquet_present="$7"
    local setup_present="$8"
    local summary_present="$9"
    local summary_path="${10}"

    python3 - "$MANIFEST_PATH" "$run_id" "$backend" "$shape" "$mode" "$threads" "$exit_code" "$parquet_present" "$setup_present" "$summary_present" "$summary_path" <<'PY'
import csv
import json
import os
import sys

(
    manifest_path,
    run_id,
    backend,
    shape,
    mode,
    threads,
    exit_code,
    parquet_present,
    setup_present,
    summary_present,
    summary_path,
) = sys.argv[1:]

summary = {}
if summary_present == "1" and os.path.exists(summary_path):
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)
    except Exception:
        summary = {}

attempted_ops = int(summary.get("attempted_ops", 0) or 0)
successful_ops = int(summary.get("successful_ops", 0) or 0)
failed_exception_ops = int(summary.get("failed_exception_ops", 0) or 0)
failed_slow_ops = int(summary.get("failed_slow_ops", 0) or 0)
success_rate = float(summary.get("success_rate", 0.0) or 0.0)
top_failure_category = str(summary.get("top_failure_category", "") or "")
top_failure_reason = str(summary.get("top_failure_reason", "") or "")

if summary_present != "1":
    status = "MISSING"
elif parquet_present == "1" and failed_exception_ops == 0 and failed_slow_ops == 0:
    status = "SUCCESS"
elif attempted_ops > 0 and (failed_exception_ops > 0 or failed_slow_ops > 0):
    status = "PARTIAL"
elif attempted_ops == 0 or (int(exit_code) != 0 and successful_ops == 0):
    status = "FAILED"
else:
    status = "FAILED"

with open(manifest_path, "a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            run_id,
            backend,
            shape,
            mode,
            int(threads),
            int(exit_code),
            int(parquet_present),
            int(setup_present),
            int(summary_present),
            status,
            attempted_ops,
            successful_ops,
            failed_exception_ops,
            failed_slow_ops,
            success_rate,
            top_failure_category,
            top_failure_reason,
        ]
    )
PY
}

run_runner_with_timeout() {
    local temp_config="$1"
    local log_file="$2"

    if [ "$RUNNER_TIMEOUT_SECONDS" -eq 0 ]; then
        python3 -m microbench.runner --config "$temp_config" --seed "$SEED" --no-progress >"$log_file" 2>&1
        return $?
    fi

    python3 -m microbench.runner --config "$temp_config" --seed "$SEED" --no-progress >"$log_file" 2>&1 &
    local runner_pid=$!
    local waited=0

    while kill -0 "$runner_pid" 2>/dev/null; do
        sleep 5
        waited=$((waited + 5))
        if [ "$waited" -ge "$RUNNER_TIMEOUT_SECONDS" ]; then
            echo "Runner timeout after ${RUNNER_TIMEOUT_SECONDS}s; terminating run point." >>"$log_file"
            kill "$runner_pid" 2>/dev/null || true
            sleep 5
            kill -9 "$runner_pid" 2>/dev/null || true
            wait "$runner_pid" 2>/dev/null || true
            return 124
        fi
    done

    wait "$runner_pid"
    return $?
}

run_point() {
    local backend="$1"
    local shape="$2"
    local mode="$3"
    local threads="$4"
    local ops_csv="$5"

    local run_id
    run_id="$(build_run_id "$backend" "$shape" "$threads" "$mode")"

    local backend_upper
    backend_upper="$(echo "$backend" | tr '[:lower:]' '[:upper:]')"
    local shape_upper
    shape_upper="$(echo "$shape" | tr '[:lower:]' '[:upper:]')"

    local setup_branches=0
    if [ "$mode" = "crud" ]; then
        setup_branches="$threads"
    fi

    local temp_config
    temp_config="$(mktemp "/tmp/exp3_${backend}_${shape}_${threads}_${mode}.XXXXXX")"

    generate_throughput_textproto \
        "$temp_config" \
        "$backend_upper" \
        "$shape_upper" \
        "$threads" \
        "$DURATION_SECONDS" \
        "$SQL_DUMP" \
        "$run_id" \
        "$ops_csv" \
        "$setup_branches" \
        "$SLOW_LATENCY_MULTIPLIER"

    rm -f \
        "$RUN_STATS_DIR/${run_id}.parquet" \
        "$RUN_STATS_DIR/${run_id}_setup.parquet" \
        "$RUN_STATS_DIR/${run_id}_summary.json" \
        "$DATA_DIR/${run_id}.parquet" \
        "$DATA_DIR/${run_id}_setup.parquet" \
        "$DATA_DIR/${run_id}_summary.json"

    local log_file="$LOG_DIR/${run_id}.log"
    echo "Running: $run_id"
    run_runner_with_timeout "$temp_config" "$log_file"
    local exit_code=$?

    collect_run_artifacts "$run_id"

    local parquet_present=0
    local setup_present=0
    local summary_present=0

    [ -f "$DATA_DIR/${run_id}.parquet" ] && parquet_present=1
    [ -f "$DATA_DIR/${run_id}_setup.parquet" ] && setup_present=1
    [ -f "$DATA_DIR/${run_id}_summary.json" ] && summary_present=1

    if [ "$exit_code" -eq 124 ] && [ "$summary_present" -eq 0 ]; then
        python3 - "$DATA_DIR/${run_id}_summary.json" "$RUNNER_TIMEOUT_SECONDS" <<'PY'
import json
import sys

summary_path = sys.argv[1]
timeout_seconds = int(sys.argv[2])

summary = {
    "attempted_ops": 0,
    "successful_ops": 0,
    "failed_exception_ops": 0,
    "failed_slow_ops": 0,
    "success_rate": 0.0,
    "top_failure_category": "FAILURE_TIMEOUT",
    "top_failure_reason": f"Runner timed out after {timeout_seconds}s",
}

with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(summary, f)
PY
        summary_present=1
    fi

    append_manifest_row \
        "$run_id" \
        "$backend" \
        "$shape" \
        "$mode" \
        "$threads" \
        "$exit_code" \
        "$parquet_present" \
        "$setup_present" \
        "$summary_present" \
        "$DATA_DIR/${run_id}_summary.json"

    rm -f "$temp_config"
    rm -rf "${DOLT_DATA_DIR:-/tmp/doltgres_data/databases}/.dolt_dropped_databases"/* || true
}

run_backend_matrix() {
    local backend="$1"
    shift
    local thread_list=("$@")

    local shape
    local mode
    local threads

    for shape in "${SHAPE_LIST[@]}"; do
        for mode in "${MODE_LIST[@]}"; do
            local ops_csv="BRANCH"
            if [ "$mode" = "crud" ]; then
                ops_csv="$CRUD_OPS_CSV"
            fi

            for threads in "${thread_list[@]}"; do
                run_point "$backend" "$shape" "$mode" "$threads" "$ops_csv"
            done
        done
    done
}

load_env
if [ "$FRESH_PG_VOLUME" = "1" ]; then
    echo "Preparing fresh PostgreSQL volume state (--delete)."
    "$REPO_ROOT/db_setup/teardown_pg_volume.sh" --delete >/dev/null 2>&1 || true
fi
SETUP_ENV_FILE="$(mktemp)"
if ! "$REPO_ROOT/db_setup/setup_pg_volume.sh" --port "$PG_VOLUME_PORT" >"$SETUP_ENV_FILE"; then
    rm -f "$SETUP_ENV_FILE"
    echo "Error: failed to set up pg volume."
    exit 1
fi
eval "$(<"$SETUP_ENV_FILE")"
rm -f "$SETUP_ENV_FILE"
trap "$REPO_ROOT/db_setup/teardown_pg_volume.sh" EXIT

write_manifest_header

echo "============================================="
echo "Experiment 3: Operation Throughput Under Branching"
echo "============================================="
echo "seed=$SEED duration=${DURATION_SECONDS}s"
echo "sql_dump=$SQL_DUMP"
echo "run_stats_dir=$RUN_STATS_DIR"
echo "fresh_pg_volume=$FRESH_PG_VOLUME"
echo "pg_volume_port=$PG_VOLUME_PORT"
echo "run_dolt=$RUN_DOLT run_file_copy=$RUN_FILE_COPY run_neon=$RUN_NEON"
echo "manifest_mode=$MANIFEST_MODE"
echo "runner_timeout_seconds=$RUNNER_TIMEOUT_SECONDS"
echo "threads_dolt=${THREAD_LIST_DOLT[*]}"
echo "threads_file_copy=${THREAD_LIST_FILE_COPY[*]}"
echo "threads_neon=${THREAD_LIST_NEON[*]}"
echo "shapes=${SHAPE_LIST[*]}"
echo "modes=${MODE_LIST[*]}"
echo "results_dir=$DATA_DIR"

if [ "$RUN_DOLT" = "1" ]; then
    run_backend_matrix "dolt" "${THREAD_LIST_DOLT[@]}"
fi

if [ "$RUN_FILE_COPY" = "1" ]; then
    run_backend_matrix "file_copy" "${THREAD_LIST_FILE_COPY[@]}"
fi

if [ "$RUN_NEON" = "1" ]; then
    run_backend_matrix "neon" "${THREAD_LIST_NEON[@]}"
else
    echo "Neon skipped (set RUN_NEON=1 or ENABLE_NEON=1 to run Neon)."
fi

echo "============================================="
echo "Experiment 3 complete."
echo "Data: $DATA_DIR"
echo "Logs: $LOG_DIR"
echo "Manifest: $MANIFEST_PATH"
echo "============================================="
