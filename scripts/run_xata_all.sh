#!/bin/bash
# run_xata_all.sh — Single consolidated 11-worker Xata orchestrator.
#
# Default behavior:
#   bash scripts/run_xata_all.sh
#
# Optional subset via positional args:
#   bash scripts/run_xata_all.sh p01_exp1_spine p09_exp3_spine
#
# Optional env:
#   RUN_TAG=<custom-tag>
#   WORKERS_CSV=<csv-worker-ids>  # overridden by positional args if present

set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_TAG="${RUN_TAG:-$(date +%Y%m%d_%H%M%S)}"
WORKERS_CSV="${WORKERS_CSV:-}"
RUN_ROOT="${REPO_ROOT}/experiments/xata_11proc_runs/${RUN_TAG}"

if [ "$#" -gt 0 ]; then
    workers_csv=""
    for worker in "$@"; do
        if [ -z "$workers_csv" ]; then
            workers_csv="$worker"
        else
            workers_csv="$workers_csv,$worker"
        fi
    done
    WORKERS_CSV="$workers_csv"
fi

if [ -d "$REPO_ROOT/.venv/bin" ]; then
    export PATH="$REPO_ROOT/.venv/bin:$PATH"
fi

if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source "$REPO_ROOT/.env"
    set +a
fi

for idx in $(seq 1 11); do
    key_var="XATA_API_KEY_${idx}"
    org_var="XATA_ORGANIZATION_ID_${idx}"
    : "${!key_var:?Set ${key_var}}"
    : "${!org_var:?Set ${org_var}}"
done

ALL_WORKERS=(
    p01_exp1_spine
    p02_exp1_bushy
    p03_exp1_fan_out
    p04_exp2a_spine
    p05_exp2a_bushy
    p06_exp2a_fan_out
    p07_exp2b_r1_10
    p08_exp2b_r50_100
    p09_exp3_spine
    p10_exp3_bushy
    p11_exp3_fan_out
)

is_valid_worker() {
    local candidate="$1"
    local w
    for w in "${ALL_WORKERS[@]}"; do
        if [ "$candidate" = "$w" ]; then
            return 0
        fi
    done
    return 1
}

SELECTED_WORKERS=()
if [ -n "$WORKERS_CSV" ]; then
    csv="${WORKERS_CSV// /}"
    IFS=',' read -r -a requested <<< "$csv"
    if [ "${#requested[@]}" -eq 0 ] || [ -z "${requested[0]}" ]; then
        echo "Error: WORKERS_CSV is empty."
        exit 1
    fi
    for w in "${requested[@]}"; do
        if ! is_valid_worker "$w"; then
            echo "Error: unknown worker '$w' in WORKERS_CSV."
            exit 1
        fi
        SELECTED_WORKERS+=("$w")
    done
else
    SELECTED_WORKERS=("${ALL_WORKERS[@]}")
fi

mkdir -p \
    "$RUN_ROOT/run_stats" \
    "$RUN_ROOT/data" \
    "$RUN_ROOT/logs" \
    "$RUN_ROOT/status" \
    "$RUN_ROOT/exp3"

ORCH_LOG="$RUN_ROOT/logs/orchestrator.log"
STATUS_CSV="$RUN_ROOT/status/process_status.csv"
echo "worker_id,pid,exit_code,elapsed_seconds,status,log_file" >"$STATUS_CSV"

log() {
    echo "$*" | tee -a "$ORCH_LOG"
}

PIDS=()
WORKER_IDS=()
START_TIMES=()
LOG_FILES=()

register_worker() {
    local pid="$1"
    local worker="$2"
    local log_file="$3"
    PIDS+=("$pid")
    WORKER_IDS+=("$worker")
    START_TIMES+=("$(date +%s)")
    LOG_FILES+=("$log_file")
}

launch_exp1_worker() {
    local worker="$1"
    local cred_idx="$2"
    local shape="$3"
    local run_stats_dir="$RUN_ROOT/run_stats/$worker"
    local data_dir="$RUN_ROOT/data/$worker"
    local log_file="$RUN_ROOT/logs/$worker.log"
    local key_var="XATA_API_KEY_${cred_idx}"
    local org_var="XATA_ORGANIZATION_ID_${cred_idx}"
    local api_key="${!key_var}"
    local org_id="${!org_var}"

    mkdir -p "$run_stats_dir" "$data_dir"
    (
        set -e
        export XATA_API_KEY="$api_key"
        export XATA_ORGANIZATION_ID="$org_id"
        export RUN_STATS_DIR="$run_stats_dir"
        export RUN_XATA=1
        export SHAPES_CSV="$shape"
        export NUM_BRANCHES_CSV="1,2,4,8,16,32,64,128,256,512,1024"
        export XATA_MAX_BRANCHES=16
        bash "$REPO_ROOT/experiments/experiment-1-2026-02-08/run.sh"
        cp -v "$RUN_STATS_DIR"/xata_*.parquet "$data_dir/" 2>/dev/null || true
        cp -v "$RUN_STATS_DIR"/xata_*_summary.json "$data_dir/" 2>/dev/null || true
    ) >"$log_file" 2>&1 &
    register_worker "$!" "$worker" "$log_file"
}

launch_exp2_worker() {
    local worker="$1"
    local cred_idx="$2"
    local enable_2a="$3"
    local enable_2b="$4"
    local shapes_csv="$5"
    local ops_csv="$6"
    local ranges_csv="$7"
    local run_stats_dir="$RUN_ROOT/run_stats/$worker"
    local data_dir="$RUN_ROOT/data/$worker"
    local log_file="$RUN_ROOT/logs/$worker.log"
    local key_var="XATA_API_KEY_${cred_idx}"
    local org_var="XATA_ORGANIZATION_ID_${cred_idx}"
    local api_key="${!key_var}"
    local org_id="${!org_var}"

    mkdir -p "$run_stats_dir" "$data_dir"
    (
        set -e
        export XATA_API_KEY="$api_key"
        export XATA_ORGANIZATION_ID="$org_id"
        export RUN_STATS_DIR="$run_stats_dir"
        export RUN_XATA=1
        export EXP2_SMOKE_ONE_POINT=0
        export EXP2_ENABLE_2A="$enable_2a"
        export EXP2_ENABLE_2B="$enable_2b"
        export EXP2_SHAPES_CSV="$shapes_csv"
        export EXP2A_OPS_CSV="$ops_csv"
        export EXP2B_RANGES_CSV="$ranges_csv"
        export NUM_BRANCHES_CSV="1,2,4,8,16,32,64,128,256,512,1024"
        export XATA_MAX_BRANCHES=16
        bash "$REPO_ROOT/experiments/experiment-2-2026-02-08/run.sh"
        cp -v "$RUN_STATS_DIR"/xata_*.parquet "$data_dir/" 2>/dev/null || true
        cp -v "$RUN_STATS_DIR"/xata_*_summary.json "$data_dir/" 2>/dev/null || true
    ) >"$log_file" 2>&1 &
    register_worker "$!" "$worker" "$log_file"
}

launch_exp3_worker() {
    local worker="$1"
    local cred_idx="$2"
    local shape="$3"
    local run_stats_dir="$RUN_ROOT/run_stats/$worker"
    local exp3_root="$RUN_ROOT/exp3/$worker"
    local data_dir="$exp3_root/data"
    local log_dir="$exp3_root/logs"
    local manifest_path="$exp3_root/run_manifest.csv"
    local log_file="$RUN_ROOT/logs/$worker.log"
    local key_var="XATA_API_KEY_${cred_idx}"
    local org_var="XATA_ORGANIZATION_ID_${cred_idx}"
    local api_key="${!key_var}"
    local org_id="${!org_var}"

    mkdir -p "$run_stats_dir" "$exp3_root" "$data_dir" "$log_dir"
    (
        set -e
        export XATA_API_KEY="$api_key"
        export XATA_ORGANIZATION_ID="$org_id"
        export RUN_STATS_DIR="$run_stats_dir"
        export RESULTS_ROOT="$exp3_root"
        export DATA_DIR="$data_dir"
        export LOG_DIR="$log_dir"
        export MANIFEST_PATH="$manifest_path"
        export SHAPES_CSV="$shape"
        export MODES_CSV=branch,crud
        export THREAD_LIST_XATA_CSV=1,2,4,8,16
        export DURATION_SECONDS=30
        bash "$REPO_ROOT/experiments/experiment-3-throughput/run.sh"
    ) >"$log_file" 2>&1 &
    register_worker "$!" "$worker" "$log_file"
}

launch_worker() {
    local worker="$1"
    case "$worker" in
        p01_exp1_spine) launch_exp1_worker "$worker" 1 spine ;;
        p02_exp1_bushy) launch_exp1_worker "$worker" 2 bushy ;;
        p03_exp1_fan_out) launch_exp1_worker "$worker" 3 fan_out ;;
        p04_exp2a_spine)
            launch_exp2_worker "$worker" 4 1 0 spine UPDATE,RANGE_UPDATE 1,10,50,100
            ;;
        p05_exp2a_bushy)
            launch_exp2_worker "$worker" 5 1 0 bushy UPDATE,RANGE_UPDATE 1,10,50,100
            ;;
        p06_exp2a_fan_out)
            launch_exp2_worker "$worker" 6 1 0 fan_out UPDATE,RANGE_UPDATE 1,10,50,100
            ;;
        p07_exp2b_r1_10)
            launch_exp2_worker "$worker" 7 0 1 spine UPDATE,RANGE_UPDATE 1,10
            ;;
        p08_exp2b_r50_100)
            launch_exp2_worker "$worker" 8 0 1 spine UPDATE,RANGE_UPDATE 50,100
            ;;
        p09_exp3_spine) launch_exp3_worker "$worker" 9 spine ;;
        p10_exp3_bushy) launch_exp3_worker "$worker" 10 bushy ;;
        p11_exp3_fan_out) launch_exp3_worker "$worker" 11 fan_out ;;
        *)
            echo "Error: unhandled worker '$worker'."
            exit 1
            ;;
    esac
}

log "Launching Xata 11-worker run"
log "RUN_ROOT=$RUN_ROOT"
log "WORKERS=${SELECTED_WORKERS[*]}"

for worker in "${SELECTED_WORKERS[@]}"; do
    launch_worker "$worker"
done

log "Started ${#PIDS[@]} worker(s): ${PIDS[*]}"
log "Worker logs: $RUN_ROOT/logs"

FAIL=0
for i in "${!PIDS[@]}"; do
    pid="${PIDS[$i]}"
    worker="${WORKER_IDS[$i]}"
    log_file="${LOG_FILES[$i]}"
    if wait "$pid"; then
        exit_code=0
        status=OK
    else
        exit_code=$?
        status=FAIL
        FAIL=1
    fi
    elapsed=$(( $(date +%s) - ${START_TIMES[$i]} ))
    echo "${worker},${pid},${exit_code},${elapsed},${status},${log_file}" >>"$STATUS_CSV"
    log "[${status}] ${worker} (PID ${pid}) exit=${exit_code} elapsed=${elapsed}s"
done

log "Status CSV: $STATUS_CSV"
log "Run complete. FAIL=$FAIL"

if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
