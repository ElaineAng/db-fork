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

echo "============================================="
echo "Experiment 2: Per-Operation Storage Overhead"
echo "============================================="

# --- Exp 2a: UPDATE + RANGE_UPDATE (range_size=20, all shapes) ---

echo ""
echo "--- Exp 2a: UPDATE + RANGE_UPDATE (all shapes) ---"

for SHAPE in spine bushy fan_out; do
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
        run_branch_sweep xata "$SQL_DUMP" "$SHAPE" "$SEED" "$XATA_MAX_BRANCHES" true UPDATE RANGE_UPDATE
    fi
done

# --- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---

echo ""
echo "--- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---"

for RANGE in 1 10 50 100; do
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

echo ""
echo "============================================="
echo "Experiment 2 complete."
echo "Results in ${RUN_STATS_DIR:-/tmp/run_stats}/"
echo "============================================="

# Teardown (stop PG, detach volume)
# "$REPO_ROOT/db_setup/teardown_pg_volume.sh"
