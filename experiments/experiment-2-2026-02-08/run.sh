#!/bin/bash
# Experiment 2: Per-Operation Storage Overhead
# Date: 2026-02-08
#
# Exp 2a: UPDATE + RANGE_UPDATE (fixed range_size=20, all topologies)
# Exp 2b: RANGE_UPDATE with varying range_size (spine only)
#
# Backends: Dolt, file_copy only.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/bench_lib.sh"

load_env
eval "$("$REPO_ROOT/db_setup/setup_pg_volume.sh")"

SEED=42
SQL_DUMP="$REPO_ROOT/db_setup/tpcc_schema.sql"

echo "============================================="
echo "Experiment 2: Per-Operation Storage Overhead"
echo "============================================="

# --- Exp 2a: UPDATE + RANGE_UPDATE (range_size=20, all shapes) ---

echo ""
echo "--- Exp 2a: UPDATE + RANGE_UPDATE (all shapes) ---"

for SHAPE in spine bushy fan_out; do
    run_branch_sweep dolt      "$SQL_DUMP" "$SHAPE" "$SEED" 512 true UPDATE RANGE_UPDATE
    run_branch_sweep file_copy "$SQL_DUMP" "$SHAPE" "$SEED" 512 true UPDATE RANGE_UPDATE
done

# --- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---

echo ""
echo "--- Exp 2b: RANGE_UPDATE varying range_size (spine only) ---"

for RANGE in 1 10 50 100; do
    echo "--- range_size=$RANGE ---"
    RANGE_SIZE=$RANGE
    run_branch_sweep dolt      "$SQL_DUMP" spine "$SEED" 512 true RANGE_UPDATE
    run_branch_sweep file_copy "$SQL_DUMP" spine "$SEED" 512 true RANGE_UPDATE
done

echo ""
echo "============================================="
echo "Experiment 2 complete."
echo "Results in /tmp/run_stats/"
echo "============================================="

# Teardown (stop PG, detach volume)
"$REPO_ROOT/db_setup/teardown_pg_volume.sh"
