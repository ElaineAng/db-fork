#!/bin/bash
# Experiment 1: Branch Creation Storage Overhead (Varying Shape)
# Date: 2026-02-08
#
# Measures marginal storage cost per branch creation across topologies.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/bench_lib.sh"

load_env
eval "$("$REPO_ROOT/db_setup/setup_pg_volume.sh")"

SEED=42
SQL_DUMP="$REPO_ROOT/db_setup/tpcc_schema.sql"

echo "============================================="
echo "Experiment 1: Branch Creation Storage (Shapes)"
echo "============================================="

# --- Dolt (up to 512 branches, 3 shapes) ---

for SHAPE in spine bushy fan_out; do
    run_branch_sweep dolt "$SQL_DUMP" "$SHAPE" "$SEED" 512 true BRANCH
done

# --- PostgreSQL CoW / file_copy (up to 512 branches, 3 shapes) ---

for SHAPE in spine bushy fan_out; do
    run_branch_sweep file_copy "$SQL_DUMP" "$SHAPE" "$SEED" 512 true BRANCH
done

# --- Neon (capped at 8 branches) ---
# We've already done this.

#for SHAPE in bushy fan_out; do
#    run_branch_sweep neon "$SQL_DUMP" "$SHAPE" "$SEED" 8 true BRANCH
#done

echo ""
echo "Experiment 1 complete."
echo "Results in /tmp/run_stats/"

# Teardown (stop PG, detach volume)
"$REPO_ROOT/db_setup/teardown_pg_volume.sh"
