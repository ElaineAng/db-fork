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
    for SHAPE in spine bushy fan_out; do
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
