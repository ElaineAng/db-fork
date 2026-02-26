#!/bin/bash
# NOTE: This file is created after the experiment is performed. So it might be different from actual run experiments.
# Experiment 0: Branch Creation Storage Scaling (Spine Only)
# Date: 2026-02-01
# Status: Done
#
# Reconstructed commands from the original run.
# Used default OPERATIONS=(BRANCH CONNECT READ UPDATE RANGE_READ RANGE_UPDATE)
# and default NUM_BRANCHES_LIST=(1 2 4 8 16 32 64 128 256 512 1024).

set -e

SEED=42
SQL_DUMP=db_setup/tpcc_schema.sql

# Dolt (full range)
./run_single_thread_bench.sh dolt "$SQL_DUMP" --shape spine --seed $SEED

# PostgreSQL CoW / file_copy (full range)
./run_single_thread_bench.sh file_copy "$SQL_DUMP" --shape spine --seed $SEED

# Neon (capped at 8 branches due to plan limit)
./run_single_thread_bench.sh neon "$SQL_DUMP" --shape spine --seed $SEED --max-branches 8

# Xata (capped at 16 branches due to hibernation)
./run_single_thread_bench.sh xata "$SQL_DUMP" --shape spine --seed $SEED --max-branches 16
