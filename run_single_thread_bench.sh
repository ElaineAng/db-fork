#!/bin/bash
# run_single_thread_bench.sh - Single-threaded benchmark script for nth-op measurements
#
# Usage: ./run_single_thread_bench.sh <backend> <sql_dump_path> [--seed <seed>] [--max-branches <max>] [--shape <shape>] [--measure-storage] [--operations <ops>] [--range-size <n>]
# Example: ./run_single_thread_bench.sh DOLT db_setup/tpcc_schema.sql
#          ./run_single_thread_bench.sh NEON db_setup/tpcc_schema.sql --seed 12345 --max-branches 128 --shape bushy
#          ./run_single_thread_bench.sh DOLT data/tpcc.sql --max-branches 2 --measure-storage
#          ./run_single_thread_bench.sh DOLT data/tpcc.sql --operations UPDATE,RANGE_UPDATE --measure-storage
#          ./run_single_thread_bench.sh DOLT data/tpcc.sql --operations RANGE_UPDATE --range-size 50

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/bench_lib.sh"

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
SEED=""
MAX_BRANCHES=1024
SHAPE="spine"
MEASURE_STORAGE=false
OPS_STRING=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --seed)
            SEED="$2"
            shift 2
            ;;
        --max-branches)
            MAX_BRANCHES="$2"
            shift 2
            ;;
        --shape)
            SHAPE="$2"
            shift 2
            ;;
        --measure-storage)
            MEASURE_STORAGE=true
            shift
            ;;
        --operations)
            OPS_STRING="$2"
            shift 2
            ;;
        --range-size)
            RANGE_SIZE="$2"
            shift 2
            ;;
        *)
            if [ -z "$BACKEND" ]; then
                BACKEND="$1"
            elif [ -z "$SQL_DUMP_PATH" ]; then
                SQL_DUMP_PATH="$1"
            else
                echo "Error: Unexpected argument '$1'"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$SQL_DUMP_PATH" ]; then
    echo "Usage: $0 <backend> <sql_dump_path> [--seed <seed>] [--max-branches <max>] [--shape <shape>] [--measure-storage]"
    echo "  backend: dolt, neon, kpg, xata, file_copy"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  --seed: (optional) Random seed for reproducibility. If not provided, a random one is generated."
    echo "  --max-branches: (optional) Maximum number of branches to test (default: 1024)"
    echo "  --shape: (optional) Branch tree shape: spine, bushy, or fan_out (default: spine)"
    echo "  --measure-storage: (optional) Measure disk_size_before/after around each update op (reduces num_ops)"
    echo "  --operations: (optional) Comma-separated list of operations (default: BRANCH). E.g. UPDATE,RANGE_UPDATE"
    echo "  --range-size: (optional) Range size for RANGE_UPDATE operation (default: 20)"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Override OPERATIONS if --operations was provided
if [ -n "$OPS_STRING" ]; then
    IFS=',' read -ra OPERATIONS <<< "$OPS_STRING"
else
    OPERATIONS=(BRANCH CONNECT READ UPDATE RANGE_READ RANGE_UPDATE)
fi

echo "==================================================="
echo "Single-Thread Benchmark Script"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Branches: max=$MAX_BRANCHES"
echo "Branch Shape: $SHAPE"
echo "Random Seed: $SEED"
echo "Measure Storage: $MEASURE_STORAGE"
echo "==================================================="

run_branch_sweep "$BACKEND" "$SQL_DUMP_PATH" "$SHAPE" "$SEED" "$MAX_BRANCHES" "$MEASURE_STORAGE" "${OPERATIONS[@]}"

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in /tmp/run_stats/"
echo "==================================================="
