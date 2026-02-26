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
    echo "Usage: $0 <backend> <sql_dump_path> [--seed <seed>] [--max-branches <max>] [--shape <shape>] "
    echo "  backend: dolt, neon, kpg, xata, file_copy, postgres transactions (txn)"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  --seed: (optional) Random seed for reproducibility. If not provided, a random one is generated."
    echo "  --max-branches: (optional) Maximum number of branches to test (default: 1024)"
    echo "  --shape: (optional) Branch tree shape: spine, bushy, or fan_out (default: spine)"
    exit 1
fi

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|FILE_COPY|TXN)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, neon, kpg, xata, file_copy, txn"
    exit 1
fi

# Convert shape to uppercase for proto config and validate
SHAPE_UPPER=$(echo "$SHAPE" | tr '[:lower:]' '[:upper:]')
if [[ ! "$SHAPE_UPPER" =~ ^(SPINE|BUSHY|FAN_OUT)$ ]]; then
    echo "Error: Invalid shape '$SHAPE'. Must be one of: spine, bushy, fan_out"
    exit 1
fi

if [[ "$BACKEND" == "TXN" && "$SHAPE_UPPER" != "SPINE" ]]; then
    echo "Error: PostgreSQL Save Point only works with spine shape"
    exit 1
fi

# Check if SQL dump file exists
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    echo "  --measure-storage: (optional) Measure disk_size_before/after around each update op (reduces num_ops)"
    echo "  --operations: (optional) Comma-separated list of operations (default: BRANCH). E.g. UPDATE,RANGE_UPDATE"
    echo "  --range-size: (optional) Range size for RANGE_UPDATE operation (default: 20)"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Configuration parameters
NUM_BRANCHES_LIST=(1 2 4 8 16 32 64 128 256 512 1024)
OPERATIONS=(BRANCH READ CONNECT INSERT UPDATE RANGE_READ RANGE_UPDATE)
if [[ "$BACKEND" == "TXN" ]]; then 
    OPERATIONS=(BRANCH READ INSERT UPDATE RANGE_READ RANGE_UPDATE CONNECT_FIRST CONNECT_MID CONNECT_LAST)
fi

# Override OPERATIONS if --operations was provided
if [ -n "$OPS_STRING" ]; then
    IFS=',' read -ra OPERATIONS <<< "$OPS_STRING"
fi

# Other fixed config values
TABLE_NAME="orders"
DB_NAME="microbench"
INSERTS_PER_BRANCH=100
UPDATES_PER_BRANCH=20
DELETES_PER_BRANCH=10
RANGE_SIZE=20

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_bench_config.XXXXXX.textproto)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Extract first 4 chars of sql_dump filename for run_id
SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
SQL_PREFIX=${SQL_BASENAME:0:4}

# Function to get num_ops based on operation type
get_num_ops() {
    local op=$1
    case $op in
        BRANCH|CONNECT_FIRST|CONNECT_MID|CONNECT_LAST)
            echo 1
            ;;
        RANGE_UPDATE)
            echo 200
            ;;
        CONNECT|READ|UPDATE|RANGE_READ)
            echo 1000
            ;;
        *)
            echo 1000
            ;;
    esac
}


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
