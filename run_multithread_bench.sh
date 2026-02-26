#!/bin/bash
# run_multithread_bench.sh - Multi-threaded benchmark script for nth-op measurements
#
# Usage: ./run_multithread_bench.sh <backend> <sql_dump_path> [--seed <seed>] [--max-branches <max>] [--shape <shape>]
# Example: ./run_multithread_bench.sh DOLT db_setup/tpcc_schema.sql
#          ./run_multithread_bench.sh NEON db_setup/tpcc_schema.sql --seed 12345 --max-branches 128 --shape bushy
#
# Number of threads always matches number of branches.

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
SEED=""
MAX_BRANCHES=1024
SHAPE="spine"

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
    echo "Usage: $0 <backend> <sql_dump_path> [--seed <seed>] [--max-branches <max>] [--shape <shape>]"
    echo "  backend: dolt, neon, kpg, xata, postgres transaction (txn)"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  --seed: (optional) Random seed for reproducibility. If not provided, a random one is generated."
    echo "  --max-branches: (optional) Maximum number of branches to test (default: 1024)"
    echo "  --shape: (optional) Branch tree shape: spine, bushy, or fan_out (default: spine)"
    echo ""
    echo "Note: Number of threads always matches number of branches."
    exit 1
fi

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|TXN)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, neon, kpg, xata, txn"
    exit 1
fi

# Convert shape to uppercase for proto config and validate
SHAPE_UPPER=$(echo "$SHAPE" | tr '[:lower:]' '[:upper:]')
if [[ ! "$SHAPE_UPPER" =~ ^(SPINE|BUSHY|FAN_OUT)$ ]]; then
    echo "Error: Invalid shape '$SHAPE'. Must be one of: spine, bushy, fan_out"
    exit 1
fi

if [[ "$BACKEND" == "TXN" &&  "$SHAPE_UPPER" != "SPINE" ]]; then
    echo "Error: PostgreSQL Save Point only works with spine shape"
    exit 1
fi

# Check if SQL dump file exists
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Configuration parameters
#NUM_BRANCHES_LIST=(1 2 4 8 16 32 64 128 256 512 1024)
NUM_BRANCHES_LIST=(16 32 64 128)
# NUM_BRANCHES_LIST=(16)
OPERATIONS=(BRANCH CONNECT READ UPDATE RANGE_READ RANGE_UPDATE)
# OPERATIONS=(BRANCH)

# Other fixed config values
TABLE_NAME="orders"
DB_NAME="microbench"
INSERTS_PER_BRANCH=100
UPDATES_PER_BRANCH=20
DELETES_PER_BRANCH=10
RANGE_SIZE=20

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_multithread_bench_config.XXXXXX.textproto)

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
        BRANCH)
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
echo "Multi-Thread Benchmark Script"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH (prefix: $SQL_PREFIX)"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Branches/Threads: ${NUM_BRANCHES_LIST[*]} (max: $MAX_BRANCHES)"
echo "Branch Shape: $SHAPE_UPPER"
echo "Random Seed: $SEED"
echo "==================================================="

# Loop through all combinations
for NUM_BRANCHES in "${NUM_BRANCHES_LIST[@]}"; do
    # Skip if exceeds max_branches
    if [ "$NUM_BRANCHES" -gt "$MAX_BRANCHES" ]; then
        echo "Skipping num_branches=$NUM_BRANCHES (exceeds max_branches=$MAX_BRANCHES)"
        continue
    fi
    
    # Number of threads matches number of branches
    NUM_THREADS=$NUM_BRANCHES
    
    for OPERATION in "${OPERATIONS[@]}"; do
        NUM_OPS=$(get_num_ops "$OPERATION")
        SHAPE_LOWER=$(echo "$SHAPE" | tr '[:upper:]' '[:lower:]')
        RUN_ID="${BACKEND}_${SQL_PREFIX}_multitrd_${NUM_BRANCHES}_${SHAPE_LOWER}"
        
        # For BRANCH operation, num_branches in setup should be 0
        # For all other operations, num_branches matches num_threads
        if [ "$OPERATION" = "BRANCH" ]; then
            SETUP_NUM_BRANCHES=0
        else
            SETUP_NUM_BRANCHES=$NUM_THREADS
        fi
        
        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID"
        echo "  Operation: $OPERATION, Num Ops: $NUM_OPS, Setup Branches: $SETUP_NUM_BRANCHES, Threads: $NUM_THREADS"
        echo "---------------------------------------------------"
        
        # Generate config file
        cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for multi-thread benchmark
run_id: "${RUN_ID}"
backend: ${BACKEND_UPPER}

table_name: "${TABLE_NAME}"
starting_branch: ""

database_setup {
  db_name: "${DB_NAME}"
  cleanup: true
  sql_dump {
    sql_dump_path: "${SQL_DUMP_PATH}"
  }
}

range_update_config {
  range_size: ${RANGE_SIZE}
}

autocommit: true
num_threads: ${NUM_THREADS}

nth_op_benchmark {
  operation: ${OPERATION}
  num_ops: ${NUM_OPS}
  setup {
    num_branches: ${SETUP_NUM_BRANCHES}
    branch_shape: ${SHAPE_UPPER}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }
}
EOF
        
        echo "Config generated at: $TEMP_CONFIG"
        cat "$TEMP_CONFIG"
        echo ""
        
        # Run the benchmark
        echo "Starting benchmark..."
        python -m microbench.runner --config "$TEMP_CONFIG" --seed $SEED --no-progress
        
        # Clean up dropped databases to prevent disk space explosion
        rm -rf "${DOLT_DATA_DIR:-/tmp/doltgres_data/databases}/.dolt_dropped_databases"/*
        
        echo "Completed: $RUN_ID"
    done
done

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in /tmp/run_stats/"
echo "==================================================="
