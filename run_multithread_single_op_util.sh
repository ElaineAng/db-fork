#!/bin/bash
# run_multithread_util.sh - Automate multi-threaded benchmark runs
#
# Usage: ./run_multithread_util.sh <backend> <sql_dump_path> [max_threads]
# Example: ./run_multithread_util.sh dolt db_setup/tpcc_schema.sql
#          ./run_multithread_util.sh neon db_setup/tpcc_schema.sql 64

set -e

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <backend> <sql_dump_path> [max_threads]"
    echo "  backend: dolt, neon"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  max_threads: (optional) Only run experiments with num_threads <= this value"
    exit 1
fi

BACKEND=$1
SQL_DUMP_PATH=$2
MAX_THREADS=${3:-9999}  # Default to large number if not specified

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|FILE_COPY)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, neon, kpg, xata, file_copy"
    exit 1
fi

# Check if SQL dump file exists
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Extract first 4 chars of sql_dump filename for run_id
SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
SQL_PREFIX=${SQL_BASENAME:0:4}

# Configuration parameters
# NUM_THREADS_LIST=(1 2 4 8 16 32 64 128 256 512 1024)
NUM_THREADS_LIST=(1024)
OPERATIONS=(BRANCH READ RANGE_UPDATE)

# Other fixed config values
TABLE_NAME="orders"
DB_NAME="microbench"
INSERTS_PER_BRANCH=50
NUM_OPS=1  # Single operation per thread

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_multithread_config.XXXXXX.textproto)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Generate a random seed for reproducibility across all runs (limited to 2^31-1)
RANDOM_SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))

echo "==================================================="
echo "Multi-Threaded Benchmark Automation Script"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH (prefix: $SQL_PREFIX)"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Threads: ${NUM_THREADS_LIST[*]}"
echo "Max Threads: $MAX_THREADS"
echo "Random Seed: $RANDOM_SEED"
echo "==================================================="

# Loop through all combinations
for NUM_THREADS in "${NUM_THREADS_LIST[@]}"; do
    # Skip if exceeds max_threads
    if [ "$NUM_THREADS" -gt "$MAX_THREADS" ]; then
        echo "Skipping num_threads=$NUM_THREADS (exceeds max_threads=$MAX_THREADS)"
        continue
    fi
    
    for OPERATION in "${OPERATIONS[@]}"; do
        RUN_ID="${BACKEND}_${SQL_PREFIX}_multithread_${NUM_THREADS}_fanout"
        
        # Set num_branches based on operation:
        # - BRANCH: 0 (we're measuring branch creation from scratch)
        # - READ/RANGE_UPDATE: same as num_threads (one branch per thread to read from)
        if [ "$OPERATION" = "BRANCH" ]; then
            NUM_BRANCHES=0
        else
            NUM_BRANCHES=$NUM_THREADS
        fi
        
        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID with operation $OPERATION"
        echo "  Threads: $NUM_THREADS, Branches: $NUM_BRANCHES"
        echo "---------------------------------------------------"
        
        # Generate config file
        cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for multi-threaded benchmark
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
  range_size: 20
}

autocommit: true
num_threads: ${NUM_THREADS}

nth_op_benchmark {
  operation: ${OPERATION}
  num_ops: ${NUM_OPS}
  setup {
    num_branches: ${NUM_BRANCHES}
    branch_shape: FAN_OUT
    inserts_per_branch: ${INSERTS_PER_BRANCH}
  }
}
EOF
        
        echo "Config generated at: $TEMP_CONFIG"
        cat "$TEMP_CONFIG"
        echo ""
        
        # Run the benchmark
        echo "Starting benchmark..."
        python -m microbench.runner --config "$TEMP_CONFIG" --seed $RANDOM_SEED
        
        # Clean up dropped databases to prevent disk space explosion
        rm -rf ~/doltgres/databases/.dolt_dropped_databases/*
        
        echo "Completed: $RUN_ID with operation $OPERATION"
    done
done

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in /tmp/run_stats/"
echo "==================================================="
