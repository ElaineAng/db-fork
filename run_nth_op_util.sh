#!/bin/bash
# run_nth_op_util.sh - Automate multiple runs of microbench/runner.py
#
# Usage: ./run_nth_op_util.sh <backend> <sql_dump_path>
# Example: ./run_nth_op_util.sh DOLT db_setup/tpcc_schema.sql
#          ./run_nth_op_util.sh NEON db_setup/tpcc_schema.sql

set -e

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <backend> <sql_dump_path> [max_branches]"
    echo "  backend: dolt, neon, kpg, xata"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  max_branches: (optional) Only run experiments with num_branches <= this value"
    exit 1
fi

BACKEND=$1
SQL_DUMP_PATH=$2
MAX_BRANCHES=${3:-9999}  # Default to large number if not specified

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, neon, kpg, xata"
    exit 1
fi

# Check if SQL dump file exists
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Configuration parameters
NUM_BRANCHES_LIST=(1 2 4 8 16 32 128 256 512 1024)
OPERATIONS=(BRANCH READ UPDATE RANGE_UPDATE)

# Other fixed config values
TABLE_NAME="orders"
DB_NAME="microbench"
INSERTS_PER_BRANCH=50
NUM_OPS=1  # Single operation per run (will be repeated across iterations)

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_nth_op_config.XXXXXX.textproto)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Extract first 4 chars of sql_dump filename for run_id
SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
SQL_PREFIX=${SQL_BASENAME:0:4}

# Generate a random seed for reproducibility across all runs (limited to 2^31-1)
RANDOM_SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))

echo "==================================================="
echo "Nth-Op Benchmark Automation Script"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH (prefix: $SQL_PREFIX)"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Branches: ${NUM_BRANCHES_LIST[*]}"
echo "Random Seed: $RANDOM_SEED"
echo "==================================================="

# Loop through all combinations
for NUM_BRANCHES in "${NUM_BRANCHES_LIST[@]}"; do
    # Skip if exceeds max_branches
    if [ "$NUM_BRANCHES" -gt "$MAX_BRANCHES" ]; then
        echo "Skipping num_branches=$NUM_BRANCHES (exceeds max_branches=$MAX_BRANCHES)"
        continue
    fi
    for OPERATION in "${OPERATIONS[@]}"; do
        RUN_ID="${BACKEND}_${SQL_PREFIX}_nth_op_${NUM_BRANCHES}_spine"
        
        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID with operation $OPERATION"
        echo "---------------------------------------------------"
        
        # Generate config file
        cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for nth-op benchmark
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
num_threads: 1

nth_op_benchmark {
  operation: ${OPERATION}
  num_ops: ${NUM_OPS}
  setup {
    num_branches: ${NUM_BRANCHES}
    branch_shape: SPINE
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
