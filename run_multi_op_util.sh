#!/bin/bash
# run_multi_op_util.sh - Automate multiple runs of microbench/runner.py for multi-op benchmarks
#
# Usage: ./run_multi_op_util.sh <backend> <sql_dump_path>
# Example: ./run_multi_op_util.sh DOLT db_setup/tpcc_schema.sql
#          ./run_multi_op_util.sh NEON db_setup/tpcc_schema.sql

set -e

if [ $# -ne 2 ]; then
    echo "Usage: $0 <backend> <sql_dump_path>"
    echo "  backend: dolt, neon"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    exit 1
fi

BACKEND=$1
SQL_DUMP_PATH=$2

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
# NUM_BRANCHES_LIST=(4 8 16 32 128 256 512 1024)
NUM_BRANCHES_LIST=(4 8 16)
OPERATIONS=(CONNECT READ RANGE_UPDATE)

# Other fixed config values
TABLE_NAME="orders"
DB_NAME="microbench"
INSERTS_PER_BRANCH=50
NUM_OPS=1000  # 1000 operations per run for multi-op benchmark

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/multi_op_config.XXXXXX.textproto)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

echo "==================================================="
echo "Multi-Op Benchmark Automation Script"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Branches: ${NUM_BRANCHES_LIST[*]}"
echo "Num Ops per run: $NUM_OPS"
echo "==================================================="

# Loop through all combinations
for NUM_BRANCHES in "${NUM_BRANCHES_LIST[@]}"; do
    for OPERATION in "${OPERATIONS[@]}"; do
        RUN_ID="benchmark_${BACKEND}_multiop_${NUM_BRANCHES}_spine"
        
        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID with operation $OPERATION"
        echo "---------------------------------------------------"
        
        # Generate config file
        cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for multi-op benchmark
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
        python -m microbench.runner --config "$TEMP_CONFIG"
        
        echo "Completed: $RUN_ID with operation $OPERATION"
    done
done

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in /tmp/run_stats/"
echo "==================================================="
