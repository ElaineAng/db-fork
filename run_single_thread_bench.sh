#!/bin/bash
# run_single_thread_bench.sh - Single-threaded benchmark script for nth-op measurements
#
# Usage: ./run_single_thread_bench.sh <backend> <sql_dump_path> <num_branches> [--seed <seed>] [--shape <shape>] [--measure-storage] [--operations <ops>]
# Example: ./run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16
#          ./run_single_thread_bench.sh neon db_setup/tpcc_schema.sql 32 --seed 12345 --shape bushy
#          ./run_single_thread_bench.sh dolt data/tpcc.sql 8 --measure-storage
#          ./run_single_thread_bench.sh dolt data/tpcc.sql 16 --operations UPDATE,RANGE_UPDATE

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
NUM_BRANCHES=""
SEED=""
SHAPE="spine"
MEASURE_STORAGE=false
OPS_STRING=""
NUM_OPS_OVERRIDE=""
OUTPUT_DIR="/tmp/run_stats"
RANGE_SIZE=200

while [[ $# -gt 0 ]]; do
    case $1 in
        --seed)
            SEED="$2"
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
        --num-ops)
            NUM_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            if [ -z "$BACKEND" ]; then
                BACKEND="$1"
            elif [ -z "$SQL_DUMP_PATH" ]; then
                SQL_DUMP_PATH="$1"
            elif [ -z "$NUM_BRANCHES" ]; then
                NUM_BRANCHES="$1"
            else
                echo "Error: Unexpected argument '$1'"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$SQL_DUMP_PATH" ] || [ -z "$NUM_BRANCHES" ]; then
    echo "Usage: $0 <backend> <sql_dump_path> <num_branches> [options]"
    echo ""
    echo "Required:"
    echo "  backend: dolt, neon, kpg, xata, file_copy, postgres transactions (txn), tiger"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql)"
    echo "  num_branches: Number of branches to create for testing (e.g., 16)"
    echo ""
    echo "Options:"
    echo "  --seed <seed>: Random seed for reproducibility (default: random)"
    echo "  --shape <shape>: Branch tree shape: spine, bushy, or fan_out (default: spine)"
    echo "  --measure-storage: Measure disk_size_before/after for each update (reduces num_ops)"
    echo "  --operations <ops>: Comma-separated list (e.g., UPDATE,RANGE_UPDATE; default: all)"
    echo "  --range-size <n>: Range size for RANGE_UPDATE operation (default: 200)"
    echo "  --num-ops <n>: Number of operations to perform (overrides defaults)"
    echo "  --output-dir <dir>: Output directory for results (default: /tmp/run_stats)"
    echo ""
    echo "Examples:"
    echo "  $0 dolt db.sql 16                                 # 16 branches, default ops"
    echo "  $0 dolt db.sql 32 --operations READ --num-ops 10  # 32 branches, 10 reads"
    echo "  $0 dolt db.sql 8 --measure-storage                # 8 branches with storage measurement"
    echo ""
    echo "Note: For sweeping across multiple branch counts, use the dedicated scripts:"
    echo "  - For single-threaded sweeps: Use bench_lib.sh functions directly"
    echo "  - For throughput analysis: Use run_throughput_bench.sh"
    exit 1
fi

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|FILE_COPY|TXN|TIGER)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, neon, kpg, xata, file_copy, txn, tiger"
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
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Default operations
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
INSERTS_PER_BRANCH=0
UPDATES_PER_BRANCH=0
DELETES_PER_BRANCH=0

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_bench_config_XXXXXX)

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
echo "Single-Thread Benchmark"
echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPERATIONS[*]}"
echo "Num Branches: $NUM_BRANCHES"
echo "Branch Shape: $SHAPE"
echo "Random Seed: $SEED"
echo "Measure Storage: $MEASURE_STORAGE"
if [ -n "$NUM_OPS_OVERRIDE" ]; then
    echo "Num Ops (override): $NUM_OPS_OVERRIDE"
fi
echo "==================================================="

# Loop through all operations
for OPERATION in "${OPERATIONS[@]}"; do
    # Use override if provided
    if [ -n "$NUM_OPS_OVERRIDE" ]; then
        NUM_OPS="$NUM_OPS_OVERRIDE"
    else
        NUM_OPS=$(get_num_ops "$OPERATION")
    fi

    SHAPE_LOWER=$(echo "$SHAPE" | tr '[:upper:]' '[:lower:]')
    RUN_ID="${BACKEND}_${SQL_PREFIX}_${NUM_BRANCHES}_${SHAPE_LOWER}"

    # For BRANCH operation, num_branches in setup should be 0
    # For all other operations, num_branches matches target
    if [ "$OPERATION" = "BRANCH" ]; then
        SETUP_NUM_BRANCHES=0
    else
        SETUP_NUM_BRANCHES=$NUM_BRANCHES
    fi

    echo ""
    echo "---------------------------------------------------"
    echo "Running: $RUN_ID"
    echo "  Operation: $OPERATION, Num Ops: $NUM_OPS, Setup Branches: $SETUP_NUM_BRANCHES"
    echo "---------------------------------------------------"

    # Generate config file
    cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for single-thread benchmark
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
num_threads: 1
measure_storage: ${MEASURE_STORAGE}

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
    python -m microbench.runner --config "$TEMP_CONFIG" --seed $SEED --no-progress --output-dir "$OUTPUT_DIR"

    # Clean up dropped databases to prevent disk space explosion
    DOLT_DIR="${DOLT_DATA_DIR:-$HOME/doltgres/databases}"
    if [ -d "$DOLT_DIR/.dolt_dropped_databases" ]; then
        echo "Cleaning up dropped databases in $DOLT_DIR/.dolt_dropped_databases"
        rm -rf "$DOLT_DIR/.dolt_dropped_databases"/*
    fi

    echo "Completed: $RUN_ID"
done  # OPERATION loop

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in $OUTPUT_DIR/"
echo "==================================================="
