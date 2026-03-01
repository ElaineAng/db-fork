#!/bin/bash
# run_throughput_bench.sh - Dedicated script for throughput experiments
#
# Usage:
#   ./run_throughput_bench.sh <backend> <sql_dump_path> --sweep-threads --branches <N> [options]
#   ./run_throughput_bench.sh <backend> <sql_dump_path> --sweep-branches --threads <N> [options]
#
# Examples:
#   # Fix branches at 1, vary threads: 1,2,4,8,16,32,64,128
#   ./run_throughput_bench.sh dolt db.sql --sweep-threads --branches 1
#
#   # Fix threads at 128, vary branches: 1,2,4,8,16,32
#   ./run_throughput_bench.sh dolt db.sql --sweep-branches --threads 128
#
#   # Custom thread/branch lists
#   ./run_throughput_bench.sh dolt db.sql --sweep-threads --branches 16 --thread-list "1,2,4,8,16,32"
#   ./run_throughput_bench.sh dolt db.sql --sweep-branches --threads 128 --branch-list "1,2,4,8,16"

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
SEED=""
SWEEP_MODE=""  # "threads" or "branches"
FIXED_THREADS=""
FIXED_BRANCHES=""
THREAD_LIST=""
BRANCH_LIST=""
OPERATIONS=""
NUM_OPS_OVERRIDE=""
OUTPUT_DIR="/tmp/run_stats"

while [[ $# -gt 0 ]]; do
    case $1 in
        --seed)
            SEED="$2"
            shift 2
            ;;
        --sweep-threads)
            SWEEP_MODE="threads"
            shift
            ;;
        --sweep-branches)
            SWEEP_MODE="branches"
            shift
            ;;
        --threads)
            FIXED_THREADS="$2"
            shift 2
            ;;
        --branches)
            FIXED_BRANCHES="$2"
            shift 2
            ;;
        --thread-list)
            THREAD_LIST="$2"
            shift 2
            ;;
        --branch-list)
            BRANCH_LIST="$2"
            shift 2
            ;;
        --num-ops)
            NUM_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --operations)
            OPERATIONS="$2"
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
            else
                echo "Error: Unexpected argument '$1'"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$SQL_DUMP_PATH" ] || [ -z "$SWEEP_MODE" ]; then
    echo "Usage: $0 <backend> <sql_dump_path> {--sweep-threads | --sweep-branches} [options]"
    echo ""
    echo "Required arguments:"
    echo "  backend: dolt, neon, kpg, xata, postgres transaction (txn), file_copy, tiger"
    echo "  sql_dump_path: Path to SQL dump file"
    echo "  --sweep-threads: Fix branches, vary threads (requires --branches)"
    echo "  --sweep-branches: Fix threads, vary branches (requires --threads)"
    echo ""
    echo "Options:"
    echo "  --threads <N>: Fixed thread count (for --sweep-branches mode)"
    echo "  --branches <N>: Fixed branch count (for --sweep-threads mode)"
    echo "  --thread-list <list>: Comma-separated thread counts (e.g., '1,2,4,8,16')"
    echo "  --branch-list <list>: Comma-separated branch counts (e.g., '1,2,4,8,16')"
    echo "  --seed <seed>: Random seed for reproducibility"
    echo "  --num-ops <n>: Number of operations per test"
    echo "  --operations <ops>: Comma-separated list (e.g., READ,RANGE_READ)"
    echo "  --output-dir <dir>: Output directory (default: /tmp/run_stats)"
    echo ""
    echo "Examples:"
    echo "  # 1 branch, varying threads (1,2,4,8,16,32,64,128)"
    echo "  $0 dolt db.sql --sweep-threads --branches 1"
    echo ""
    echo "  # 128 threads, varying branches (1,2,4,8,16,32)"
    echo "  $0 dolt db.sql --sweep-branches --threads 128"
    echo ""
    echo "  # Custom lists"
    echo "  $0 dolt db.sql --sweep-threads --branches 16 --thread-list '1,2,4,8,16,32'"
    echo "  $0 dolt db.sql --sweep-branches --threads 128 --branch-list '1,2,4,8,16'"
    exit 1
fi

# Validate sweep mode requirements
if [ "$SWEEP_MODE" = "threads" ] && [ -z "$FIXED_BRANCHES" ]; then
    echo "Error: --sweep-threads requires --branches <N>"
    exit 1
fi

if [ "$SWEEP_MODE" = "branches" ] && [ -z "$FIXED_THREADS" ]; then
    echo "Error: --sweep-branches requires --threads <N>"
    exit 1
fi

# Convert backend to uppercase
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|TXN|FILE_COPY|TIGER)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'"
    exit 1
fi

# Check SQL dump file
if [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Default operation lists
if [ -z "$OPERATIONS" ]; then
    OPERATIONS="READ,UPDATE,RANGE_READ,RANGE_UPDATE"
fi

# Convert operations to array
IFS=',' read -ra OPS_ARRAY <<< "$OPERATIONS"

# Determine thread and branch lists based on sweep mode
if [ "$SWEEP_MODE" = "threads" ]; then
    # Fix branches, vary threads
    NUM_BRANCHES=$FIXED_BRANCHES
    BRANCH_COUNTS=($NUM_BRANCHES)

    if [ -n "$THREAD_LIST" ]; then
        IFS=',' read -ra THREAD_COUNTS <<< "$THREAD_LIST"
    else
        # Default thread counts
        THREAD_COUNTS=(1 2 4 8 16 32 64 128 256 512 1024)
    fi

    echo "==================================================="
    echo "Throughput Benchmark: SWEEP THREADS"
    echo "Fixed branches: $NUM_BRANCHES"
    echo "Thread counts: ${THREAD_COUNTS[*]}"
elif [ "$SWEEP_MODE" = "branches" ]; then
    # Fix threads, vary branches
    NUM_THREADS=$FIXED_THREADS
    THREAD_COUNTS=($NUM_THREADS)

    if [ -n "$BRANCH_LIST" ]; then
        IFS=',' read -ra BRANCH_COUNTS <<< "$BRANCH_LIST"
    else
        # Default branch counts
        BRANCH_COUNTS=(1 2 4 8 16 32 64 128 256 512 1024)
    fi

    echo "==================================================="
    echo "Throughput Benchmark: SWEEP BRANCHES"
    echo "Fixed threads: $NUM_THREADS"
    echo "Branch counts: ${BRANCH_COUNTS[*]}"
fi

echo "Backend: $BACKEND"
echo "SQL Dump: $SQL_DUMP_PATH"
echo "Operations: ${OPS_ARRAY[*]}"
echo "Random Seed: $SEED"
if [ -n "$NUM_OPS_OVERRIDE" ]; then
    echo "Num Ops (override): $NUM_OPS_OVERRIDE"
fi
echo "==================================================="

# Fixed config values
TABLE_NAME="orders"
DB_NAME="throughput_bench"
INSERTS_PER_BRANCH=0
UPDATES_PER_BRANCH=0
DELETES_PER_BRANCH=0
RANGE_SIZE=100
SHAPE_UPPER="FAN_OUT"

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_throughput_bench_config_XXXXXX)
    
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

# Main loop: iterate through all combinations
for NUM_BRANCHES in "${BRANCH_COUNTS[@]}"; do
    for NUM_THREADS in "${THREAD_COUNTS[@]}"; do

        # Generate run_id that includes both thread and branch counts
        RUN_ID="${BACKEND}_${SQL_PREFIX}_tp_t${NUM_THREADS}_b${NUM_BRANCHES}"

        echo ""
        echo "==================================================="
        echo "Configuration: $NUM_THREADS threads, $NUM_BRANCHES branches"

        # Calculate distribution
        if [ $NUM_THREADS -le $NUM_BRANCHES ]; then
            echo "Distribution: Each thread handles multiple branches (round-robin)"
        else
            THREADS_PER_BRANCH=$((NUM_THREADS / NUM_BRANCHES))
            echo "Distribution: ~${THREADS_PER_BRANCH} threads per branch (cyclic)"
        fi
        echo "==================================================="

        for OPERATION in "${OPS_ARRAY[@]}"; do
            # Use override if provided
            if [ -n "$NUM_OPS_OVERRIDE" ]; then
                NUM_OPS="$NUM_OPS_OVERRIDE"
            # For CONNECT operations, scale with number of threads (2x)
            elif [[ "$OPERATION" =~ ^CONNECT ]]; then
                NUM_OPS=$((NUM_THREADS * 2))
            else
                NUM_OPS=$(get_num_ops "$OPERATION")
            fi

            # For BRANCH operation, num_branches in setup should be 0
            # For all other operations, setup num_branches matches the target
            if [ "$OPERATION" = "BRANCH" ]; then
                SETUP_NUM_BRANCHES=0
            else
                SETUP_NUM_BRANCHES=$NUM_BRANCHES
            fi

            echo ""
            echo "---------------------------------------------------"
            echo "Running: $RUN_ID, Operation: $OPERATION"
            echo "  Num Ops: $NUM_OPS, Setup Branches: $SETUP_NUM_BRANCHES"
            echo "  Threads: $NUM_THREADS, Branches: $NUM_BRANCHES"
            echo "---------------------------------------------------"

            # Generate config file
            cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for throughput benchmark
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

            # Run the benchmark
            echo "Starting benchmark..."
            python -m microbench.runner --config "$TEMP_CONFIG" --seed $SEED --no-progress --output-dir "$OUTPUT_DIR"

            # Clean up dropped databases to prevent disk space explosion
            DOLT_DIR="${DOLT_DATA_DIR:-$HOME/doltgres/databases}"
            if [ -d "$DOLT_DIR/.dolt_dropped_databases" ]; then
                echo "Cleaning up dropped databases in $DOLT_DIR/.dolt_dropped_databases"
                rm -rf "$DOLT_DIR/.dolt_dropped_databases"/*
            fi

            echo "Completed: $RUN_ID, Operation: $OPERATION"
        done  # OPERATION loop
    done  # NUM_THREADS loop
done  # NUM_BRANCHES loop

echo ""
echo "==================================================="
echo "All throughput benchmarks completed!"
echo "Results are in $OUTPUT_DIR/"
echo "==================================================="
