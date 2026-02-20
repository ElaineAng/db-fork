#!/bin/bash
# bench_lib.sh — Shared benchmark utility functions.
# Source this file; do not execute directly.

# Default constants (override before calling functions if needed)
TABLE_NAME="${TABLE_NAME:-orders}"
DB_NAME="${DB_NAME:-microbench}"
INSERTS_PER_BRANCH="${INSERTS_PER_BRANCH:-100}"
UPDATES_PER_BRANCH="${UPDATES_PER_BRANCH:-20}"
DELETES_PER_BRANCH="${DELETES_PER_BRANCH:-10}"
RANGE_SIZE="${RANGE_SIZE:-20}"
NUM_BRANCHES_LIST=(${NUM_BRANCHES_LIST[@]:-1 2 4 8 16 32 64 128 256 512 1024})

# load_env - Load .env file if present in the current directory.
load_env() {
    if [ -f .env ]; then
        export $(grep -v '^#' .env | xargs)
    fi
}

# validate_backend BACKEND
# Sets BACKEND_UPPER. Exits on invalid backend.
validate_backend() {
    local backend="$1"
    BACKEND_UPPER=$(echo "$backend" | tr '[:lower:]' '[:upper:]')
    if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|NEON|KPG|XATA|FILE_COPY)$ ]]; then
        echo "Error: Invalid backend '$backend'. Must be one of: dolt, neon, kpg, xata, file_copy"
        return 1
    fi
}

# validate_shape SHAPE
# Sets SHAPE_UPPER. Exits on invalid shape.
validate_shape() {
    local shape="$1"
    SHAPE_UPPER=$(echo "$shape" | tr '[:lower:]' '[:upper:]')
    if [[ ! "$SHAPE_UPPER" =~ ^(SPINE|BUSHY|FAN_OUT)$ ]]; then
        echo "Error: Invalid shape '$shape'. Must be one of: spine, bushy, fan_out"
        return 1
    fi
}

# get_num_ops_storage OPERATION
# Echoes reduced op count for storage measurement mode.
get_num_ops_storage() {
    case "$1" in
        BRANCH)            echo 1    ;;
        RANGE_UPDATE)      echo 20   ;;
        UPDATE)            echo 50   ;;
        CONNECT|READ|RANGE_READ) echo 1000 ;;
        *)                 echo 1000 ;;
    esac
}

# get_num_ops_default OPERATION
# Echoes standard op count.
get_num_ops_default() {
    case "$1" in
        BRANCH)            echo 1    ;;
        RANGE_UPDATE)      echo 200  ;;
        CONNECT|READ|UPDATE|RANGE_READ) echo 1000 ;;
        *)                 echo 1000 ;;
    esac
}

# generate_textproto CONFIG_FILE BACKEND_UPPER SHAPE_UPPER NUM_BRANCHES OPERATION NUM_OPS \
#                    MEASURE_STORAGE SQL_DUMP_PATH RUN_ID
# Writes a textproto config to CONFIG_FILE.
generate_textproto() {
    local config_file="$1"
    local backend_upper="$2"
    local shape_upper="$3"
    local num_branches="$4"
    local operation="$5"
    local num_ops="$6"
    local measure_storage="$7"
    local sql_dump_path="$8"
    local run_id="$9"

    cat > "$config_file" << EOF
# Auto-generated config for single-thread benchmark
run_id: "${run_id}"
backend: ${backend_upper}

table_name: "${TABLE_NAME}"
starting_branch: ""

database_setup {
  db_name: "${DB_NAME}"
  cleanup: true
  sql_dump {
    sql_dump_path: "${sql_dump_path}"
  }
}

range_update_config {
  range_size: ${RANGE_SIZE}
}

autocommit: true
num_threads: 1
$([ "$measure_storage" = true ] && echo "measure_storage: true")

nth_op_benchmark {
  operation: ${operation}
  num_ops: ${num_ops}
  setup {
    num_branches: ${num_branches}
    branch_shape: ${shape_upper}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }
}
EOF
}

# run_one_benchmark CONFIG_FILE SEED
# Runs the python benchmark runner and cleans up dropped Dolt databases.
run_one_benchmark() {
    local config_file="$1"
    local seed="$2"

    echo "Config generated at: $config_file"
    cat "$config_file"
    echo ""

    echo "Starting benchmark..."
    python -m microbench.runner --config "$config_file" --seed "$seed"

    # Clean up dropped databases to prevent disk space explosion
    rm -rf "${DOLT_DATA_DIR:-/tmp/doltgres_data/databases}/.dolt_dropped_databases"/*
}

# run_branch_sweep BACKEND SQL_DUMP_PATH SHAPE SEED MAX_BRANCHES MEASURE_STORAGE OPERATION [OPERATION...]
# Loops over NUM_BRANCHES_LIST, generates config + runs for each branch count and operation.
run_branch_sweep() {
    local backend="$1"
    local sql_dump_path="$2"
    local shape="$3"
    local seed="$4"
    local max_branches="$5"
    local measure_storage="$6"
    shift 6
    local operations=("$@")

    validate_backend "$backend"
    validate_shape "$shape"

    # Check if SQL dump file exists
    if [ ! -f "$sql_dump_path" ]; then
        echo "Error: SQL dump file not found: $sql_dump_path"
        return 1
    fi

    # Extract first 4 chars of sql_dump filename for run_id
    local sql_basename
    sql_basename=$(basename "$sql_dump_path" .sql)
    local sql_prefix="${sql_basename:0:4}"

    # Create temporary config file
    local temp_config
    temp_config=$(mktemp /tmp/${backend}_bench_config.XXXXXX.textproto)

    local shape_lower
    shape_lower=$(echo "$shape" | tr '[:upper:]' '[:lower:]')

    echo "==================================================="
    echo "Branch Sweep: backend=$backend shape=$shape_lower"
    echo "  Operations: ${operations[*]}"
    echo "  Branches: ${NUM_BRANCHES_LIST[*]} (max: $max_branches)"
    echo "  Measure Storage: $measure_storage"
    echo "==================================================="

    for num_branches in "${NUM_BRANCHES_LIST[@]}"; do
        # Skip if exceeds max_branches
        if [ "$num_branches" -gt "$max_branches" ]; then
            echo "Skipping num_branches=$num_branches (exceeds max_branches=$max_branches)"
            continue
        fi

        for operation in "${operations[@]}"; do
            local num_ops
            if [ "$measure_storage" = true ]; then
                num_ops=$(get_num_ops_storage "$operation")
            else
                num_ops=$(get_num_ops_default "$operation")
            fi
            local op_lower
            op_lower=$(echo "$operation" | tr '[:upper:]' '[:lower:]')
            local run_id="${backend}_${sql_prefix}_${num_branches}_${shape_lower}_${op_lower}"
            # Append range_size suffix for RANGE_UPDATE to disambiguate different range sizes
            if [ "$operation" = "RANGE_UPDATE" ]; then
                run_id="${run_id}_r${RANGE_SIZE}"
            fi

            echo ""
            echo "---------------------------------------------------"
            echo "Running: $run_id"
            echo "  Operation: $operation, Num Ops: $num_ops, Branches: $num_branches"
            echo "---------------------------------------------------"

            generate_textproto "$temp_config" "$BACKEND_UPPER" "$SHAPE_UPPER" \
                "$num_branches" "$operation" "$num_ops" "$measure_storage" \
                "$sql_dump_path" "$run_id"

            run_one_benchmark "$temp_config" "$seed"

            echo "Completed: $run_id"
        done
    done

    rm -f "$temp_config"
}

# generate_throughput_textproto CONFIG_FILE BACKEND_UPPER SHAPE_UPPER NUM_THREADS \
#   DURATION_SECONDS SQL_DUMP_PATH RUN_ID OPERATIONS_CSV [SETUP_NUM_BRANCHES] \
#   [SLOW_LATENCY_MULTIPLIER] [BASELINE_PARQUET_PATH] [BASELINE_MIN_SAMPLES]
# Writes a throughput_benchmark textproto config to CONFIG_FILE.
# OPERATIONS_CSV: comma-separated ops, e.g. "BRANCH" or "READ,UPDATE,RANGE_READ,RANGE_UPDATE"
# SETUP_NUM_BRANCHES: if > 0, includes a setup block with that many branches.
generate_throughput_textproto() {
    local config_file="$1"
    local backend_upper="$2"
    local shape_upper="$3"
    local num_threads="$4"
    local duration_seconds="$5"
    local sql_dump_path="$6"
    local run_id="$7"
    local ops_csv="$8"
    local setup_num_branches="${9:-0}"
    local slow_latency_multiplier="${10:-10.0}"
    local baseline_parquet_path="${11:-}"
    local baseline_min_samples="${12:-50}"

    # Build repeated operations lines
    local ops_lines=""
    IFS=',' read -ra ops_arr <<< "$ops_csv"
    for op in "${ops_arr[@]}"; do
        ops_lines="${ops_lines}  operations: ${op}\n"
    done

    # Build optional setup block
    local setup_block=""
    if [ "$setup_num_branches" -gt 0 ]; then
        setup_block="  setup {
    num_branches: ${setup_num_branches}
    branch_shape: ${shape_upper}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }"
    fi

    local baseline_block=""
    if [ -n "$baseline_parquet_path" ]; then
        baseline_block="  baseline_parquet_path: \"${baseline_parquet_path}\""
    fi

    cat > "$config_file" << EOF
# Auto-generated config for throughput benchmark
run_id: "${run_id}"
backend: ${backend_upper}

table_name: "${TABLE_NAME}"
starting_branch: ""

database_setup {
  db_name: "${DB_NAME}"
  cleanup: true
  sql_dump {
    sql_dump_path: "${sql_dump_path}"
  }
}

range_update_config {
  range_size: ${RANGE_SIZE}
}

autocommit: true
num_threads: ${num_threads}

throughput_benchmark {
  duration_seconds: ${duration_seconds}
  slow_latency_multiplier: ${slow_latency_multiplier}
  baseline_min_samples: ${baseline_min_samples}
$(echo -e "$ops_lines")${setup_block}
${baseline_block}
}
EOF
}

# run_throughput_sweep BACKEND SQL_DUMP_PATH SHAPE SEED MAX_THREADS DURATION_SECONDS \
#   EXP_MODE OPERATIONS_CSV [THREAD_LIST...]
# EXP_MODE: "branch" (3a, no setup) or "crud" (3b, setup branches = num_threads)
# Loops over thread counts, generates throughput config + runs each.
run_throughput_sweep() {
    local backend="$1"
    local sql_dump_path="$2"
    local shape="$3"
    local seed="$4"
    local max_threads="$5"
    local duration_seconds="$6"
    local exp_mode="$7"  # "branch" or "crud"
    local ops_csv="$8"
    shift 8
    local thread_list=("$@")

    validate_backend "$backend"
    validate_shape "$shape"

    if [ ! -f "$sql_dump_path" ]; then
        echo "Error: SQL dump file not found: $sql_dump_path"
        return 1
    fi

    local sql_basename
    sql_basename=$(basename "$sql_dump_path" .sql)
    local sql_prefix="${sql_basename:0:4}"

    local temp_config
    temp_config=$(mktemp /tmp/${backend}_throughput_config.XXXXXX.textproto)

    local shape_lower
    shape_lower=$(echo "$shape" | tr '[:upper:]' '[:lower:]')

    echo "==================================================="
    echo "Throughput Sweep: backend=$backend shape=$shape_lower mode=$exp_mode"
    echo "  Operations: $ops_csv"
    echo "  Threads: ${thread_list[*]} (max: $max_threads)"
    echo "  Duration: ${duration_seconds}s"
    echo "==================================================="

    for num_threads in "${thread_list[@]}"; do
        if [ "$num_threads" -gt "$max_threads" ]; then
            echo "Skipping num_threads=$num_threads (exceeds max=$max_threads)"
            continue
        fi

        local setup_branches=0
        if [ "$exp_mode" = "crud" ]; then
            setup_branches=$num_threads
        fi

        local run_id="${backend}_${sql_prefix}_${shape_lower}_${num_threads}t_${exp_mode}_throughput"

        echo ""
        echo "---------------------------------------------------"
        echo "Running: $run_id"
        echo "  Threads: $num_threads, Duration: ${duration_seconds}s, Setup branches: $setup_branches"
        echo "---------------------------------------------------"

        generate_throughput_textproto "$temp_config" "$BACKEND_UPPER" "$SHAPE_UPPER" \
            "$num_threads" "$duration_seconds" "$sql_dump_path" "$run_id" \
            "$ops_csv" "$setup_branches"

        run_one_benchmark "$temp_config" "$seed"

        echo "Completed: $run_id"
    done

    rm -f "$temp_config"
}
