#!/usr/bin/env bash
# Run a macrobenchmark experiment with custom backend, scale, and SQL path.
#
# Usage:
#   ./run_macrobench.sh [--mini] [--outdir DIR] <workflow> <backend> <db_scale> <sql_path>
#
# Arguments:
#   --mini              Use the mini config (fewer workers/steps for Neon)
#   --outdir DIR        Directory for output parquet files (default: run_stats/)
#   --max-runtime-sec N Cap total workflow runtime in seconds (0 = no limit)
#   workflow     One of: software_dev, failure_repro, data_cleaning, mcts, simulation
#   backend      One of: dolt, neon, kpg, xata, file_copy, txn
#   db_scale     Integer scale factor (num warehouses)
#   sql_path     Path to the schema SQL dump file
#
# Example:
#   ./run_macrobench.sh mcts neon 10 db_setup/ch_benchmark_schema.sql
#   ./run_macrobench.sh --mini --outdir run_stats/neon_mini simulation neon 1 db_setup/ch-w1.sql
#   ./run_macrobench.sh --max-runtime-sec 600 mcts neon 10 db_setup/ch_benchmark_schema.sql

set -euo pipefail

MINI=false
STORAGE=false
MEASURE_STORAGE=false
OUTDIR="run_stats/"
MAX_RUNTIME_SEC=0

# Parse optional flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mini)            MINI=true; shift ;;
        --storage)         STORAGE=true; shift ;;
        --measure-storage) MEASURE_STORAGE=true; shift ;;
        --outdir)          OUTDIR="$2"; shift 2 ;;
        --max-runtime-sec) MAX_RUNTIME_SEC="$2"; shift 2 ;;
        *)                 break ;;
    esac
done

if [[ $# -ne 4 ]]; then
    echo "Usage: $0 [--mini] [--outdir DIR] [--max-runtime-sec N] <workflow> <backend> <db_scale> <sql_path>"
    echo "  --mini:              use mini config (fewer workers/steps for Neon)"
    echo "  --measure-storage:   enable Neon storage measurement (15-min sleep before/after)"
    echo "  --outdir:            output directory for parquet files (default: run_stats/)"
    echo "  --max-runtime-sec:   cap total workflow runtime in seconds (0 = no limit)"
    echo "  workflow:    software_dev | failure_repro | data_cleaning | mcts | simulation"
    echo "  backend:     dolt | neon | kpg | xata | file_copy | txn"
    echo "  db_scale:    integer scale factor (num warehouses)"
    echo "  sql_path:    path to schema SQL dump"
    exit 1
fi

WORKFLOW="$1"
BACKEND="$2"
DB_SCALE="$3"
SQL_PATH="$4"

# Validate workflow
VALID_WORKFLOWS="software_dev failure_repro data_cleaning mcts simulation"
if ! echo "$VALID_WORKFLOWS" | grep -qw "$WORKFLOW"; then
    echo "Error: invalid workflow '$WORKFLOW'"
    echo "Must be one of: $VALID_WORKFLOWS"
    exit 1
fi

# Validate backend
VALID_BACKENDS="dolt neon kpg xata file_copy txn"
if ! echo "$VALID_BACKENDS" | grep -qw "$BACKEND"; then
    echo "Error: invalid backend '$BACKEND'"
    echo "Must be one of: $VALID_BACKENDS"
    exit 1
fi

# Validate sql_path exists
if [[ ! -f "$SQL_PATH" ]]; then
    echo "Error: SQL file not found: $SQL_PATH"
    exit 1
fi

BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

if $STORAGE; then
    SUFFIX="_storage"
    RUN_ID="storage_${WORKFLOW}_${BACKEND}_${DB_SCALE}"
elif $MINI; then
    SUFFIX="_mini"
    RUN_ID="macro_${WORKFLOW}_mini_${BACKEND}_${DB_SCALE}"
else
    SUFFIX=""
    RUN_ID="macro_${WORKFLOW}_${BACKEND}_${DB_SCALE}"
fi

BASE_CONFIG="macrobench/configs/${WORKFLOW}${SUFFIX}.textproto"

if [[ ! -f "$BASE_CONFIG" ]]; then
    echo "Error: base config not found: $BASE_CONFIG"
    exit 1
fi

# Build a temporary config by patching the base config
# Use PID in the temp file name to avoid conflicts with concurrent runs
TMP_CONFIG=$(mktemp /tmp/macrobench_$$_XXXXXX.textproto)
trap 'rm -f "$TMP_CONFIG"' EXIT

sed \
    -e "s|^run_id:.*|run_id: \"${RUN_ID}\"|" \
    -e "s|^backend:.*|backend: ${BACKEND_UPPER}|" \
    -e "s|sql_dump_path:.*|sql_dump_path: \"${SQL_PATH}\"|" \
    -e "s|db_scale:.*|db_scale: ${DB_SCALE}|" \
    "$BASE_CONFIG" > "$TMP_CONFIG"

echo "=== Macrobench Run ==="
echo "  Workflow:  $WORKFLOW${SUFFIX}"
echo "  Backend:   $BACKEND_UPPER"
echo "  Scale:     $DB_SCALE"
echo "  SQL:       $SQL_PATH"
echo "  Run ID:    $RUN_ID"
echo "  Output:    $OUTDIR"
echo "  Timeout:   ${MAX_RUNTIME_SEC}s (0 = no limit)"
echo "  Config:    $TMP_CONFIG (patched from $BASE_CONFIG)"
echo "======================"

EXTRA_FLAGS=()
if $MEASURE_STORAGE; then
    EXTRA_FLAGS+=(--measure-storage)
fi

python -m macrobench.runner \
    --config "$TMP_CONFIG" \
    --outdir "$OUTDIR" \
    --measure-interference \
    --monitor-queries olap_heavy,olap_light \
    --max-runtime-sec "$MAX_RUNTIME_SEC" \
    "${EXTRA_FLAGS[@]}"
