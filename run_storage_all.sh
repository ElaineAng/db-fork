#!/usr/bin/env bash
# Run storage measurement benchmarks for all workflows on both Dolt and Neon.
#
# Uses the *_storage.textproto configs via run_macrobench.sh --storage.
# Results go to run_stats/dolt_storage/ and run_stats/neon_storage/.
#
# Usage:
#   ./run_storage_all.sh [--sql-path PATH] [--db-scale N]
#
# Defaults:
#   --sql-path  db_setup/ch_benchmark_seed.sql
#   --db-scale  1

set -euo pipefail

SQL_PATH="db_setup/ch_benchmark_seed.sql"
DB_SCALE=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sql-path)  SQL_PATH="$2"; shift 2 ;;
        --db-scale)  DB_SCALE="$2"; shift 2 ;;
        *)           echo "Unknown flag: $1"; exit 1 ;;
    esac
done

WORKFLOWS=(software_dev failure_repro data_cleaning mcts simulation)
BACKENDS=(dolt neon)

TOTAL=$(( ${#WORKFLOWS[@]} * ${#BACKENDS[@]} ))
COUNT=0

for BACKEND in "${BACKENDS[@]}"; do
    OUTDIR="run_stats/${BACKEND}_storage"
    mkdir -p "$OUTDIR"

    for WORKFLOW in "${WORKFLOWS[@]}"; do
        COUNT=$((COUNT + 1))
        echo ""
        echo "========================================"
        echo "  [$COUNT/$TOTAL] $WORKFLOW / $BACKEND"
        echo "========================================"

        ./run_macrobench.sh \
            --storage \
            --measure-storage \
            --outdir "$OUTDIR" \
            "$WORKFLOW" "$BACKEND" "$DB_SCALE" "$SQL_PATH"

        echo "  Done: $WORKFLOW / $BACKEND"
    done
done

echo ""
echo "All storage benchmarks complete."
echo "  Dolt results: run_stats/dolt_storage/"
echo "  Neon results: run_stats/neon_storage/"
