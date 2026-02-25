#!/bin/bash
# run_xata_all.sh — Run Xata experiments 1, 2, and 3 concurrently.
#
# Each experiment uses a separate Xata org + API key.
# The run.sh scripts have non-Xata backends commented out.
#
# Usage:
#   bash scripts/run_xata_all.sh          # all three
#   bash scripts/run_xata_all.sh 1 3      # only exp 1 and 3

set -u

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Load .env
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source <(grep -v '^#' "$REPO_ROOT/.env" | grep -v '^\s*$')
    set +a
fi

# Credentials
XATA_API_KEY_1="${XATA_API_KEY_1:?Set XATA_API_KEY_1}"
XATA_ORGANIZATION_ID_1="${XATA_ORGANIZATION_ID_1:?Set XATA_ORGANIZATION_ID_1}"
XATA_API_KEY_2="${XATA_API_KEY_2:?Set XATA_API_KEY_2}"
XATA_ORGANIZATION_ID_2="${XATA_ORGANIZATION_ID_2:?Set XATA_ORGANIZATION_ID_2}"
XATA_API_KEY_3="${XATA_API_KEY_3:?Set XATA_API_KEY_3}"
XATA_ORGANIZATION_ID_3="${XATA_ORGANIZATION_ID_3:?Set XATA_ORGANIZATION_ID_3}"

# Separate staging dirs so parquet filenames don't collide
RUN_STATS_DIR_EXP1="/tmp/run_stats_xata_exp1"
RUN_STATS_DIR_EXP2="/tmp/run_stats_xata_exp2"
RUN_STATS_DIR_EXP3="/tmp/run_stats_xata_exp3"

# Standard result dirs (where other backends' data already lives)
DATA_DIR_EXP1="$REPO_ROOT/experiments/experiment-1-2026-02-08/results/data/xata"
DATA_DIR_EXP2="$REPO_ROOT/experiments/experiment-2-2026-02-08/results/data/xata"
DATA_DIR_EXP3="$REPO_ROOT/experiments/experiment-3-throughput/results/data/xata"

# Which experiments to run (default: all)
if [ "$#" -eq 0 ]; then
    EXPS_TO_RUN=(1 2 3)
else
    EXPS_TO_RUN=("$@")
fi

LOG_DIR="/tmp/xata_run_logs"
mkdir -p "$LOG_DIR"

PIDS=()
LABELS=()

# ── Experiment 1 ────────────────────────────────────────────────────
if printf '%s\n' "${EXPS_TO_RUN[@]}" | grep -qx 1; then
    echo "Launching Experiment 1 (Xata) ..."
    mkdir -p "$RUN_STATS_DIR_EXP1" "$DATA_DIR_EXP1"
    (
        export XATA_API_KEY="$XATA_API_KEY_1"
        export XATA_ORGANIZATION_ID="$XATA_ORGANIZATION_ID_1"
        export RUN_STATS_DIR="$RUN_STATS_DIR_EXP1"
        bash "$REPO_ROOT/experiments/experiment-1-2026-02-08/run.sh"
        cp -v "$RUN_STATS_DIR"/xata_*.parquet "$DATA_DIR_EXP1/" 2>/dev/null || true
    ) >"$LOG_DIR/exp1.log" 2>&1 &
    PIDS+=($!)
    LABELS+=("Exp1")
fi

# ── Experiment 2 ────────────────────────────────────────────────────
if printf '%s\n' "${EXPS_TO_RUN[@]}" | grep -qx 2; then
    echo "Launching Experiment 2 (Xata) ..."
    mkdir -p "$RUN_STATS_DIR_EXP2" "$DATA_DIR_EXP2"
    (
        export XATA_API_KEY="$XATA_API_KEY_2"
        export XATA_ORGANIZATION_ID="$XATA_ORGANIZATION_ID_2"
        export RUN_STATS_DIR="$RUN_STATS_DIR_EXP2"
        bash "$REPO_ROOT/experiments/experiment-2-2026-02-08/run.sh"
        cp -v "$RUN_STATS_DIR"/xata_*.parquet "$DATA_DIR_EXP2/" 2>/dev/null || true
    ) >"$LOG_DIR/exp2.log" 2>&1 &
    PIDS+=($!)
    LABELS+=("Exp2")
fi

# ── Experiment 3 ────────────────────────────────────────────────────
if printf '%s\n' "${EXPS_TO_RUN[@]}" | grep -qx 3; then
    echo "Launching Experiment 3 (Xata) ..."
    mkdir -p "$RUN_STATS_DIR_EXP3" "$DATA_DIR_EXP3"
    (
        export XATA_API_KEY="$XATA_API_KEY_3"
        export XATA_ORGANIZATION_ID="$XATA_ORGANIZATION_ID_3"
        export RUN_STATS_DIR="$RUN_STATS_DIR_EXP3"
        export MANIFEST_MODE="append"
        bash "$REPO_ROOT/experiments/experiment-3-throughput/run.sh"
    ) >"$LOG_DIR/exp3.log" 2>&1 &
    PIDS+=($!)
    LABELS+=("Exp3")
fi

# ── Wait ────────────────────────────────────────────────────────────
echo ""
echo "Running ${#PIDS[@]} experiment(s) in parallel.  PIDs: ${PIDS[*]}"
echo "Logs:  tail -f $LOG_DIR/exp*.log"
echo ""

FAIL=0
for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then
        echo "[OK]   ${LABELS[$i]} (PID ${PIDS[$i]})"
    else
        echo "[FAIL] ${LABELS[$i]} (PID ${PIDS[$i]}) exit $?"
        FAIL=1
    fi
done

echo ""
echo "Results:"
echo "  Exp 1: $DATA_DIR_EXP1/"
echo "  Exp 2: $DATA_DIR_EXP2/"
echo "  Exp 3: $DATA_DIR_EXP3/"
[ "$FAIL" -ne 0 ] && echo "Some experiments failed — check $LOG_DIR/"
