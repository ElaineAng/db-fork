#!/bin/bash
# Set up an isolated APFS disk image for PostgreSQL benchmarks.
# On macOS APFS, CREATE DATABASE ... FILE_COPY uses clonefile() which makes
# st_blocks overcount. An isolated volume lets shutil.disk_usage() measure
# true physical usage. On Linux st_blocks is accurate so this is a no-op.
#
# Usage: eval "$(./db_setup/setup_pg_volume.sh [--port <port>] [--size <size>])"

set -e

IMAGE_SIZE="50g"
PG_PORT=5432
PG_MAX_CONNECTIONS="${PG_MAX_CONNECTIONS:-1200}"
PG_ULIMIT_NOFILE="${PG_ULIMIT_NOFILE:-65536}"
VOLUME_NAME="PGBench"
IMAGE_PATH="$HOME/pgbench.sparseimage"
MOUNT_POINT="/Volumes/$VOLUME_NAME"

while [[ $# -gt 0 ]]; do
    case $1 in
        --size)  IMAGE_SIZE="$2"; shift 2 ;;
        --port)  PG_PORT="$2"; shift 2 ;;
        --max-connections) PG_MAX_CONNECTIONS="$2"; shift 2 ;;
        --nofile) PG_ULIMIT_NOFILE="$2"; shift 2 ;;
        *)       echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

pg_is_ours_running() {
    local pidfile="$1/postmaster.pid"
    [ -f "$pidfile" ] || return 1
    local pid; pid=$(head -1 "$pidfile")
    if kill -0 "$pid" 2>/dev/null; then return 0; fi
    >&2 echo "Removing stale postmaster.pid (pid $pid)..."
    rm -f "$pidfile"
    return 1
}

OS="$(uname -s)"

if [ "$OS" = "Darwin" ]; then
    [ -f "$IMAGE_PATH" ] || hdiutil create -size "$IMAGE_SIZE" -type SPARSE -fs APFS -volname "$VOLUME_NAME" "$IMAGE_PATH" >&2
    mount | grep -q "$MOUNT_POINT" || hdiutil attach "$IMAGE_PATH" >&2

    PG_DATA="$MOUNT_POINT/pgdata"
    [ -f "$PG_DATA/PG_VERSION" ] || initdb -D "$PG_DATA" >&2

    if ! pg_is_ours_running "$PG_DATA"; then
        if lsof -iTCP:"$PG_PORT" -sTCP:LISTEN -P -n >/dev/null 2>&1; then
            >&2 echo "ERROR: Port $PG_PORT already in use. Stop the other process or use --port <N>."
            exit 1
        fi
        if ! ulimit -n "$PG_ULIMIT_NOFILE" 2>/dev/null; then
            >&2 echo "Warning: unable to set ulimit -n to $PG_ULIMIT_NOFILE; continuing with current limit $(ulimit -n)."
        fi
        pg_ctl -D "$PG_DATA" -l "$MOUNT_POINT/pg.log" \
            -o "-p $PG_PORT -c max_connections=$PG_MAX_CONNECTIONS" start >&2
    fi

    # Ensure 'postgres' superuser exists (initdb creates OS-user role only)
    psql -p "$PG_PORT" -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname='postgres'" \
        | grep -q 1 \
        || psql -p "$PG_PORT" -d postgres -c "CREATE ROLE postgres WITH LOGIN SUPERUSER PASSWORD 'password'" >&2

    echo "export PGSQL_DATA_DIR=$MOUNT_POINT"
    echo "export PGSQL_PORT=$PG_PORT"
    echo "export PGSQL_MAX_CONNECTIONS=$PG_MAX_CONNECTIONS"

elif [ "$OS" = "Linux" ]; then
    echo "unset PGSQL_DATA_DIR"
    echo "export PGSQL_PORT=$PG_PORT"
    echo "export PGSQL_MAX_CONNECTIONS=$PG_MAX_CONNECTIONS"
else
    >&2 echo "Unsupported OS: $OS"; exit 1
fi
