#!/bin/bash
# Teardown: stop PostgreSQL and detach the isolated APFS volume.
# Usage: ./db_setup/teardown_pg_volume.sh [--delete]

set -e

VOLUME_NAME="PGBench"
IMAGE_PATH="$HOME/pgbench.sparseimage"
MOUNT_POINT="/Volumes/$VOLUME_NAME"
DELETE_IMAGE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --delete)  DELETE_IMAGE=true; shift ;;
        *)         echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [ "$(uname -s)" != "Darwin" ]; then exit 0; fi

PG_DATA="$MOUNT_POINT/pgdata"
[ -f "$PG_DATA/postmaster.pid" ] && pg_ctl -D "$PG_DATA" stop || true
mount | grep -q "$MOUNT_POINT" && hdiutil detach "$MOUNT_POINT"
[ "$DELETE_IMAGE" = true ] && [ -f "$IMAGE_PATH" ] && rm "$IMAGE_PATH"

exit 0
