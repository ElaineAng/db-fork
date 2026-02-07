#!/bin/bash
# load_bird_critic_databases.sh - Load all BIRD-Critic database dumps into PostgreSQL
#
# Usage: ./load_bird_critic_databases.sh [--host HOST] [--port PORT] [--user USER] [--password PASSWORD] [--psql PSQL]
# Example: ./load_bird_critic_databases.sh --port 5433 --user elaineang --psql psql17

# Note: Not using 'set -e' to allow proper error handling and reporting

# Default connection parameters
HOST="localhost"
PORT="5433"
USER="elaineang"
PASSWORD=""
PSQL_CMD="psql17"
DUMP_DIR="db_setup/postgre_table_dumps"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --user)
            USER="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        --dump-dir)
            DUMP_DIR="$2"
            shift 2
            ;;
        --psql)
            PSQL_CMD="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "============================================="
echo "BIRD-Critic Database Loader"
echo "Host: $HOST:$PORT"
echo "User: $USER"
echo "PSQL Command: $PSQL_CMD"
echo "Dump Directory: $DUMP_DIR"
echo "============================================="

# Set PGPASSWORD for non-interactive authentication
export PGPASSWORD="$PASSWORD"

# Check if dump directory exists
if [ ! -d "$DUMP_DIR" ]; then
    echo "Error: Dump directory not found: $DUMP_DIR"
    exit 1
fi

# Count databases
DB_COUNT=$(find "$DUMP_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
echo "Found $DB_COUNT databases to load"
echo ""

# Track success/failure
SUCCESS=0
FAILED=0

# Iterate through each database directory
for DB_DIR in "$DUMP_DIR"/*/; do
    # Skip if not a directory
    [ -d "$DB_DIR" ] || continue
    
    # Get database name (remove _template suffix if present)
    DB_NAME=$(basename "$DB_DIR")
    DB_NAME=${DB_NAME%_template}
    
    echo "-------------------------------------------"
    echo "Loading database: $DB_NAME"
    
    # Create the database (drop if exists)
    echo "  Creating database..."
    DROP_OUTPUT=$($PSQL_CMD -h "$HOST" -p "$PORT" -U "$USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" 2>&1) || true
    CREATE_OUTPUT=$($PSQL_CMD -h "$HOST" -p "$PORT" -U "$USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";" 2>&1)
    CREATE_EXIT=$?
    
    if [ $CREATE_EXIT -ne 0 ]; then
        echo "  ERROR: Failed to create database $DB_NAME"
        echo "  Error message: $CREATE_OUTPUT"
        FAILED=$((FAILED + 1))
        continue
    fi
    
    # Load each SQL file in the database directory
    SQL_COUNT=0
    TABLE_ERRORS=0
    for SQL_FILE in "$DB_DIR"*.sql; do
        [ -f "$SQL_FILE" ] || continue
        
        TABLE_NAME=$(basename "$SQL_FILE" .sql)
        echo "  Loading table: $TABLE_NAME"
        
        # Run psql and show any errors (but don't fail on "already exists" or "role doesn't exist")
        OUTPUT=$($PSQL_CMD -h "$HOST" -p "$PORT" -U "$USER" -d "$DB_NAME" -f "$SQL_FILE" 2>&1)
        EXIT_CODE=$?
        
        # Check for COPY success (indicates data was loaded)
        if echo "$OUTPUT" | grep -q "^COPY [0-9]"; then
            ROWS=$(echo "$OUTPUT" | grep "^COPY [0-9]" | awk '{print $2}')
            echo "    Loaded $ROWS rows"
            SQL_COUNT=$((SQL_COUNT + 1))
        elif [ $EXIT_CODE -ne 0 ]; then
            echo "    WARNING: Error loading $TABLE_NAME"
            echo "$OUTPUT" | grep -i "error" | head -3
            TABLE_ERRORS=$((TABLE_ERRORS + 1))
        else
            # Table created but no data (might be OK for some tables)
            SQL_COUNT=$((SQL_COUNT + 1))
        fi
    done
    
    echo "  Loaded $SQL_COUNT table(s), $TABLE_ERRORS error(s)"
    SUCCESS=$((SUCCESS + 1))
done

echo ""
echo "============================================="
echo "Complete!"
echo "  Successful: $SUCCESS databases"
echo "  Failed: $FAILED databases"
echo "============================================="
