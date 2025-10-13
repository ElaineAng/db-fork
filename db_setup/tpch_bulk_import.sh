#!/bin/bash

# A script to bulk import all TPC-H .tbl files from a directory into a PostgreSQL database.

# --- 1. Input Validation ---
if [ "$#" -ne 4 ]; then
  echo "Error: Incorrect number of arguments."
  echo "Usage: $0 <directory_path> <database_name> <db_user> <db_port>"
  exit 1
fi

DATA_DIR="$1"
DB_NAME="$2"
DB_USER="$3"
DB_PORT="$4"

# Check if the provided path is a valid directory.
if [ ! -d "$DATA_DIR" ]; then
  echo "Error: Directory '$DATA_DIR' not found."
  exit 1
fi

# --- 2. Define TPC-H Tables ---
# The script will look for .tbl files matching these names.
TPCH_TABLES=(
  "region"
  "nation"
  "part"
  "supplier"
  "partsupp"
  "customer"
  "orders"
  "lineitem"
)

# --- 3. Processing Loop ---
echo "Starting TPC-H data import into database '$DB_NAME'..."
echo "====================================================="

for table in "${TPCH_TABLES[@]}"; do
  file_path="$DATA_DIR/$table.tbl"
  
  if [ -f "$file_path" ]; then
    echo " -> Importing data into '$table' from '$file_path'..."
    
    # Use psql's \copy command to load the pipe-delimited file.
    # The -c flag allows us to run a single command string.
    psql -d "$DB_NAME" -U "$DB_USER" -p "$DB_PORT" -c "\copy $table FROM '$file_path' WITH (FORMAT csv, DELIMITER '|');"
  else
    echo " -> WARNING: File '$file_path' not found. Skipping table '$table'."
  fi
done

# --- 4. Finalization ---
echo ""
echo "====================================================="
echo "Bulk import process finished."
echo "Don't forget to run 'VACUUM ANALYZE;' on your database to gather statistics for the query planner!"
