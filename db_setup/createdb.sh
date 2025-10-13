#!/bin/bash

# A script to create a PostgreSQL database if it does not already exist.

# Exit immediately if a command exits with a non-zero status.
set -e

# Check if a database name was provided as an argument.
if [ -z "$1" ]; then
  echo "Usage: $0 <databasename>"
  exit 1
fi

DB_NAME=$1
DB_USER=${2:-postgres} # Default to 'postgres' user if no second argument is given

# The psql command lists all databases.
# We pipe this to grep to search for the provided database name.
# The -q flag for grep suppresses output, and its exit code (0 for found, 1 for not found)
# is used for the conditional logic.
# The -w flag ensures an exact word match.
if psql -U "$DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "✅ Database '$DB_NAME' already exists. No action taken."
else
    echo "Database '$DB_NAME' does not exist. Creating..."
    # Create the database. The `createdb` utility is a simple wrapper for the CREATE DATABASE SQL command.
    createdb -U "$DB_USER" "$DB_NAME"
    echo "✅ Database '$DB_NAME' created successfully."
fi