#!/usr/bin/env python3
"""
Import CSV files into a PostgreSQL database using COPY command.

This script reads CSV files from a directory and imports them into corresponding
tables in the database. It assumes tables already exist with matching names
(filename without .csv extension).

Usage:
    python import_csv.py <csv_directory> [options]

Options:
    -H, --host       Database host (default: localhost)
    -p, --port       Database port (default: 5432)
    -d, --database   Database name (default: microbench_setup)
    -U, --user       Database user (default: postgres)
    -W, --password   Database password (default: empty)
    -t, --tables     Specific tables to import (default: all)

Example:
    python import_csv.py /tmp/db-fork/tpcc-3 -U elaineang
    python import_csv.py /tmp/db-fork/tpcc-3 -H localhost -d mydb -U myuser -t customer orders
"""

import argparse
import psycopg2
from pathlib import Path


def get_csv_files(directory: str) -> list[Path]:
    """Get all CSV files from the given directory."""
    csv_dir = Path(directory)
    if not csv_dir.exists():
        raise ValueError(f"Directory does not exist: {directory}")

    csv_files = sorted(csv_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No CSV files found in: {directory}")

    return csv_files


def import_csv_file(conn, csv_path: Path, table_name: str) -> int:
    """
    Import a single CSV file into the database table using COPY.

    Returns the number of rows imported.
    """
    with conn.cursor() as cur:
        # Get row count before import (quote table name for reserved words like 'order')
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        before_count = cur.fetchone()[0]

        # Use COPY command for fast bulk import
        with open(csv_path, "r") as f:
            # No header row in these CSV files
            cur.copy_expert(
                f'COPY "{table_name}" FROM STDIN '
                f"WITH (FORMAT CSV, NULL 'null', DELIMITER ',');",
                f,
            )

        # Get row count after import
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        after_count = cur.fetchone()[0]

        conn.commit()

        return after_count - before_count


def import_all_csvs(
    connection_uri: str, csv_directory: str, tables: list[str] = None
) -> dict:
    """
    Import all CSV files from a directory into the database.

    Args:
        connection_uri: PostgreSQL connection URI
        csv_directory: Directory containing CSV files
        tables: Optional list of specific tables to import. If None, imports all.

    Returns:
        Dict mapping table names to number of rows imported.
    """
    csv_files = get_csv_files(csv_directory)
    results = {}

    conn = psycopg2.connect(connection_uri)

    try:
        for csv_path in csv_files:
            table_name = csv_path.stem  # filename without extension

            # Skip if not in specified tables list
            if tables and table_name not in tables:
                print(f"Skipping {table_name} (not in tables list)")
                continue

            print(f"Importing {csv_path.name} into {table_name}...", end=" ")
            try:
                rows = import_csv_file(conn, csv_path, table_name)
                results[table_name] = rows
                print(f"✓ {rows} rows")
            except Exception as e:
                print(f"✗ Error: {e}")
                results[table_name] = f"Error: {e}"
                conn.rollback()
    finally:
        conn.close()

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Import CSV files into PostgreSQL database"
    )
    parser.add_argument(
        "csv_directory",
        type=str,
        help="Directory containing CSV files to import",
    )
    parser.add_argument(
        "-H",
        "--host",
        type=str,
        default="localhost",
        help="Database host (default: localhost)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=5432,
        help="Database port (default: 5432)",
    )
    parser.add_argument(
        "-d",
        "--database",
        type=str,
        default="microbench_setup",
        help="Database name (default: microbench_setup)",
    )
    parser.add_argument(
        "-U",
        "--user",
        type=str,
        default="postgres",
        help="Database user (default: postgres)",
    )
    parser.add_argument(
        "-W",
        "--password",
        type=str,
        default="",
        help="Database password (default: empty)",
    )
    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="+",
        default=None,
        help="Specific tables to import (default: all CSV files)",
    )

    args = parser.parse_args()

    # Build connection string
    connection_uri = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

    print(f"Importing CSV files from: {args.csv_directory}")
    print(f"Target database: {args.host}:{args.port}/{args.database}")
    print("-" * 50)

    results = import_all_csvs(
        connection_uri,
        args.csv_directory,
        args.tables,
    )

    print("-" * 50)
    print("Import Summary:")
    total_rows = 0
    for table, result in results.items():
        if isinstance(result, int):
            total_rows += result
            print(f"  {table}: {result} rows")
        else:
            print(f"  {table}: {result}")
    print(f"Total: {total_rows} rows imported")


if __name__ == "__main__":
    main()
