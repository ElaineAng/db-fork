import argparse
import sys
from pathlib import Path
from typing import Union

import psycopg2
from psycopg2.extensions import connection as Connection


def load_sql_file(
    conn: Connection,
    sql_file_path: Union[str, Path],
    verbose: bool = False,
) -> None:
    """
    Loads and executes a SQL file into a PostgreSQL database.

    Args:
        conn: Active psycopg2 connection to the target database
        sql_file_path: Path to the .sql file to execute
        verbose: If True, prints progress and statement counts

    Raises:
        FileNotFoundError: If the SQL file doesn't exist
        psycopg2.Error: If there's an error executing the SQL statements
    """
    sql_file_path = Path(sql_file_path)

    if not sql_file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    if verbose:
        print(f"Loading SQL file: {sql_file_path}")

    # Read the entire SQL file
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Execute the SQL content
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql_content)
            conn.commit()

            if verbose:
                print(f"✓ Successfully executed SQL file: {sql_file_path}")
                if cursor.rowcount >= 0:
                    print(f"  Rows affected: {cursor.rowcount}")

        except psycopg2.Error as e:
            conn.rollback()
            print(f"✗ Error executing SQL file: {e}", file=sys.stderr)
            raise


def main():
    """
    Standalone script entry point for loading SQL files into PostgreSQL.

    Usage:
        python -m dblib.util <sql_file> --host <host> --port <port> --user <user> --password <password> --database <db_name>
    """
    parser = argparse.ArgumentParser(
        description="Load a SQL file into a PostgreSQL database"
    )
    parser.add_argument("sql_file", type=str, help="Path to the .sql file")
    parser.add_argument(
        "--host", type=str, default="localhost", help="Database host"
    )
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--user", type=str, required=True, help="Database user")
    parser.add_argument(
        "--password", type=str, required=True, help="Database password"
    )
    parser.add_argument(
        "--database", "-d", type=str, required=True, help="Database name"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )

    args = parser.parse_args()

    # Create database connection
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.database,
        )

        if args.verbose:
            print(
                f"Connected to database: {args.database} at {args.host}:{args.port}"
            )

        # Load the SQL file
        load_sql_file(conn, args.sql_file, verbose=args.verbose)

        conn.close()

        if args.verbose:
            print("Connection closed successfully")

        sys.exit(0)

    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"File error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
