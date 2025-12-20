"""
Database helper utilities for PostgreSQL operations.

This module provides standalone utility functions for common database operations
that work with any psycopg2 connection.
"""

from typing import Optional
import psycopg2


def _run_sql_query(
    conn: psycopg2.extensions.connection, query: str, params: tuple = None
) -> list[tuple]:
    """
    Execute a SQL query and return all results (internal helper).

    Args:
        conn: Active psycopg2 connection
        query: SQL query to execute
        params: Optional query parameters for parameterized queries

    Returns:
        List of result tuples

    Raises:
        Exception: If query execution fails
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                # No results to fetch (e.g., for INSERT/UPDATE statements)
                return []
    except Exception as e:
        raise Exception(f"Error executing SQL query: {query}; {params}; {e}")


def initialize_schema(
    conn: psycopg2.extensions.connection, schema_ddl: str
) -> None:
    """
    Initialize the database schema using the provided DDL statements.

    Args:
        conn: Active psycopg2 connection
        schema_ddl: DDL statements separated by semicolons
    """
    print("Initializing database schema...")
    sql_statements = [
        stmt.strip() for stmt in schema_ddl.split(";") if stmt.strip()
    ]
    with conn.cursor() as cur:
        for stmt in sql_statements:
            cur.execute(stmt)

    conn.commit()


def _get_primary_key_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> list[tuple[str, int]]:
    """
    Get the primary key columns for a table.

    Args:
        conn: Active psycopg2 connection
        table_name: Name of the table

    Returns:
        List of (column_name, ordinal_position) tuples
    """
    query = """
        SELECT 
            column_name, ordinal_position
        FROM 
            information_schema.key_column_usage
        WHERE 
            table_schema = 'public'
            AND table_name = %s
            AND constraint_name = (
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = 'public'
                AND table_name = %s
                AND constraint_type = 'PRIMARY KEY'
            )
        ORDER BY ordinal_position ASC;
    """
    pk_columns = _run_sql_query(conn, query, (table_name, table_name))
    return [(col[0], col[1]) for col in pk_columns]


def get_pk_column_names(
    conn: psycopg2.extensions.connection, table_name: str
) -> list[str]:
    """
    Get the primary key column names for a table.

    Args:
        conn: Active psycopg2 connection
        table_name: Name of the table

    Returns:
        List of primary key column names

    Raises:
        ValueError: If table has no primary key
    """
    all_columns = [col[0] for col in _get_primary_key_columns(conn, table_name)]
    if not all_columns:
        raise ValueError(f"Table {table_name} has no primary key.")
    print(f" PK columns: {all_columns}")
    return all_columns


def get_pk_values(
    conn: psycopg2.extensions.connection,
    table_name: str,
    pk_columns: Optional[list[str]] = None,
) -> set[tuple]:
    """
    Get all primary key values for a table.

    This should be reasonably fast since it's an index-only scan.

    Args:
        conn: Active psycopg2 connection
        table_name: Name of the table
        pk_columns: Optional list of PK column names (auto-detected if not provided)

    Returns:
        Set of primary key value tuples
    """
    if not pk_columns:
        pk_columns = get_pk_column_names(conn, table_name)

    sql = f"SELECT {', '.join(pk_columns)} FROM {table_name};"
    all_pks = _run_sql_query(conn, sql)

    # count_sql = f"SELECT COUNT(*) FROM ({sql.rstrip(';')}) as sub;"
    # count_result = _run_sql_query(conn, count_sql)
    # total_count = count_result[0][0] if count_result else 0

    # # The following count should be the same as the length of all_pks,
    # # otherwise there is a bug with how the backend handles fetchall.
    # print(
    #     f"\t --> Total primary keys fetched: {total_count}, "
    #     f"Loaded {len(all_pks)} primary keys."
    # )
    return all_pks


def get_all_tables(conn: psycopg2.extensions.connection) -> list[str]:
    """
    Get all table names in the public schema.

    Args:
        conn: Active psycopg2 connection

    Returns:
        List of table names
    """
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
    """
    tables = _run_sql_query(conn, query)
    return [table[0] for table in tables]


def get_db_size(conn: psycopg2.extensions.connection) -> int:
    """
    Get the current database size in bytes.

    Args:
        conn: Active psycopg2 connection

    Returns:
        Database size in bytes, or 0 if unable to determine
    """
    # Get the current database name
    db_name_query = "SELECT current_database();"
    db_name_result = _run_sql_query(conn, db_name_query)
    db_name = db_name_result[0][0] if db_name_result else None

    if not db_name:
        print("Warning: Could not determine database name, returning 0")
        return 0

    # Query the size of the current database using pg_database_size
    size_query = "SELECT pg_database_size(%s);"
    size_result = _run_sql_query(conn, size_query, (db_name,))

    if size_result and size_result[0][0] is not None:
        return int(size_result[0][0])

    return 0


def get_all_columns(
    conn: psycopg2.extensions.connection, table_name: str
) -> list[str]:
    """
    Get all column names for a table.

    Args:
        conn: Active psycopg2 connection
        table_name: Name of the table

    Returns:
        List of column names
    """
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s
    """
    columns = _run_sql_query(conn, query, (table_name,))
    return [col[0] for col in columns]
