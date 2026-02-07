"""Database utilities for BIRD-Critic agent."""

import os
from contextlib import contextmanager
from typing import Any, Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection


class DatabaseManager:
    """Manages PostgreSQL database connections and queries."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        user: str = "elaineang",
        password: str = "",
        database: str = "postgres",
    ):
        """Initialize database connection parameters.

        Args:
            host: Database host.
            port: Database port.
            user: Database user.
            password: Database password.
            database: Default database name.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._connection: Optional[PgConnection] = None

    @classmethod
    def from_env(cls) -> "DatabaseManager":
        """Create a DatabaseManager from environment variables."""
        return cls(
            host=os.getenv("PGHOST", "localhost"),
            port=int(os.getenv("PGPORT", "5432")),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "password"),
            database=os.getenv("PGDATABASE", "postgres"),
        )

    @classmethod
    def from_uri(cls, uri: str) -> "DatabaseManager":
        """Create a DatabaseManager from a PostgreSQL URI."""
        # Parse postgresql://user:password@host:port/database
        from urllib.parse import urlparse

        parsed = urlparse(uri)
        return cls(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            user=parsed.username or "postgres",
            password=parsed.password or "password",
            database=parsed.path.lstrip("/") or "postgres",
        )

    def connect(self, database: Optional[str] = None) -> PgConnection:
        """Connect to the database.

        Args:
            database: Database name to connect to (uses default if not specified).

        Returns:
            PostgreSQL connection object.
        """
        db_name = database or self.database
        self._connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=db_name,
        )
        self._connection.autocommit = True
        return self._connection

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    @contextmanager
    def connection_context(self, database: Optional[str] = None):
        """Context manager for database connections.

        Args:
            database: Database name to connect to.

        Yields:
            PostgreSQL connection object.
        """
        conn = self.connect(database)
        try:
            yield conn
        finally:
            self.close()

    def execute_sql(
        self, sql: str, params: Optional[tuple] = None
    ) -> tuple[bool, Any, Optional[str]]:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query to execute.
            params: Query parameters (optional).

        Returns:
            Tuple of (success, results, error_message).
            - success: True if query executed without error.
            - results: Query results (list of tuples) or None.
            - error_message: Error message if failed, None otherwise.
        """
        if not self._connection:
            return False, None, "No database connection"

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)

                # Check if query returns results
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return True, {"columns": columns, "rows": rows}, None
                else:
                    # DML statement (INSERT, UPDATE, DELETE)
                    return True, {"rowcount": cursor.rowcount}, None

        except Exception as e:
            return False, None, str(e)

    def get_table_schema(self, table_name: str) -> Optional[str]:
        """Get the CREATE TABLE statement for a table.

        Args:
            table_name: Name of the table.

        Returns:
            CREATE TABLE statement or None if table not found.
        """
        if not self._connection:
            return None

        try:
            with self._connection.cursor() as cursor:
                # Get column information
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """,
                    (table_name,),
                )
                columns = cursor.fetchall()

                if not columns:
                    return None

                # Build CREATE TABLE statement
                col_defs = []
                for col_name, data_type, nullable, default in columns:
                    col_def = f"    {col_name} {data_type.upper()}"
                    if nullable == "NO":
                        col_def += " NOT NULL"
                    if default:
                        col_def += f" DEFAULT {default}"
                    col_defs.append(col_def)

                # Get primary key
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                """,
                    (table_name,),
                )
                pk_cols = [row[0] for row in cursor.fetchall()]

                if pk_cols:
                    col_defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

                return (
                    f"CREATE TABLE {table_name} (\n"
                    + ",\n".join(col_defs)
                    + "\n);"
                )

        except Exception as e:
            return f"-- Error getting schema: {e}"

    def list_tables(self) -> list[str]:
        """List all tables in the current database.

        Returns:
            List of table names.
        """
        if not self._connection:
            return []

        try:
            with self._connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []

    def get_sample_rows(
        self, table_name: str, limit: int = 5
    ) -> Optional[dict]:
        """Get sample rows from a table.

        Args:
            table_name: Name of the table.
            limit: Maximum number of rows to return.

        Returns:
            Dictionary with columns and rows, or None on error.
        """
        success, result, _ = self.execute_sql(
            f"SELECT * FROM {table_name} LIMIT %s", (limit,)
        )
        return result if success else None

    def run_preprocess_sql(self, sql: Optional[str]) -> bool:
        """Run preprocessing SQL before a test.

        Args:
            sql: SQL to execute (may be semicolon-separated statements).

        Returns:
            True if all statements succeeded.
        """
        if not sql:
            return True

        # Split by semicolons and execute each statement
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            success, _, error = self.execute_sql(stmt)
            if not success:
                print(f"Preprocess SQL failed: {error}")
                return False
        return True

    def run_cleanup_sql(self, sql: Optional[str]) -> bool:
        """Run cleanup SQL after a test.

        Args:
            sql: SQL to execute (may be semicolon-separated statements).

        Returns:
            True if all statements succeeded.
        """
        if not sql:
            return True

        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            success, _, error = self.execute_sql(stmt)
            if not success:
                print(f"Cleanup SQL failed: {error}")
                return False
        return True
