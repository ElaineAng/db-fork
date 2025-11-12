from abc import ABC, abstractmethod
from typing import Tuple
import psycopg2
from psycopg2.extensions import connection as _pgconn
from psycopg2.extensions import cursor as _pgcursor


class DBToolSuite(ABC):
    """
    An API for interacting with Postgres via a shared connection. The connection
    is always for a specific database, and, in some cases, a specific branch.
    """

    def __init__(
        self, connection: _pgconn = None, timed_cursor: _pgcursor = None
    ):
        # NOTE: Subclass need to create the connection if connection isn't
        # provided.
        self.conn = connection
        self.timed_cursor = timed_cursor
        if not self.timed_cursor:
            print("Timed cursor is not provided.")

    def check_connection(self):
        if not self.conn:
            raise ValueError("Database connection is not established.")

    def close_connection(self) -> None:
        """
        Closes the current database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    @abstractmethod
    def create_branch(
        self, branch_name: str, timed: bool = False, parent_id: str = None
    ) -> bool:
        """
        Creates a new branch.
        """
        pass

    @abstractmethod
    def connect_branch(self, branch_name: str, timed: bool = False) -> str:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch.
        """
        pass

    @abstractmethod
    def list_branches(self) -> set[str]:
        """
        Lists all existing branches by branch name.
        Used for debugging/logging/db setup so timing isn't enabled.
        """
        pass

    @abstractmethod
    def get_current_branch(self) -> Tuple[str, str]:
        """
        Returns a tuple of the current (branch_name, branch_id).
        branch_name isn't always unique and should be used for debugging/logging
        purposes only, while branch_id is needed to uniquely identify the
        current branch.
        Used for debugging/logging/db setup so timing isn't enabled.
        """
        pass

    def commit_changes(self, message: str = "", timed: bool = False) -> None:
        """
        Commits any pending changes to the database with an optional message.
        """
        self.check_connection()
        self.conn.commit()

    def delete_db(self, db_name: str) -> None:
        """
        Deletes a database from the underlying Postgres server.
        """
        query = f"DROP DATABASE IF EXISTS {db_name};"
        self.run_sql_query(query)

    def bulk_copy_from_file(self, table_name: str, file_path: str) -> None:
        """
        Bulk copies data from a CSV file into the specified table.
        """
        self.check_connection()
        try:
            with self.conn.cursor() as cur:
                with open(file_path, "r") as f:
                    cur.copy_expert(
                        (
                            f"COPY {table_name} FROM STDIN "
                            "WITH (FORMAT CSV, DELIMITER '|');"
                        ),
                        file=f,
                    )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error during bulk copy: {e}")

    def initialize_schema(self, schema_ddl: str) -> None:
        """
        Initializes the database schema using the provided DDL statements.
        """
        print("Initializing database schema...")
        sql_statements = [
            stmt.strip() for stmt in schema_ddl.split(";") if stmt.strip()
        ]
        for stmt in sql_statements:
            self.run_sql_query(stmt)

    def get_table_schema(self, table_name: str) -> str:
        """
        Returns the schema of a specific table in a CREATE TABLE format.
        """
        # Query for column details, including length and precision/scale
        query = """
        SELECT
            column_name,
            udt_name,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.columns
        WHERE
            table_name = %s
        ORDER BY
            ordinal_position;
        """
        columns = self.run_sql_query(query, (table_name,))

        if not columns:
            return f"Error: Table '{table_name}' not found."

        column_definitions = []
        for (
            col_name,
            udt_name,
            is_nullable,
            char_len,
            num_prec,
            num_scale,
        ) in columns:
            data_type = udt_name

            # Append length for character types
            if char_len is not None:
                data_type += f"({char_len})"
            # Append precision and scale for numeric types
            elif udt_name in ("numeric", "decimal") and num_prec is not None:
                data_type += f"({num_prec}, {num_scale})"

            # Construct the column definition line
            definition = f"  {col_name} {data_type}"
            if is_nullable == "NO":
                definition += " NOT NULL"
            column_definitions.append(definition)

        # Assemble the final CREATE TABLE string
        return "CREATE TABLE {} (\n{}\n);".format(
            table_name, ",\n".join(column_definitions)
        )

    def get_primary_key_columns(self, table_name: str) -> list[(str, int)]:
        """
        Returns a list of (pk_column_name, ordinal_position) pairs for the
        specified table.
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
                );  
        """
        pk_columns = self.run_sql_query(query, (table_name, table_name))
        return [(col[0], col[1]) for col in pk_columns]

    def get_all_tables(self) -> list[str]:
        """
        Returns a list of all table names in the public schema.
        """
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema');
        """
        tables = self.run_sql_query(query)
        return [table[0] for table in tables]

    def get_all_columns(self, table_name: str) -> list[str]:
        """
        Returns a list of all column names for the specified table.
        """
        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s;
        """
        columns = self.run_sql_query(query, (table_name,))
        return [col[0] for col in columns]

    def run_sql_query(
        self, query: str, vars=None, timed: bool = False
    ) -> list[tuple]:
        """
        Runs an SQL query in the postgres database on the current branch. The
        query could be anything supported by the underlying database.
        """
        self.check_connection()
        try:
            with self.conn.cursor(
                cursor_factory=self.timed_cursor if timed else None
            ) as cur:
                cur.execute(query, vars)
                # print(f"Executed query: {query} with vars: {vars}")
                return cur.fetchall()
        except psycopg2.ProgrammingError:
            # No results to fetch (e.g., for INSERT/UPDATE statements).
            return []
        except Exception as e:
            raise Exception(f"Error executing sql query: {query}; {vars}; {e}")
