from abc import ABC, abstractmethod
from psycopg2.extensions import connection as pgconn


class DBToolSuite(ABC):
    """
    An API for interacting with Postgres via a shared connection.
    """

    def __init__(self, connection: pgconn):
        self.conn = connection

    @abstractmethod
    def create_db_branch(self, branch_name: str) -> str:
        """
        Creates a new branch in the underlying database.
        """
        pass

    @abstractmethod
    def connect_db_branch(self, branch_name: str) -> str:
        """
        Connects to an existing branch in the underlying database to allow
        reading and writing data to that branch.
        """
        pass

    @abstractmethod
    def list_db_branches(self) -> set[str]:
        """
        Lists all branches in the underlying database.
        """
        pass

    @abstractmethod
    def commit_changes(self, message: str = "") -> None:
        """
        Commits changes to the current branch with an optional message.
        """
        pass

    def create_db(self, db_name: str):
        """
        Creates a new database in the underlying Postgres server.
        """
        query = f"CREATE DATABASE {db_name};"
        self.run_sql_query(query)

    def delete_db(self, db_name: str):
        """
        Deletes a database from the underlying Postgres server.
        """
        query = f"DROP DATABASE IF EXISTS {db_name};"
        self.run_sql_query(query)

    def bulk_copy_from_file(self, table_name: str, file_path: str) -> None:
        """
        Bulk copies data from a CSV file into the specified table.
        """
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
        print(f"Initializing database schema...\n\n{schema_ddl}")
        self.run_sql_query(schema_ddl)

    def get_table_schema(self, table_name: str) -> str:
        """
        Returns the schema of a specific table in a simplified CREATE TABLE
        format. This is used to understand the structure of a table before
        trying to insert data.
        """
        # Query to get column name, data type, and if it's nullable
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s;
        """
        columns = self.run_sql_query(query, (table_name,))

        if not columns:
            return f"Error: Table '{table_name}' not found."

        # Format the schema into a readable string for the LLM
        schema_parts = [f"CREATE TABLE {table_name} ("]
        for col_name, data_type, is_nullable in columns:
            part = f"  {col_name} {data_type}"
            if is_nullable == "NO":
                part += " NOT NULL"
            schema_parts.append(part + ",")

        schema_parts.append(");")
        return "\n".join(schema_parts)

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

    def run_sql_query(self, query: str, vars=None) -> list[tuple]:
        """
        Runs an SQL query in the postgres database on the current branch. The
        query could be anything supported by the underlying database.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, vars)
                return cur.fetchall()
        except Exception as e:
            raise Exception(f"Error executing sql query: {e}")
