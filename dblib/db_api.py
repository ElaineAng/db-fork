from dblib.result_collector import OpType
from abc import ABC, abstractmethod
from typing import Tuple
import psycopg2
from psycopg2.extensions import connection as _pgconn

import dblib.result_collector as rc


class DBToolSuite(ABC):
    """
    An API for interacting with Postgres via a shared connection. The connection
    is always for a specific database, and, in some cases, a specific branch.
    """

    def __init__(
        self, connection: _pgconn = None, 
        result_collector: Optional[ResultCollector] = None
    ):
        # NOTE: Subclass need to create the connection if connection isn't
        # provided.
        self.conn = connection
        self.result_collector = result_collector
        if not self.result_collector:
            print("Result collector is not provided.")

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
    
    def create_branch(self, branch_name: str, timed: bool = False, parent_id: str = None) -> bool:
        """
        Creates a new branch.
        """
        done = self.create_branch_impl(branch_name, timed, parent_id)
        if done:
            self.result_collector.flush_record()
        return done
    
    def connect_branch(self, branch_name: str, timed: bool = False) -> str:
        done = self.connect_branch_impl(branch_name, timed)
        if done:
            self.result_collector.flush_record()
        return done
    
    def timed_read(self, sql: str, values = None, num_keys: int = 0) -> List:
        read_result = self.run_sql_query(sql, values, timed=True, op_type=OpType.READ)
        self.result_collector.record_num_keys_touched(num_keys)
        self.result_collector.flush_record()
        return read_result
    
    def timed_insert(self, sql: str, values = None, num_keys: int = 0) -> None:
        self.run_sql_query(sql, values, timed=True, op_type=OpType.INSERT)
        self.conn.commit()
        self.result_collector.record_num_keys_touched(num_keys)
        self.result_collector.flush_record()
    
    def timed_update(self, sql: str, values = None, num_keys: int = 0) -> None:
        self.run_sql_query(sql, values, timed=True, op_type=OpType.UPDATE)
        self.conn.commit()
        self.result_collector.record_num_keys_touched(num_keys)
        self.result_collector.flush_record()

    @abstractmethod
    def create_branch_impl(
        self, branch_name: str, timed: bool = False, parent_id: str = None
    ) -> bool:
        """
        Creates a new branch.
        """
        pass

    @abstractmethod
    def connect_branch_impl(self, branch_name: str, timed: bool = False) -> str:
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
                            "WITH (FORMAT CSV, NULL 'null', DELIMITER ',');"
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
        with self.conn.cursor() as cur:
            for stmt in sql_statements:
                cur.execute(stmt)
            self.conn.commit()

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

    def _get_primary_key_columns(self, table_name: str) -> list[(str, int)]:
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
            ORDER BY ordinal_position ASC;
        """
        pk_columns = self.run_sql_query(query, (table_name, table_name))
        return [(col[0], col[1]) for col in pk_columns]
    
    def get_pk_columns_name(self, table_name: str) -> list[str]:
        """
        Returns a list of primary key column names for the specified table.
        """
        all_columns = [col[0] for col in self._get_primary_key_columns(table_name)]
        if not all_columns:
            raise ValueError(f"Table {table_name} has no primary key.")
        print(f" PK columns: {all_columns}")
        return all_columns

    def get_pk_values(self, table_name: str, pk_columns: Optional[list[str]] = None) -> set[tuple]:
        """
        Returns a set of primary key values for the specified table. This should
        be reasonably fast since it's an index-only scan.
        """
        if not pk_columns:
            pk_columns = self.get_pk_columns_name(table_name)

        sql = f"SELECT {', '.join(pk_columns)} FROM {table_name};"
        all_pks = self.run_sql_query(sql)

        count_sql = f"SELECT COUNT(*) FROM ({sql.rstrip(';')}) as sub; "
        count_result = self.run_sql_query(count_sql)
        total_count = count_result[0][0] if count_result else 0

        # The following count should be the same as the length of all_pks,
        # otherwise there is a bug with how the backend handles fetchall.
        print(f" Total primary keys fetched: {total_count}")
        print(f" Loaded {len(all_pks)} primary keys.")
        return set(all_pks) 


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
    
    def get_db_size(self) -> int:
        """
        Get the current database size in bytes.
        
        Returns:
            Database size in bytes.
        """
        # Get the current database name
        db_name_query = "SELECT current_database();"
        db_name_result = self.run_sql_query(db_name_query)
        db_name = db_name_result[0][0] if db_name_result else None
        
        if not db_name:
            print("Warning: Could not determine database name, returning 0")
            return 0
        
        # Query the size of the current database using pg_database_size
        size_query = "SELECT pg_database_size(%s);"
        size_result = self.run_sql_query(size_query, (db_name,))
        
        if size_result and size_result[0][0] is not None:
            return int(size_result[0][0])
        
        return 0

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
        self, query: str, vars=None, timed: bool = False, op_type: OpType = OpType.UNSPECIFIED
    ) -> list[tuple]:
        """
        Runs an SQL query in the postgres database on the current branch. The
        query could be anything supported by the underlying database.
        """
        self.check_connection()
        
        try:
            with self.conn.cursor(
                cursor_factory=lambda *args, **kwargs: rc.TimedCursor(
                    *args, **kwargs, result_collector=self.result_collector, 
                    op_type=op_type,
                ) if timed else None
            ) as cur:
                cur.execute(query, vars)
                #  print(f"Executed query: {query} with vars: {vars}")
                return cur.fetchall()
        except psycopg2.ProgrammingError:
            # No results to fetch (e.g., for INSERT/UPDATE statements).
            return []
        except Exception as e:
            raise Exception(f"Error executing sql query: {query}; {vars}; {e}")
