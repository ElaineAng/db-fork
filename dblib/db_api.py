import functools
from psycopg2.extensions import connection as _pgconn
from abc import ABC, abstractmethod
from typing import Tuple, Optional

import dblib.result_collector as rc
from dblib import result_pb2 as rslt


def _require_connection(func):
    """Decorator that checks if database connection is established before calling the method."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.conn:
            raise ValueError("Database connection is not established.")
        return func(self, *args, **kwargs)

    return wrapper


class DBToolSuite(ABC):
    """
    An API for interacting with Postgres via a shared connection. The connection
    is always for a specific database, and, in some cases, a specific branch.
    """

    def __init__(
        self,
        connection: _pgconn = None,
        result_collector: Optional[rc.ResultCollector] = None,
    ):
        self.conn = connection
        self.result_collector = result_collector
        if not self.result_collector:
            print("Result collector is not provided.")

    def close_connection(self) -> None:
        """
        Closes the current database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_current_connection(self) -> _pgconn:
        return self.conn

    @abstractmethod
    def get_uri_for_db_setup(self) -> str:
        """
        Returns the connection URI for database setup operations (e.g., psql).
        This is needed because psql requires a full connection URI with credentials.
        """
        pass

    ######################################################################
    # Protected methods
    ######################################################################

    @abstractmethod
    def _connect_branch_impl(self, branch_name: str) -> None:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Might raise an exception if connection fails.
        This method is timed by its caller. Don't implement additional timing.
        """
        pass

    @abstractmethod
    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """
        Creates a new branch. Might raise an exception if creation fails.
        This method is timed by its caller. Don't implement additional timing.
        """
        pass

    @abstractmethod
    def _get_current_branch_impl(self) -> Tuple[str, str]:
        """
        Returns a tuple of the current (branch_name, branch_id).
        branch_name isn't always unique and should be used for debugging/logging
        purposes only, while branch_id is needed to uniquely identify the
        current branch.
        This is used for debugging/logging so timing shouldn't matter.
        """
        pass

    def _prepare_commit(self, message: str = "") -> None:
        """
        Does any necessary preparation before committing the current list of
        changes to the database.
        This method is timed by its caller. Don't implement additional timing.
        """
        pass

    #########################################################################
    # Public methods
    #########################################################################

    def delete_db(self, db_name: str) -> None:
        """
        Deletes a database from the underlying Postgres server. This is used
        when we want to delete the db after a microbenchmark run.
        """
        query = f"DROP DATABASE IF EXISTS {db_name};"
        self.execute_sql(query)

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
        columns = self.execute_sql(query, (table_name,))

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

    #########################################################################
    # API exposed to interact with a branchable database
    #########################################################################

    @_require_connection
    def create_branch(self, branch_name: str, parent_id: str = None) -> None:
        """
        Creates a new branch. This is always timed.
        """
        try:
            with self.result_collector.maybe_time_ops(
                op_type=rslt.OpType.BRANCH_CREATE, timed=True
            ):
                self._create_branch_impl(branch_name, parent_id)
        except Exception as e:
            raise Exception(f"Error creating branch: {e}")
        self.result_collector.flush_record()

    @_require_connection
    def connect_branch(self, branch_name: str, timed: bool = False) -> None:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        """
        try:
            with self.result_collector.maybe_time_ops(
                op_type=rslt.OpType.BRANCH_CONNECT, timed=timed
            ):
                self._connect_branch_impl(branch_name)
        except Exception as e:
            raise Exception(f"Error connecting to branch: {e}")
        if timed:
            self.result_collector.flush_record()

    @_require_connection
    def get_current_branch(self) -> Tuple[str, str]:
        """
        Returns a tuple of the current (branch_name, branch_id).
        branch_name isn't always unique and should be used for debugging/logging
        purposes only, while branch_id is needed to uniquely identify the
        current branch.
        """
        return self._get_current_branch_impl()

    @_require_connection
    def commit_changes(self, timed: bool = False, message: str = "") -> None:
        """
        Commits any pending changes to the database with an optional message.
        """
        with self.result_collector.maybe_time_ops(timed, rslt.OpType.COMMIT):
            self._prepare_commit(message)
            self.conn.commit()
        if timed:
            self.result_collector.flush_record()

    @_require_connection
    def execute_sql(
        self,
        query: str,
        vars=None,
        timed: bool = False,
    ) -> list[tuple]:
        """
        Runs an SQL query in the postgres database on the current branch. The
        query could be anything supported by the underlying database. This is
        intentionally separated from commit_changes to allow for more
        fine-grained timing and multiple queries to be executed in a single
        transaction.
        """
        res = None
        try:
            with self.conn.cursor() as cur:
                # Timing both the execute and fetchall together
                op_type = rc.GetOpTypeFromSQL(query)
                with self.result_collector.maybe_time_ops(timed, op_type):
                    cur.execute(query, vars)
                    # cur.description is None for INSERT/UPDATE (no results to fetch)
                    if cur.description is not None:
                        res = cur.fetchall()
                # print(f"Executed query: {query} with vars: {vars}")
        except Exception as e:
            raise Exception(f"Error executing sql query: {query}; {vars}; {e}")
        if timed:
            # Record query with args for debugging/analysis
            query_with_args = f"{query} -- args: {vars}" if vars else query
            self.result_collector.record_sql_query(query_with_args)
            self.result_collector.flush_record()
        return res
