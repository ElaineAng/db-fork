import functools
import psycopg2
from psycopg2.extensions import connection as _pgconn
from abc import ABC, abstractmethod
from typing import Tuple, Optional

from dblib.result_collector import OpType, GetOpTypeFromSQL
import dblib.result_collector as rc


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

    ######################################################################
    # Protected methods
    ######################################################################

    def _require_connection(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not self.conn:
                raise ValueError("Database connection is not established.")
            return func(*args, **kwargs)

        return wrapper

    @abstractmethod
    def _connect_branch_impl(self, branch_name: str) -> bool:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        This method is timed by its caller. Don't implement additional timing.
        """
        pass

    @abstractmethod
    def _create_branch_impl(
        self, branch_name: str, parent_id: str = None
    ) -> bool:
        """
        Creates a new branch. Return a bool indicating whether the operation was
        successful.
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

    #########################################################################
    # API exposed to interact with a branchable database
    #########################################################################

    @_require_connection
    def create_branch(self, branch_name: str, parent_id: str = None) -> bool:
        """
        Creates a new branch. This is always timed.
        """
        done = False
        with self.result_collector.maybe_time_ops(
            op_type=OpType.CREATE_BRANCH, timed=True
        ):
            done = self._create_branch_impl(branch_name, parent_id)
        if done:
            self.result_collector.flush_record()
        return done

    @_require_connection
    def connect_branch(self, branch_name: str, timed: bool = False) -> bool:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        """
        done = False
        with self.result_collector.maybe_time_ops(
            op_type=OpType.CONNECT_BRANCH, timed=timed
        ):
            done = self._connect_branch_impl(branch_name)
        if done:
            self.result_collector.flush_record()
        return done

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
    def commit_changes(self, message: str = "", timed: bool = False) -> None:
        """
        Commits any pending changes to the database with an optional message.
        """
        with self.result_collector.maybe_time_ops(timed, OpType.COMMIT):
            self._prepare_commit(message)
            self.conn.commit()
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
                op_type = GetOpTypeFromSQL(query)
                with self.result_collector.maybe_time_ops(timed, op_type):
                    cur.execute(query, vars)
                    res = cur.fetchall()
                print(f"Executed query: {query} with vars: {vars}")
                return res
        except psycopg2.ProgrammingError:
            # No results to fetch (e.g., for INSERT/UPDATE statements).
            return res
        except Exception as e:
            raise Exception(f"Error executing sql query: {query}; {vars}; {e}")
