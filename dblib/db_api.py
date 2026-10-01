import contextvars
import functools
import time
from psycopg2.extensions import connection as _pgconn
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Union
from contextlib import asynccontextmanager

import dblib.result_collector as rc
from dblib import result_pb2 as rslt

# Type hint for async connection (optional import)
try:
    from psycopg import AsyncConnection
    _AsyncConnection = AsyncConnection
except ImportError:
    _AsyncConnection = None

# Pool connection held by the current asyncio task, if any. Multi-statement
# operations (e.g. checkout parent, then create child) pin one connection so
# every statement runs in the same session. Each task has its own context, so
# concurrent tasks never see each other's pinned connection.
_pinned_async_conn = contextvars.ContextVar("_pinned_async_conn", default=None)


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

    Supports both synchronous (psycopg2) and asynchronous (psycopg3) connections.
    """

    def __init__(
        self,
        connection: _pgconn = None,
        result_collector: Optional[rc.ResultCollector] = None,
        async_connection = None,  # Optional async connection
    ):
        self.conn = connection  # Sync connection (psycopg2)
        self.async_conn = async_connection  # Async connection (psycopg3)
        # Async connection pool, one connection per concurrent request.
        # Opened by open_async_pool().
        self.async_pool = None
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
    def get_total_storage_bytes(self) -> int:
        """Get the total storage used by the current database/branch.

        Each subclass must implement its own storage measurement strategy:
        - Directory-based (Dolt, KPG): use ``dbutil.get_directory_size_bytes()``
        - SQL-based (Neon): ``pg_database_size()`` per branch
        - Metrics API (Xata): branch-level ``disk`` metric via REST API

        Returns:
            Total storage in bytes, or 0 if unavailable.
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

    def _merge_branch_impl(
        self, source_branch: str, message: str = ""
    ) -> dict:
        """
        Merges the source branch into the current branch.
        Must already be connected to the target (destination) branch.
        This method is timed by its caller. Don't implement additional timing.

        Args:
            source_branch: Name of the branch to merge from.
            message: Optional merge commit message.

        Returns:
            A dict with backend-specific merge result info, e.g.
            {"fast_forward": bool, "conflicts": int}.
            Backends that don't support merge return an empty dict.
        """
        return {}

    def _delete_branch_impl(self, branch_name: str, branch_id: str) -> None:
        """
        Deletes a branch. Must NOT be connected to the branch being deleted.
        This method is timed by its caller. Don't implement additional timing.

        Args:
            branch_name: Name of the branch to delete.
            branch_id: Backend-specific ID of the branch to delete.
        """
        pass

    ######################################################################
    # Protected async methods (async variants of above)
    ######################################################################

    async def _connect_branch_impl_async(self, branch_name: str) -> None:
        """
        Async version of _connect_branch_impl.
        Default implementation: call sync version in thread pool.
        Backends should override for true async support.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._connect_branch_impl, branch_name)

    async def _create_branch_impl_async(
        self, branch_name: str, parent_id: str = None
    ) -> None:
        """
        Async version of _create_branch_impl.
        Default implementation: call sync version in thread pool.
        Backends should override for true async support.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._create_branch_impl, branch_name, parent_id)

    async def _get_current_branch_impl_async(self) -> Tuple[str, str]:
        """
        Async version of _get_current_branch_impl.
        Default implementation: call sync version in thread pool.
        Backends should override for true async support.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_current_branch_impl)

    async def _delete_branch_impl_async(self, branch_name: str, branch_id: str) -> None:
        """
        Async version of _delete_branch_impl.
        Default implementation: call sync version in thread pool.
        Backends should override for true async support.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._delete_branch_impl, branch_name, branch_id)

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

    def _get_table_columns(self, table_name: str) -> list[tuple]:
        """
        Returns one (column_name, type_name, is_nullable, char_max_length,
        numeric_precision, numeric_scale) row per column, in column order.
        Backends whose information_schema differs from Postgres override this.
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
        return self.execute_sql(query, (table_name,))

    def get_table_schema(self, table_name: str) -> str:
        """
        Returns the schema of a specific table in a CREATE TABLE format.
        """
        columns = self._get_table_columns(table_name)

        if not columns or len(columns) == 0:
            raise Exception(f"Error: Table '{table_name}' not found.")

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
    def create_branch(
        self, branch_name: str, parent_id: str = None, timed: bool = True, storage: bool = False
    ) -> None:
        """
        Creates a new branch.

        Args:
            branch_name: Name of the new branch.
            parent_id: ID of the parent branch to branch from.
            timed: Whether to time and record this operation (default True).
            storage: Whether to measure storage before/after this operation.
        """
        try:
            with self.result_collector.maybe_measure_ops(
                op_type=rslt.OpType.BRANCH_CREATE, timed=timed, storage=storage
            ):
                self._create_branch_impl(branch_name, parent_id)
        except Exception as e:
            raise Exception(f"Error creating branch: {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    @_require_connection
    def connect_branch(self, branch_name: str, timed: bool = False, storage: bool = False) -> None:
        """
        Connects to an existing branch to allow reading and writing data to that
        branch. Return a bool indicating whether the operation was successful.
        """
        try:
            with self.result_collector.maybe_measure_ops(
                op_type=rslt.OpType.BRANCH_CONNECT, timed=timed, storage=storage
            ):
                self._connect_branch_impl(branch_name)
        except Exception as e:
            raise Exception(f"Error connecting to branch: {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
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
    def commit_changes(self, timed: bool = False, storage: bool = False, message: str = "") -> None:
        """
        Commits any pending changes to the database with an optional message.
        """
        with self.result_collector.maybe_measure_ops(timed, rslt.OpType.COMMIT, storage=storage):
            self._prepare_commit(message)
            self.conn.commit()
        if timed:
            self.result_collector.flush_record()

    @_require_connection
    def merge_branch(
        self,
        source_branch: str,
        timed: bool = True,
        storage: bool = False,
        message: str = "",
    ) -> dict:
        """
        Merges the source branch into the currently connected branch.

        The caller must already be connected to the target branch before
        calling this method.

        Args:
            source_branch: Name of the branch to merge from.
            timed: Whether to time and record this operation.
            storage: Whether to measure storage before/after this operation.
            message: Optional merge commit message.

        Returns:
            Backend-specific merge result dict.
        """
        result = {}
        try:
            with self.result_collector.maybe_measure_ops(
                op_type=rslt.OpType.MERGE, timed=timed, storage=storage
            ):
                result = self._merge_branch_impl(source_branch, message)
        except Exception as e:
            raise Exception(f"Error merging branch '{source_branch}': {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()
        return result

    @_require_connection
    def delete_branch(
        self,
        branch_name: str,
        branch_id: str = "",
        timed: bool = True,
        storage: bool = False,
    ) -> None:
        """
        Deletes a branch. The caller must NOT be connected to the branch
        being deleted.

        Args:
            branch_name: Name of the branch to delete.
            branch_id: Backend-specific branch ID (needed for API-based backends).
            timed: Whether to time and record this operation.
            storage: Whether to measure storage before/after this operation.
        """
        try:
            with self.result_collector.maybe_measure_ops(
                op_type=rslt.OpType.BRANCH_DELETE, timed=timed, storage=storage
            ):
                self._delete_branch_impl(branch_name, branch_id)
        except Exception as e:
            raise Exception(f"Error deleting branch '{branch_name}': {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    @_require_connection
    def execute_sql(
        self,
        query: str,
        vars=None,
        timed: bool = False,
        storage: bool = False,
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
                with self.result_collector.maybe_measure_ops(timed, op_type, storage=storage):
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

    #########################################################################
    # Async public methods
    #########################################################################

    async def open_async_pool(self, conninfo: str, size: int, configure=None) -> None:
        """Open a psycopg async pool of `size` autocommit connections.

        With one connection per concurrent request, requests run in parallel
        on the server instead of queueing on a single connection.

        Args:
            conninfo: Connection URI.
            size: Number of connections (fixed; min_size == max_size).
            configure: Optional async callback run on each new connection,
                e.g. to check out the worker's branch.
        """
        from psycopg_pool import AsyncConnectionPool

        self.async_pool = AsyncConnectionPool(
            conninfo,
            min_size=size,
            max_size=size,
            kwargs={"autocommit": True},
            configure=configure,
            open=False,
        )
        await self.async_pool.open(wait=True)

    def _pool_connection(self):
        """Return an async context manager that borrows a pool connection."""
        return self.async_pool.connection()

    @asynccontextmanager
    async def _acquire_async_conn(self, record_wait: bool = False):
        """Borrow a pool connection and pin it for the current task.

        Reuses the task's pinned connection if there is one, so nested calls
        stay in the same session. When record_wait is set, the time spent
        waiting for a free connection is recorded as pool_wait_time. It is
        kept out of the op latency, which only covers the server round trip.
        """
        conn = _pinned_async_conn.get()
        if conn is not None:
            yield conn
            return
        if not self.async_pool:
            raise ValueError("Async pool not open. Cannot execute async SQL.")

        start = time.perf_counter()
        async with self._pool_connection() as conn:
            if record_wait:
                self.result_collector.record_pool_wait(time.perf_counter() - start)
            token = _pinned_async_conn.set(conn)
            try:
                yield conn
            finally:
                _pinned_async_conn.reset(token)

    async def close_connection_async(self) -> None:
        """Closes the async connection pool."""
        if self.async_pool:
            await self.async_pool.close()
            self.async_pool = None

    async def create_branch_async(
        self, branch_name: str, parent_id: str = None, timed: bool = True, storage: bool = False
    ) -> None:
        """Async version of create_branch."""
        try:
            async with self._acquire_async_conn(record_wait=timed):
                async with self._async_measure_ops(
                    op_type=rslt.OpType.BRANCH_CREATE, timed=timed, storage=storage
                ):
                    await self._create_branch_impl_async(branch_name, parent_id)
        except Exception as e:
            raise Exception(f"Error creating branch: {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    async def connect_branch_async(self, branch_name: str, timed: bool = False, storage: bool = False) -> None:
        """Async version of connect_branch."""
        try:
            async with self._acquire_async_conn(record_wait=timed):
                async with self._async_measure_ops(
                    op_type=rslt.OpType.BRANCH_CONNECT, timed=timed, storage=storage
                ):
                    await self._connect_branch_impl_async(branch_name)
        except Exception as e:
            raise Exception(f"Error connecting to branch: {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    async def get_current_branch_async(self) -> Tuple[str, str]:
        """Async version of get_current_branch."""
        return await self._get_current_branch_impl_async()

    async def delete_branch_async(
        self,
        branch_name: str,
        branch_id: str = "",
        timed: bool = True,
        storage: bool = False,
    ) -> None:
        """Async version of delete_branch."""
        try:
            async with self._acquire_async_conn(record_wait=timed):
                async with self._async_measure_ops(
                    op_type=rslt.OpType.BRANCH_DELETE, timed=timed, storage=storage
                ):
                    await self._delete_branch_impl_async(branch_name, branch_id)
        except Exception as e:
            raise Exception(f"Error deleting branch '{branch_name}': {e}")
        if timed:
            self.result_collector.record_num_keys_touched(0)
            self.result_collector.flush_record()

    async def execute_sql_async(
        self,
        query: str,
        vars=None,
        timed: bool = False,
        storage: bool = False,
    ) -> list[tuple]:
        """
        Async version of execute_sql. Runs an SQL query on a pool connection.

        The connection is acquired before the timer starts, so latency covers
        only execute + fetch. Pool wait is recorded separately.
        """
        res = None
        try:
            async with self._acquire_async_conn(record_wait=timed) as conn:
                async with conn.cursor() as cur:
                    # Timing both the execute and fetchall together
                    op_type = rc.GetOpTypeFromSQL(query)
                    async with self._async_measure_ops(timed, op_type, storage=storage):
                        await cur.execute(query, vars)
                        # cur.description is None for INSERT/UPDATE (no results to fetch)
                        if cur.description is not None:
                            res = await cur.fetchall()
        except Exception as e:
            raise Exception(f"Error executing async sql query: {query}; {vars}; {e}")
        if timed:
            # Record query with args for debugging/analysis
            query_with_args = f"{query} -- args: {vars}" if vars else query
            self.result_collector.record_sql_query(query_with_args)
            self.result_collector.flush_record()
        return res

    @asynccontextmanager
    async def _async_measure_ops(self, timed: bool, op_type: rslt.OpType, storage: bool = False):
        """
        Async context manager for measuring operation timing.
        """
        state = self.result_collector._get_thread_state()

        # Measure storage before if requested
        if storage and state.storage_fn:
            state.disk_size_before = state.storage_fn() if callable(state.storage_fn) else 0

        if not timed and not storage:
            yield
            return

        # Capture start time
        start_perf = time.perf_counter() if timed else None
        start_wall = time.time() if timed else None

        try:
            yield
        except Exception as e:
            raise e
        else:
            if timed:
                # Capture end time immediately after operation
                end_perf = time.perf_counter()
                end_wall = time.time()
                latency = end_perf - start_perf

                # Validate and set operation type
                self.result_collector._validate_and_set_op_type(op_type)

                # Record timing
                state.current_latency = latency
                state.start_time = start_wall
                state.end_time = end_wall

            # Measure storage after if requested
            if storage and state.storage_fn:
                state.disk_size_after = state.storage_fn() if callable(state.storage_fn) else 0
