from tqdm import tqdm
from util.import_db import load_sql_file
import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Self, Tuple, Optional, Callable

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node

from google.protobuf import text_format
from microbench import task_pb2 as tp
from microbench.datagen import DynamicDataGenerator
from microbench.runner_support import (
    BackendInfo,
    SharedBranchManager,
    SharedProgress,
    SharedTimer,
    validate_config,
    get_benchmark_setup,
    get_num_iterations,
    get_ops_per_thread,
)
from util import db_helpers as dbh

from dblib import result_collector as rc
from dblib.dolt import DoltToolSuite, commit_dolt_schema
from dblib.neon import NeonToolSuite
from dblib.kpg import KpgToolSuite
from dblib.file_copy import FileCopyToolSuite
from dblib.xata import XataToolSuite
from dblib.storage import StorageMeasurer


def OPS_WEIGHT(op_type: tp.OperationType):
    if op_type == tp.OperationType.BRANCH:
        return 1
    else:
        return 5


def build_branch_tree(
    root_branch: str, tree_depth: int, degree: int
) -> Tuple[Node, int]:
    root_node = Node(root_branch)
    total_branches = 1

    current_level_nodes = [root_node]
    for d in range(tree_depth):
        next_level_nodes = []
        for idx, parent_node in enumerate(current_level_nodes):
            for i in range(degree):
                branch_name = f"branch_d{d + 1}_n{idx * degree + i + 1}"
                total_branches += 1
                child_node = Node(branch_name, parent=parent_node)
                next_level_nodes.append(child_node)
        current_level_nodes = next_level_nodes

    return root_node, total_branches


def create_backend_project(config: tp.TaskConfig) -> BackendInfo:
    """Create backend-specific project and return connection info.

    This is a standalone function that can be called before creating any
    BenchmarkSuite instances. It handles project creation for cloud backends
    (NEON, XATA) and returns connection info for all backends.

    Args:
        config: Task configuration containing backend type and database setup.

    Returns:
        BackendInfo containing connection URI, branch info, and project IDs.
    """
    backend = config.backend
    db_name = config.database_setup.db_name
    require_db_setup = config.database_setup.WhichOneof("source") == "sql_dump"
    info = BackendInfo()

    if backend == tp.Backend.DOLT:
        info.default_uri = DoltToolSuite.get_default_connection_uri()
        info.default_branch_name = "main"
        print(f"Default Dolt connection URI: {info.default_uri}")

    elif backend == tp.Backend.KPG:
        info.default_uri = KpgToolSuite.get_default_connection_uri()
        info.default_branch_name = "main"
        print(f"Default KPG connection URI: {info.default_uri}")

    elif backend == tp.Backend.FILE_COPY:
        info.file_copy_info = FileCopyToolSuite.FileCopyInfo(db_name)
        info.default_uri = FileCopyToolSuite.get_default_connection_uri()
        info.default_branch_name = "main"
        print(f"Default FILE_COPY connection URI: {info.default_uri}")


    elif backend == tp.Backend.NEON:
        if require_db_setup:
            # Create a new Neon project for the benchmark.
            neon_project = NeonToolSuite.create_neon_project(
                f"project_{db_name}"
            )
            info.neon_project_id = neon_project["project"]["id"]
            info.default_uri = (
                neon_project["connection_uris"][0]["connection_uri"]
                if neon_project["connection_uris"]
                else ""
            )
            info.default_branch_id = neon_project["branch"]["id"]
            info.default_branch_name = neon_project["branch"]["name"]
            print(f"Neon project ID: {info.neon_project_id}")
            print(f"Default Neon connection URI: {info.default_uri}")
        else:
            # Reuse existing Neon project from config.
            info.neon_project_id = (
                config.database_setup.existing_db.neon_project_id
            )
            proj_branches = NeonToolSuite.get_project_branches(
                info.neon_project_id
            )
            for branch in proj_branches["branches"]:
                if branch["default"]:
                    info.default_branch_name = branch["name"]
                    info.default_branch_id = branch["id"]
                    break

    elif backend == tp.Backend.XATA:
        if require_db_setup:
            (
                xata_project_id,
                default_branch_id,
                default_branch_name,
                default_uri,
            ) = XataToolSuite.create_xata_project(f"project_{db_name}")
            info.xata_project_id = xata_project_id
            info.default_uri = default_uri
            info.default_branch_id = default_branch_id
            info.default_branch_name = default_branch_name
            print(
                f"Xata project ID: {info.xata_project_id}, "
                f"Default Xata connection URI: {info.default_uri}"
            )
        else:
            raise NotImplementedError("Xata requires database setup")

    else:
        raise ValueError(f"Unsupported backend: {backend}")

    # Create the benchmark database and load contents from a SQL dump file if required.
    if require_db_setup:
        create_benchmark_database(info.default_uri, db_name)
        # Load the database contents from a SQL dump file into the benchmark
        # database.
        db_uri = get_initial_connection_uri(config, info)
        load_sql_file(db_uri, config.database_setup.sql_dump.sql_dump_path)

        # Commit to ensure schema changes are visible for certain backends.
        if backend == tp.Backend.DOLT:
            commit_dolt_schema(db_uri)

    # Perform branch setup if the benchmark mode includes a setup block.
    setup = get_benchmark_setup(config)
    if setup and setup.num_branches > 0:
        info.setup_branches = perform_branch_setup(config, info, setup)

    return info


def get_initial_connection_uri(
    config: tp.TaskConfig, backend_info: BackendInfo
) -> str:
    """Get the connection URI for the benchmark database.

    Each backend has its own way to construct the database-specific URI.

    Args:
        config: Task configuration containing backend and database info.
        backend_info: Backend info from create_backend_project().

    Returns:
        Connection URI for the benchmark database.
    """
    db_name = config.database_setup.db_name
    backend = config.backend

    if backend == tp.Backend.DOLT:
        return DoltToolSuite.get_initial_connection_uri(db_name)

    elif backend == tp.Backend.KPG:
        return KpgToolSuite.get_initial_connection_uri(db_name)

    elif backend == tp.Backend.FILE_COPY:
        return FileCopyToolSuite.get_initial_connection_uri(db_name)

    elif backend == tp.Backend.NEON:
        return NeonToolSuite._get_neon_connection_uri(
            backend_info.neon_project_id,
            backend_info.default_branch_id,
            db_name,
        )

    elif backend == tp.Backend.XATA:
        return XataToolSuite._get_xata_connection_uri(
            backend_info.xata_project_id,
            backend_info.default_branch_id,
            db_name,
        )

    else:
        raise ValueError(f"Unsupported backend: {backend}")


def create_benchmark_database(uri: str, db_name: str) -> None:
    """Create the benchmark database on the given connection URI.

    Args:
        uri: PostgreSQL connection URI for the database server.
        db_name: Name of the database to create.
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(uri)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        create_db_command = f"CREATE DATABASE {db_name};"
        try:
            cur.execute(create_db_command)
            print("Database created successfully.")
        except psycopg2.errors.DuplicateDatabase:
            print(f"Database '{db_name}' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        if cur:
            cur.close()
            conn.close()


def cleanup_backend(
    config: tp.TaskConfig, backend_info: BackendInfo, db_name: str = None
) -> None:
    """Clean up backend-specific resources (projects, databases).

    Args:
        config: Task configuration with cleanup settings.
        backend_info: Backend info containing project IDs and connection URI.
        db_name: Name of the database to delete. If None, uses config value.
    """
    if not config.database_setup.cleanup:
        return

    db_name = db_name or config.database_setup.db_name

    # Delete the database using a direct connection through default_uri.
    if backend_info.file_copy_info:
        FileCopyToolSuite.cleanup(backend_info.file_copy_info)
    elif backend_info.default_uri and db_name:
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(backend_info.default_uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            print(f"Database '{db_name}' deleted successfully.")
        except Exception as e:
            print(f"Error deleting database: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    # Delete backend-specific project if applicable.
    if backend_info.neon_project_id:
        NeonToolSuite.delete_project(backend_info.neon_project_id)
    elif backend_info.xata_project_id:
        XataToolSuite.delete_project(backend_info.xata_project_id)


def perform_branch_setup(
    config: tp.TaskConfig,
    backend_info: BackendInfo,
    setup: tp.BenchmarkSetup,
):
    """Create branches and perform setup operations before measurement.

    Shared by nth_op and throughput benchmark modes. Creates branches
    according to the specified shape and performs inserts, updates,
    and deletes on each branch in a randomized order.

    Args:
        config: Task configuration (for backend, db_name, etc.).
        backend_info: Backend info from create_backend_project().
        setup: BenchmarkSetup message with branch count, shape, and per-branch ops.
    """
    num_branches = setup.num_branches
    shape = setup.branch_shape
    inserts_per_branch = setup.inserts_per_branch
    updates_per_branch = setup.updates_per_branch
    deletes_per_branch = setup.deletes_per_branch

    print(
        f"Nth-op setup: Creating {num_branches} branches "
        f"shape: {shape} "
        f"with {inserts_per_branch} inserts, {updates_per_branch} updates, "
        f"{deletes_per_branch} deletes per branch..."
    )

    # Create a temporary BenchmarkSuite for setup operations.
    # Branch creation is timed and saved to a separate _setup.parquet file.
    setup_result_collector = rc.ResultCollector(
        run_id=f"{config.run_id}_setup",
    )
    setup_branch_manager = SharedBranchManager()

    with BenchmarkSuite(
        config,
        backend_info,
        seed=42,  # Fixed seed for reproducible setup
        thread_id=0,
        result_collector=setup_result_collector,
        branch_manager=setup_branch_manager,
    ) as setup_bench:
        last_branch_name, last_branch_id = setup_bench.setup_nth_op_branches(
            num_branches,
            shape,
            inserts_per_branch,
            updates_per_branch,
            deletes_per_branch,
        )

    # Store the last branch info in BackendInfo for measurement phase.
    backend_info.default_branch_name = last_branch_name
    backend_info.default_branch_id = last_branch_id

    # Get all branches created during setup to pass to worker threads.
    setup_branches = setup_branch_manager.get_all_branches()

    # Save setup timing data to a separate parquet file.
    setup_result_collector.write_to_parquet(
        filename=f"{config.run_id}_setup.parquet"
    )

    print(
        f"Nth-op setup complete. Created {num_branches} branches. "
        f"Last branch: {last_branch_name}"
    )

    return setup_branches


class BenchmarkSuite:
    _DEFAULT_OP_RUNNERS: dict[tp.OperationType, str] = {
        tp.OperationType.BRANCH: "_run_op_branch_and_connect",
        tp.OperationType.READ: "_run_op_read",
        tp.OperationType.INSERT: "_run_op_insert",
        tp.OperationType.UPDATE: "_run_op_update",
        tp.OperationType.RANGE_UPDATE: "_run_op_range_update",
        tp.OperationType.RANGE_READ: "_run_op_range_read",
        tp.OperationType.CONNECT: "_run_op_connect",
    }

    def __init__(
        self,
        config: tp.TaskConfig,
        backend_info: BackendInfo,
        seed: int = None,
        thread_id: int = 0,
        result_collector: Optional[rc.ResultCollector] = None,
        branch_manager: Optional[SharedBranchManager] = None,
        shared_progress: Optional[SharedProgress] = None,
        shared_timer: Optional[SharedTimer] = None,
        assigned_branches: Optional[list] = None,
    ):
        self._db_name = config.database_setup.db_name
        self._config = config
        self._backend_info = backend_info
        self._seed = seed  # Optional seed for reproducibility
        self._thread_id = thread_id
        self._shared_result_collector = result_collector
        self._shared_branch_manager = branch_manager
        self._shared_progress = shared_progress
        self._shared_timer = shared_timer
        self._assigned_branches = (
            assigned_branches or []
        )  # Per-thread exclusive branches
        self._require_db_setup = (
            config.database_setup.WhichOneof("source") == "sql_dump"
        )
        self._measure_storage = config.measure_storage

        # Mapping between table name and data generator.
        self._table_datagen = None

        # List of all branches created.
        self._all_branches = []

        # Cached keys to read from.

        # Mapping from branch ID to list of modified keys by this benchmark.
        self._modified_keys = {}

        # List of existing primary keys in the database for the current branch.
        # Not used currently. Technically we don't need to always re-read the
        # existing primary keys.
        self._existing_pks = []

        # Cache for pk columns.
        self._pk_columns = []

    def _add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared branch list."""
        self._shared_branch_manager.add_branch(branch_name)

    def _get_random_branch(self, rnd) -> Optional[str]:
        """Get a random branch from the shared branch list."""
        return self._shared_branch_manager.get_random_branch(rnd)

    def __enter__(self) -> Self:
        db_tools = None
        # Use shared result collector if provided (for worker threads),
        # otherwise create a new one.
        assert self._shared_result_collector is not None, (
            "Shared result collector must be provided for worker threads"
        )
        result_collector = self._shared_result_collector

        try:
            # Use backend info passed to constructor.
            default_branch_id = self._backend_info.default_branch_id
            self._root_branch_name = self._backend_info.default_branch_name

            # Initialize the appropriate db_tools for the backend.
            if self._config.backend == tp.Backend.DOLT:
                db_tools = DoltToolSuite.init_for_bench(
                    result_collector,
                    self._db_name,
                    self._config.autocommit,
                    self._backend_info.default_branch_name,
                )
            elif self._config.backend == tp.Backend.KPG:
                db_tools = KpgToolSuite.init_for_bench(
                    result_collector, self._db_name, self._config.autocommit
                )
            elif self._config.backend == tp.Backend.FILE_COPY:
                db_tools = FileCopyToolSuite.init_for_bench(
                    result_collector,
                    self._db_name,
                    self._config.autocommit,
                    self._backend_info.default_branch_name,
                    self._backend_info.file_copy_info.branches,
                )
            elif self._config.backend == tp.Backend.NEON:
                print(
                    f"Default Neon branch name: {self._root_branch_name}, "
                    f"ID: {default_branch_id}"
                )
                db_tools = NeonToolSuite.init_for_bench(
                    result_collector,
                    self._backend_info.neon_project_id,
                    default_branch_id,
                    self._root_branch_name,
                    self._db_name,
                    self._config.autocommit,
                )
            elif self._config.backend == tp.Backend.XATA:
                db_tools = XataToolSuite.init_for_bench(
                    result_collector,
                    self._backend_info.xata_project_id,
                    default_branch_id,
                    self._root_branch_name,
                    self._db_name,
                    self._config.autocommit,
                )
            else:
                raise ValueError(f"Unsupported backend: {self._config.backend}")

            self._add_branch(self._root_branch_name)
            self.db_tools = db_tools
            self._storage = StorageMeasurer(db_tools, self._measure_storage)

            # If this thread has assigned branches (FAN_OUT mode), connect to the first one
            if self._assigned_branches:
                initial_branch = self._assigned_branches[0]
                if self._thread_id == 0:
                    print(
                        f"Thread {self._thread_id} connecting to assigned branch: {initial_branch}"
                    )
                self.db_tools.connect_branch(initial_branch, timed=False)

            return self

        except Exception as e:
            print(f"Error during BenchmarkSuite setup: {e}")
            # Clean up any created projects on error.
            if self._config.database_setup.cleanup and self._backend_info:
                if self._backend_info.neon_project_id:
                    NeonToolSuite.delete_project(
                        self._backend_info.neon_project_id
                    )
                elif self._backend_info.xata_project_id:
                    XataToolSuite.delete_project(
                        self._backend_info.xata_project_id
                    )
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        # We do parquet writing in the main thread.
        # Close the database connection.
        # NOTE: _cleanup_backend() should be called separately by the main
        # thread after all worker threads have finished.
        self.db_tools.close_connection()

    def maybe_branch_and_reconnect(self, next_bid, rnd) -> None:
        cur_name, cur_id = self.db_tools.get_current_branch()
        # Check shared branch limit status. This is still buggy since there's a
        # extended period between reading the limit and checking if it's reached.
        # Multiple threads can be checking the limit before it's updated and all
        # decide that a limit isn't reached.
        # TODO: Fix the race here. Low priority.
        branch_limit_reached = (
            self._shared_branch_manager.is_branch_limit_reached()
        )
        if not branch_limit_reached:
            next_branch_name = f"branch_tid{self._thread_id}_{next_bid}"
            with self._storage.measure():
                self.db_tools.create_branch(
                    branch_name=next_branch_name, parent_id=cur_id
                )
            self.db_tools.result_collector.record_num_keys_touched(0)
            self.db_tools.result_collector.flush_record()
            self._add_branch(next_branch_name)

            # Toss a fair coin to connect to the new branch, or stay on the
            # current branch.
            if rnd.random() < 0.5:
                self.db_tools.connect_branch(next_branch_name, timed=True)
                # clear existing pks cache if switching to a different branch.
                self._existing_pks = []
        elif rnd.random() < 0.25:
            # 1/4 chance to connect to a random branch.
            to_connect = self._get_random_branch(rnd)
            if to_connect:
                self.db_tools.connect_branch(to_connect, timed=True)
                # clear existing pks cache if switing to a different branch.
                self._existing_pks = []

    def branch_and_connect(self, next_bid: int) -> str:
        """Create a new branch and connect to it, timing both operations.

        This is used for deterministic Nth-op benchmarks where we always want
        to create and connect to measure the combined latency.

        Args:
            next_bid: The branch ID number to use for naming.

        Returns:
            The name of the newly created branch.
        """
        _, cur_id = self.db_tools.get_current_branch()
        next_branch_name = f"branch_tid{self._thread_id}_{next_bid}"

        # Create branch (timed, with optional storage measurement)
        with self._storage.measure():
            self.db_tools.create_branch(
                branch_name=next_branch_name, parent_id=cur_id, timed=True
            )
        self.db_tools.result_collector.record_num_keys_touched(0)
        self.db_tools.result_collector.flush_record()
        self._add_branch(next_branch_name)

        # Connect to the new branch (timed)
        self.db_tools.connect_branch(next_branch_name, timed=True)
        # Clear existing pks cache since we're on a new branch
        self._existing_pks = []

        return next_branch_name

    def connect_to_branch(self, rnd) -> None:
        """Connect to a random existing branch (timed).

        This is used for deterministic Nth-op benchmarks to measure
        the cost of connecting to an existing branch.

        Args:
            rnd: Random instance for selecting branch.
        """
        to_connect = self._get_random_branch(rnd)
        if to_connect:
            self.db_tools.connect_branch(to_connect, timed=True)
            # Clear existing pks cache since we're on a different branch
            self._existing_pks = []
        else:
            raise ValueError("No branches available to connect to")

    def _select_random_key(self, rnd, benchmark_table):
        """Select a random key from existing PKs or modified keys.

        Returns:
            Tuple of (cur_branch_id, pk_columns, selected_key) or
            (cur_branch_id, pk_columns, None) if no keys are available.
        """
        _, cur_branch_id = self.db_tools.get_current_branch()

        self.maybe_load_pk_columns(benchmark_table)
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(),
            benchmark_table,
            self._pk_columns,
        )
        # Cache the fetched PKs for subsequent operations.
        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        selected_key = None
        read_from_modified = (
            self._modified_keys.get(cur_branch_id) and rnd.random() < 0.5
        )
        if read_from_modified:
            selected_key = rnd.choice(self._modified_keys[cur_branch_id])
        else:
            selected_key = rnd.choice(existing_pks)

        return cur_branch_id, selected_key

    def read_op(self, rnd, benchmark_table):
        _, key_to_read = self._select_random_key(rnd, benchmark_table)

        if not key_to_read:
            raise ValueError("No existing keys found during read, do nothing")

        # Build the SQL query to read the key.
        where_clause = " AND ".join(
            [f"{pk_name} = %s" for pk_name in self._pk_columns]
        )
        select_sql = f"SELECT * FROM {benchmark_table} WHERE {where_clause};"

        # Read only touches a single key. We might be able to set this in
        # execute_sql() but doing it here is easier.
        self.db_tools.result_collector.record_num_keys_touched(1)

        # Run the read.
        res = self.db_tools.execute_sql(select_sql, key_to_read, timed=True)
        if not res:
            raise ValueError("Read failed, do nothing")

    def maybe_load_pk_columns(self, benchmark_table):
        if not self._pk_columns:
            self._pk_columns = dbh.get_pk_column_names(
                self.db_tools.get_current_connection(), benchmark_table
            )

    def insert_op(self, benchmark_table) -> bool:
        _, cur_branch_id = self.db_tools.get_current_branch()

        col_names = dbh.get_all_columns(
            self.db_tools.get_current_connection(), benchmark_table
        )
        self.maybe_load_pk_columns(benchmark_table)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {benchmark_table} ({', '.join(col_names)}) VALUES ({placeholders});"

        inserted = False

        # Pre-record the number of keys for this op.
        self.db_tools.result_collector.record_num_keys_touched(1)
        for _ in range(5):
            if inserted:
                break
            # Generate a new row. Note that this is using a data generator that
            # isn't initialized with the current seed. But this should be fine
            # since we shouldn't care about the exact values inserted.
            row_data = self._table_datagen.generate_row()
            pk_tuple = tuple(row_data[pk] for pk in self._pk_columns)

            # Try to insert it, it may fail for PK collision.
            try:
                self.db_tools.execute_sql(insert_sql, row_data, timed=True)
                self._modified_keys.setdefault(cur_branch_id, []).append(
                    pk_tuple
                )
                inserted = True
                if not self.db_tools.autocommit:
                    self.db_tools.commit_changes(timed=True, message="insert")
                break
            except Exception:
                continue

    def _execute_timed_op(self, sql, row_data, commit_message):
        """Execute SQL with timing and optional storage measurement.

        Bypasses execute_sql(timed=True) auto-flush so disk_size_before and
        disk_size_after land on the same record.
        """

        result_collector = self.db_tools.result_collector
        db_tools = self.db_tools

        with self._storage.measure():
            op_type = rc.GetOpTypeFromSQL(sql)
            with result_collector.maybe_time_ops(timed=True, op_type=op_type):
                db_tools.execute_sql(sql, row_data, timed=False)
                if not db_tools.autocommit:
                    db_tools.commit_changes(timed=False, message=commit_message)
        result_collector.record_sql_query(f"{sql} -- args: {row_data}")
        result_collector.flush_record()

    def update_op(self, rnd, benchmark_table, timed: bool = True) -> None:
        """Update a random row in the table.

        Args:
            rnd: Random instance for selecting keys.
            benchmark_table: Table to update.
            timed: If True, record timing metrics; if False, skip timing (setup phase).
        """
        cur_branch_id, key_to_update = self._select_random_key(
            rnd, benchmark_table
        )

        if not key_to_update:
            if timed:
                raise ValueError(
                    "No existing keys found during update, do nothing"
                )
            return  # Silently skip during setup

        # Get all columns and filter out PK columns to get updatable columns.
        all_columns = dbh.get_all_columns(
            self.db_tools.get_current_connection(), benchmark_table
        )
        non_pk_columns = [
            col for col in all_columns if col not in self._pk_columns
        ]

        if not non_pk_columns:
            if timed:
                raise ValueError("No non-PK columns to update")
            return  # Silently skip during setup

        # Generate new values for non-PK columns.
        row_data = self._table_datagen.generate_row()

        # Build the SET clause for non-PK columns.
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])

        # Build the WHERE clause using PK columns.
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in self._pk_columns]
        )

        update_sql = (
            f"UPDATE {benchmark_table} SET {set_clause} WHERE {where_clause};"
        )

        # Add PK values to the row_data for the WHERE clause.
        for i, pk_col in enumerate(self._pk_columns):
            row_data[pk_col] = key_to_update[i]

        # Update only touches a single key.
        if timed:
            self.db_tools.result_collector.record_num_keys_touched(1)

        # Run the update.
        if timed:
            self._execute_timed_op(update_sql, row_data, "update")
        else:
            # NOTE: Never call execute_sql(timed=True) directly — its auto-flush
            # conflicts with StorageMeasurer. Use _execute_timed_op for timed ops.
            self.db_tools.execute_sql(update_sql, row_data, timed=False)

        # Track the modified key.
        if key_to_update not in self._modified_keys.get(cur_branch_id, []):
            self._modified_keys.setdefault(cur_branch_id, []).append(
                key_to_update
            )

    def range_update_op(self, rnd, benchmark_table) -> None:
        """Perform a range update on multiple rows.

        Selects two random keys to bound the range. The number of rows between
        the two keys is approximately config.range_update_config.range_size.

        Uses the first PK column for the range condition. Note that for tables
        with composite primary keys, the actual number of rows updated may exceed
        the specified range size since we only constrain the first PK column.

        Args:
            rnd: Random module or object with random() and choice() methods.

        Returns:
            The actual number of rows updated, or 0 if the update failed.
        """
        _, cur_branch_id = self.db_tools.get_current_branch()

        # Get range query components from helper.
        range_info = self._prepare_range_query(
            rnd, benchmark_table, "range update"
        )

        # Get all columns and filter out PK columns for the SET clause.
        all_columns = dbh.get_all_columns(
            self.db_tools.get_current_connection(), benchmark_table
        )
        non_pk_columns = [
            col for col in all_columns if col not in self._pk_columns
        ]

        if not non_pk_columns:
            raise ValueError("No non-PK columns to update")

        # Generate new values for non-PK columns.
        row_data = self._table_datagen.generate_row()

        # Build the SET clause for non-PK columns.
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])

        # Add range params to row_data.
        row_data.update(range_info["params"])

        update_sql = f"UPDATE {benchmark_table} SET {set_clause} WHERE {range_info['where_clause']};"

        # Record the accurate number of keys touched.
        self.db_tools.result_collector.record_num_keys_touched(
            len(range_info["keys_in_range"])
        )

        # Run the range update.
        self._execute_timed_op(update_sql, row_data, "range update")

        # Track all actual keys in the range as modified.
        modified_list = self._modified_keys.setdefault(cur_branch_id, [])
        for key in range_info["keys_in_range"]:
            if key not in modified_list:
                modified_list.append(key)
        return len(range_info["keys_in_range"])

    def range_read_op(self, rnd, benchmark_table) -> int:
        """Perform a range read on multiple rows.

        Selects two random keys to bound the range. The number of rows between
        the two keys is approximately config.range_update_config.range_size.

        Uses the first PK column for the range condition. Note that for tables
        with composite primary keys, the actual number of rows read may exceed
        the specified range size since we only constrain the first PK column.

        Args:
            rnd: Random module or object with random() and choice() methods.
            benchmark_table: Table to read from.

        Returns:
            The actual number of rows read, or 0 if the read failed.
        """
        # Get range query components from helper.
        range_info = self._prepare_range_query(
            rnd, benchmark_table, "range read"
        )

        select_sql = f"SELECT * FROM {benchmark_table} WHERE {range_info['where_clause']};"

        # Record the accurate number of keys touched.
        self.db_tools.result_collector.record_num_keys_touched(
            len(range_info["keys_in_range"])
        )

        # Run the range read.
        self.db_tools.execute_sql(select_sql, range_info["params"], timed=True)

        return len(range_info["keys_in_range"])

    def _prepare_range_query(
        self, rnd, benchmark_table: str, operation_name: str
    ) -> dict:
        """Prepare common components for range queries (read or update).

        Args:
            rnd: Random module or object with random() and choice() methods.
            benchmark_table: Table to query.
            operation_name: Name of operation for error messages.

        Returns:
            Dictionary with:
                - keys_in_range: List of PK tuples in the selected range.
                - where_clause: SQL WHERE clause for the range.
                - params: Dictionary of query parameters for the range bounds.
        """
        self.maybe_load_pk_columns(benchmark_table)

        # Get all PK values sorted by all PK columns to determine range bounds.
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(),
            benchmark_table,
            self._pk_columns,
        )
        # Cache the fetched PKs for subsequent operations.
        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        if not existing_pks:
            raise ValueError(
                f"No existing keys found during {operation_name}, do nothing"
            )

        # Sort by all PK columns (tuple comparison).
        sorted_pks = sorted(existing_pks)
        total_keys = len(sorted_pks)

        # Get range size from config.
        range_size = self._config.range_update_config.range_size or 10
        range_size = min(range_size, total_keys)  # Can't exceed total keys

        # Pick a random start index, ensuring we have room for range_size keys.
        max_start_idx = max(0, total_keys - range_size)
        start_idx = rnd.randint(0, max_start_idx)
        end_idx = min(start_idx + range_size - 1, total_keys - 1)

        start_key = sorted_pks[start_idx]
        end_key = sorted_pks[end_idx]

        # Keys in range are exactly the slice from sorted_pks.
        keys_in_range = sorted_pks[start_idx : end_idx + 1]

        # Build tuple comparison for composite PK range queries.
        # Uses SQL row value comparison: (col1, col2, ...) >= (val1, val2, ...)
        pk_tuple_sql = f"({', '.join(self._pk_columns)})"
        placeholders_start = ", ".join(
            [f"%(_start_{i})s" for i in range(len(self._pk_columns))]
        )
        placeholders_end = ", ".join(
            [f"%(_end_{i})s" for i in range(len(self._pk_columns))]
        )

        # Build params dict for the range bounds.
        params = {}
        for i, val in enumerate(start_key):
            params[f"_start_{i}"] = val
        for i, val in enumerate(end_key):
            params[f"_end_{i}"] = val

        # Build WHERE clause using tuple comparison on all PK columns.
        where_clause = f"{pk_tuple_sql} >= ({placeholders_start}) AND {pk_tuple_sql} <= ({placeholders_end})"

        return {
            "keys_in_range": keys_in_range,
            "where_clause": where_clause,
            "params": params,
        }

    def setup_nth_op_branches(
        self,
        num_branches: int,
        shape: tp.BranchShape,
        inserts_per_branch: int,
        updates_per_branch: int = 0,
        deletes_per_branch: int = 0,
    ):
        """Setup branches and data for Nth operation measurement.

        Creates the specified number of branches according to the shape pattern
        and performs inserts, updates, and deletes on each branch. Operations
        are executed in a randomized order based on the seed.

        Args:
            num_branches: Number of branches to create.
            shape: SPINE (linear chain) or FAN_OUT (all from root).
            inserts_per_branch: Number of inserts per branch.
            updates_per_branch: Number of updates per branch.
            deletes_per_branch: Number of deletes per branch.
        """
        # Initialize RNG with seed for reproducible operation ordering.
        rnd = random.Random(self._seed)
        db_tools = self.db_tools

        # Initialize datagen for inserts.
        benchmark_table = self._config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(
                db_tools.get_current_connection()
            )
            benchmark_table = rnd.choice(all_tables)

        table_schema = db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(
                f"Could not fetch DDL for table {benchmark_table}."
            )

        self._table_datagen = DynamicDataGenerator(table_schema)
        self.maybe_load_pk_columns(benchmark_table)

        # Get column info for inserts.
        col_names = dbh.get_all_columns(
            db_tools.get_current_connection(), benchmark_table
        )

        _, current_parent_id = db_tools.get_current_branch()
        root_branch_id = current_parent_id

        # Track branch IDs for BUSHY shape (random parent selection).
        branch_ids = [(self._root_branch_name, root_branch_id)]

        # Perform setup operations on root branch.
        print(
            f"Performing setup ops on root branch: "
            f"{inserts_per_branch} inserts, {updates_per_branch} updates, "
            f"{deletes_per_branch} deletes..."
        )
        self._perform_branch_setup_ops(
            rnd,
            benchmark_table,
            col_names,
            inserts_per_branch,
            updates_per_branch,
            deletes_per_branch,
        )

        for i in tqdm(range(num_branches)):
            branch_name = f"setup_branch_{i + 1}"

            if shape == tp.BranchShape.SPINE:
                # Linear: branch from current
                with self._storage.measure():
                    db_tools.create_branch(
                        branch_name, current_parent_id, timed=True
                    )
                db_tools.result_collector.record_num_keys_touched(0)
                db_tools.result_collector.flush_record()
                db_tools.connect_branch(branch_name, timed=False)
                _, current_parent_id = db_tools.get_current_branch()
                branch_ids.append((branch_name, current_parent_id))
            elif shape == tp.BranchShape.FAN_OUT:
                # Fan-out: always branch from root
                with self._storage.measure():
                    db_tools.create_branch(
                        branch_name, root_branch_id, timed=True
                    )
                db_tools.result_collector.record_num_keys_touched(0)
                db_tools.result_collector.flush_record()
                db_tools.connect_branch(branch_name, timed=False)
                _, new_branch_id = db_tools.get_current_branch()
                branch_ids.append((branch_name, new_branch_id))
            else:  # BUSHY
                # Bushy: branch from a random existing branch
                parent_name, parent_id = rnd.choice(branch_ids)
                with self._storage.measure():
                    db_tools.create_branch(branch_name, parent_id, timed=True)
                db_tools.result_collector.record_num_keys_touched(0)
                db_tools.result_collector.flush_record()
                db_tools.connect_branch(branch_name, timed=False)
                _, new_branch_id = db_tools.get_current_branch()
                branch_ids.append((branch_name, new_branch_id))

            # Perform setup operations on this branch.
            self._perform_branch_setup_ops(
                rnd,
                benchmark_table,
                col_names,
                inserts_per_branch,
                updates_per_branch,
                deletes_per_branch,
            )

            self._add_branch(branch_name)

        # Get the last branch info to return.
        last_branch_name, last_branch_id = db_tools.get_current_branch()

        print(
            f"Setup complete: {num_branches} branches created, "
            f"{inserts_per_branch} inserts, {updates_per_branch} updates, "
            f"{deletes_per_branch} deletes per branch."
        )

        return last_branch_name, last_branch_id

    def _perform_branch_setup_ops(
        self,
        rnd: random.Random,
        benchmark_table: str,
        col_names: list,
        inserts: int,
        updates: int,
        deletes: int,
    ):
        """Perform inserts, updates, and deletes on the current branch during setup.

        Inserts are performed first to ensure there is data to update/delete.
        Then updates and deletes are shuffled and performed in random order.

        Args:
            rnd: Random instance for shuffling operations.
            benchmark_table: Table to operate on.
            col_names: Column names for the table.
            inserts: Number of inserts to perform.
            updates: Number of updates to perform.
            deletes: Number of deletes to perform.
        """
        # First, perform all inserts to ensure we have data.
        for _ in range(inserts):
            self._insert_without_timing(benchmark_table, col_names)

        # Commit inserts if not autocommit.
        if not self.db_tools.autocommit:
            self.db_tools.commit_changes(timed=False, message="setup_inserts")

        # Build a shuffled list of updates and deletes.
        ops = ["update"] * updates + ["delete"] * deletes
        rnd.shuffle(ops)

        # Perform updates and deletes in random order.
        for op in ops:
            if op == "update":
                self.update_op(rnd, benchmark_table, timed=False)
            else:  # delete
                self._delete_without_timing(rnd, benchmark_table)

        # Commit updates/deletes if not autocommit.
        if not self.db_tools.autocommit and (updates > 0 or deletes > 0):
            self.db_tools.commit_changes(
                timed=False, message="setup_updates_deletes"
            )

    def _insert_without_timing(self, benchmark_table: str, col_names: list):
        """Insert a row without timing (used during setup phase)."""
        _, cur_branch_id = self.db_tools.get_current_branch()

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {benchmark_table} ({', '.join(col_names)}) VALUES ({placeholders});"
        inserted = False
        error_msg = ""
        for _ in range(5):
            row_data = self._table_datagen.generate_row()
            pk_tuple = tuple(row_data[pk] for pk in self._pk_columns)

            try:
                # Execute without timing.
                self.db_tools.execute_sql(insert_sql, row_data, timed=False)
                self._modified_keys.setdefault(cur_branch_id, []).append(
                    pk_tuple
                )
                inserted = True
                break
            except Exception as e:
                error_msg = str(e)
                continue
        assert inserted, f"Failed to insert row: {error_msg}"

    def _delete_without_timing(
        self, rnd: random.Random, benchmark_table: str
    ) -> None:
        """Delete a random row without timing (used during setup phase)."""
        cur_branch_id, key_to_delete = self._select_random_key(
            rnd, benchmark_table
        )

        if not key_to_delete:
            # No keys to delete, skip this operation
            return

        # Build the WHERE clause using PK columns.
        where_clause = " AND ".join(
            [f"{pk_name} = %s" for pk_name in self._pk_columns]
        )

        delete_sql = f"DELETE FROM {benchmark_table} WHERE {where_clause};"

        # Run the delete without timing.
        self.db_tools.execute_sql(delete_sql, key_to_delete, timed=False)

        # Remove the key from modified_keys if present.
        if cur_branch_id in self._modified_keys:
            if key_to_delete in self._modified_keys[cur_branch_id]:
                self._modified_keys[cur_branch_id].remove(key_to_delete)

    # ---------------------------------------------------------------
    # Shared benchmark helpers (DRY)
    # ---------------------------------------------------------------
    def _prepare_benchmark(self):
        """Common setup for all benchmark modes.

        Resolves the benchmark table, loads schema and datagen, creates
        a seeded RNG, measures initial DB size, and sets ResultCollector
        context.

        Returns:
            (benchmark_table, rnd) — the resolved table name and a
            thread-local Random instance.
        """
        benchmark_table = self._config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(
                self.db_tools.get_current_connection()
            )
            benchmark_table = random.choice(all_tables)

        table_schema = self.db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(
                f"Could not fetch DDL for table {benchmark_table}."
            )

        self._table_datagen = DynamicDataGenerator(table_schema)

        seed = self._seed if self._seed is not None else int(time.time())
        rnd = random.Random(seed)
        if self._thread_id == 0:
            print(f"Thread {self._thread_id} using seed: {seed}")

        initial_db_size = 0
        try:
            initial_db_size = dbh.get_db_size(
                self.db_tools.get_current_connection()
            )
        except Exception as e:
            if self._thread_id == 0:
                print(f"Error getting initial DB size: {e}")

        self.db_tools.result_collector.set_context(
            table_name=benchmark_table,
            table_schema=table_schema,
            initial_db_size=initial_db_size,
            seed=seed,
        )

        return benchmark_table, rnd

    def _run_op_branch_and_connect(
        self, _rnd: random.Random, _benchmark_table: str
    ) -> None:
        next_bid = self._shared_branch_manager.get_next_branch_id()
        self.branch_and_connect(next_bid)

    def _run_op_branch_maybe_reconnect(
        self, rnd: random.Random, _benchmark_table: str
    ) -> None:
        next_bid = self._shared_branch_manager.get_next_branch_id()
        try:
            self.maybe_branch_and_reconnect(next_bid, rnd)
        except Exception as e:
            if "branches limit exceeded" in str(e):
                self._shared_branch_manager.set_branch_limit_reached()
            raise

    def _run_op_read(self, rnd: random.Random, benchmark_table: str) -> None:
        self.read_op(rnd, benchmark_table)

    def _run_op_insert(self, _rnd: random.Random, benchmark_table: str) -> None:
        self.insert_op(benchmark_table)

    def _run_op_update(self, rnd: random.Random, benchmark_table: str) -> None:
        self.update_op(rnd, benchmark_table)

    def _run_op_range_update(
        self, rnd: random.Random, benchmark_table: str
    ) -> None:
        self.range_update_op(rnd, benchmark_table)

    def _run_op_range_read(
        self, rnd: random.Random, benchmark_table: str
    ) -> None:
        self.range_read_op(rnd, benchmark_table)

    def _run_op_connect(
        self, rnd: random.Random, _benchmark_table: str
    ) -> None:
        self.connect_to_branch(rnd)

    def _execute_op(
        self,
        op: tp.OperationType,
        rnd: random.Random,
        benchmark_table: str,
        overrides: Optional[dict[tp.OperationType, str]] = None,
    ) -> None:
        """Execute one operation using map-based dispatch.

        Args:
            op: OperationType to execute.
            rnd: Thread-local RNG.
            benchmark_table: Target benchmark table.
            overrides: Optional op runner overrides by operation type.
        """
        runner_name = overrides.get(op) if overrides else None
        if not runner_name:
            runner_name = self._DEFAULT_OP_RUNNERS.get(op)
        if not runner_name:
            raise ValueError(f"Unsupported operation type: {op}")
        runner: Callable[[random.Random, str], None] = getattr(self, runner_name)
        runner(rnd, benchmark_table)

    def _report_op_error(self, error: Exception) -> None:
        msg = f"[Thread {self._thread_id}] Error performing operation: {error}"
        if self._shared_progress:
            self._shared_progress.write(msg)
        else:
            tqdm.write(msg)

    # ---------------------------------------------------------------
    # Benchmark modes
    # ---------------------------------------------------------------
    def run_nth_op_benchmark(self):
        """Measure the cost of a single Nth operation.

        The setup (branches and data) has already been created by
        perform_branch_setup(), and we should already be connected
        to the latest created branch.
        """
        benchmark_table, rnd = self._prepare_benchmark()

        op = self._config.nth_op_benchmark.operation
        num_ops = self._config.nth_op_benchmark.num_ops or 1

        for i in range(num_ops):
            self._execute_op(op, rnd, benchmark_table)
            if self._shared_progress:
                self._shared_progress.update(1)

    def run_randomized_avg_benchmark(self):
        """Run randomized average-cost benchmark with weighted operation mix."""
        benchmark_table, rnd = self._prepare_benchmark()

        randomized_config = self._config.randomized_benchmark
        all_operations = randomized_config.operations
        ops_weights = [OPS_WEIGHT(op) for op in all_operations]
        randomized_overrides = {
            tp.OperationType.BRANCH: "_run_op_branch_maybe_reconnect"
        }

        for _ in range(randomized_config.num_ops):
            cur_ops = rnd.choices(all_operations, ops_weights)[0]
            try:
                self._execute_op(
                    cur_ops,
                    rnd,
                    benchmark_table,
                    overrides=randomized_overrides,
                )
            except Exception as e:
                self._report_op_error(e)

            if self._shared_progress:
                self._shared_progress.update(1)

    def run_throughput_benchmark(self):
        """Run operations for a fixed wall-clock duration, measuring goodput.

        Used for Experiment 3:
          3a (branch creation throughput): operations = [BRANCH]
          3b (CRUD throughput under branching): operations = [READ, UPDATE, ...]

        Goodput (ops/sec) is computed in post-processing from the parquet rows.
        """
        benchmark_table, rnd = self._prepare_benchmark()

        tc = self._config.throughput_benchmark
        all_operations = list(tc.operations)
        ops_weights = [OPS_WEIGHT(op) for op in all_operations]

        while self._shared_timer.should_continue():
            op = rnd.choices(all_operations, ops_weights)[0]
            try:
                self._execute_op(op, rnd, benchmark_table)
            except Exception as e:
                self._report_op_error(e)

    # Dispatch table: benchmark_mode field name → method name
    _BENCHMARK_RUNNERS = {
        "nth_op_benchmark": "run_nth_op_benchmark",
        "randomized_benchmark": "run_randomized_avg_benchmark",
        "throughput_benchmark": "run_throughput_benchmark",
    }

    def run_benchmark(self):
        mode = self._config.WhichOneof("benchmark_mode")
        runner_name = self._BENCHMARK_RUNNERS.get(mode)
        if not runner_name:
            raise ValueError(f"Unknown benchmark mode: {mode}")
        getattr(self, runner_name)()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Run database benchmarks from config file."
    )

    parser.add_argument(
        "--config",
        type=str,
        default="microbench/test_config.textproto",
        help="Path to the task configuration file (textproto format).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible benchmark operations. If not specified, uses current timestamp.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar (useful for running in background/tmux).",
    )
    args = parser.parse_args()

    # Load and parse the textproto config file
    try:
        config = tp.TaskConfig()
        with open(args.config, "r") as f:
            text_format.Parse(f.read(), config)

        print(f"Loaded configuration from {args.config}")
        print(f"Run ID: {config.run_id}")
        print(f"Backend: {tp.Backend.Name(config.backend)}")
        print(f"Benchmark mode: {config.WhichOneof('benchmark_mode')}")
        print(f"Table name: {config.table_name}")

    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)

    validate_config(config)

    num_threads = config.num_threads if config.num_threads > 0 else 1
    print(f"Running benchmark with {num_threads} thread(s)")

    # Number of runs for Nth-op benchmarks to average out noise
    NTH_OP_NUM_RUNS = 3

    benchmark_mode = config.WhichOneof("benchmark_mode")
    is_throughput = benchmark_mode == "throughput_benchmark"
    num_iterations = get_num_iterations(config, NTH_OP_NUM_RUNS)

    if num_iterations > 1:
        print(f"Running {num_iterations} iterations for averaging")

    # Shared result collector across all runs (appends to same parquet)
    shared_result_collector = rc.ResultCollector(run_id=config.run_id)

    # Generate a fixed seed once to use across all iterations for reproducibility.
    # If args.seed is provided, use it; otherwise generate one from time.
    fixed_seed = args.seed if args.seed is not None else int(time.time())
    print(f"Using fixed seed across all iterations: {fixed_seed}")

    for run_idx in range(num_iterations):
        if num_iterations > 1:
            print(f"\n{'=' * 60}")
            print(f"Run {run_idx + 1}/{num_iterations}")
            print(f"{'=' * 60}")

        # Setup backend project, database, and schema for this run.
        backend_info = create_backend_project(config)

        # Get branches created during setup.
        setup_branches = backend_info.setup_branches or []

        # Shared branch manager for unique branch IDs and branch list across threads.
        shared_branch_manager = SharedBranchManager(
            initial_branches=setup_branches
        )

        # Throughput mode uses a shared timer; other modes use a progress bar.
        shared_progress = None
        shared_timer = None

        if is_throughput:
            duration_sec = config.throughput_benchmark.duration_seconds
            shared_timer = SharedTimer(duration_sec)
            print(f"Throughput mode: {duration_sec}s duration, {num_threads} threads")
        else:
            ops_per_thread = get_ops_per_thread(config)
            total_ops = ops_per_thread * num_threads
            shared_progress = SharedProgress(
                total=total_ops,
                desc=f"Benchmark ({num_threads} threads)",
                disable=args.no_progress,
            )

        # Partition setup branches among threads
        thread_branch_assignments = {}
        if setup_branches and num_threads > 1:
            branches_per_thread = len(setup_branches) // num_threads
            for tid in range(num_threads):
                start_idx = tid * branches_per_thread
                end_idx = start_idx + branches_per_thread
                if tid == num_threads - 1:
                    end_idx = len(setup_branches)
                thread_branch_assignments[tid] = setup_branches[
                    start_idx:end_idx
                ]
            print(f"{branches_per_thread}+ branches assigned per thread")

        def worker_benchmark(
            thread_id: int, backend_info: BackendInfo, assigned_branches: list
        ) -> None:
            """Worker function that runs benchmark in its own thread."""
            rc.set_current_thread_id(thread_id)
            worker_seed = fixed_seed + thread_id

            worker_bench = BenchmarkSuite(
                config,
                backend_info,
                seed=worker_seed,
                thread_id=thread_id,
                result_collector=shared_result_collector,
                branch_manager=shared_branch_manager,
                shared_progress=shared_progress,
                shared_timer=shared_timer,
                assigned_branches=assigned_branches,
            )

            with worker_bench:
                worker_bench.run_benchmark()

        try:
            # Start the timer before spawning workers (throughput mode).
            if shared_timer:
                shared_timer.start()

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [
                    executor.submit(
                        worker_benchmark,
                        thread_id=i,
                        backend_info=backend_info,
                        assigned_branches=thread_branch_assignments.get(i, []),
                    )
                    for i in range(num_threads)
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Worker thread failed: {e}")

            if shared_progress:
                shared_progress.close()
            if shared_timer:
                print(f"Throughput run complete. Elapsed: {shared_timer.elapsed():.1f}s")

        finally:
            cleanup_backend(
                config,
                backend_info,
            )

    # Write all collected results to parquet (appends across runs).
    shared_result_collector.write_to_parquet()
