from tqdm import tqdm
from util.import_db import load_sql_file
import argparse
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Self, Tuple, Optional

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node

from google.protobuf import text_format
from microbench import task_pb2 as tp
from microbench.datagen import DynamicDataGenerator
from util import db_helpers as dbh

from dblib import result_collector as rc
from dblib.dolt import DoltToolSuite
from dblib.neon import NeonToolSuite
from dblib.kpg import KpgToolSuite
from dblib.xata import XataToolSuite


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


def validate_config(config: tp.TaskConfig):
    if config.backend == tp.Backend.NEON:
        db_setup = config.database_setup
        source_type = db_setup.WhichOneof("source")
        if source_type == "existing_db":
            assert db_setup.existing_db.neon_project_id, (
                "When reusing existing Neon database, neon_project_id "
                "must be provided."
            )


@dataclass
class BackendInfo:
    """Backend-specific connection and project information."""

    # NOTE: Default URI isn't the URI for the benchmark database. The benchmark
    # runs on a separate database
    default_uri: str = ""
    default_branch_id: str = ""
    default_branch_name: str = ""
    neon_project_id: Optional[str] = None
    xata_project_id: Optional[str] = None


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
    config: tp.TaskConfig, backend_info: BackendInfo, db_tools=None
) -> None:
    """Clean up backend-specific resources (projects, databases).

    Args:
        config: Task configuration with cleanup settings.
        backend_info: Backend info containing project IDs.
        db_tools: Optional db_tools for deleting database. If None, skips db deletion.
    """
    if not config.database_setup.cleanup:
        return

    # Delete the database if db_tools is provided.
    if db_tools:
        try:
            db_tools.delete_db(config.database_setup.db_name)
            print("Database deleted successfully.")
        except Exception as e:
            if "database not found" in str(e):
                print("Database not found. Assuming it was already deleted.")
            else:
                print(f"Error deleting database: {e}")

    # Delete backend-specific project if applicable.
    if backend_info.neon_project_id:
        NeonToolSuite.delete_project(backend_info.neon_project_id)
    elif backend_info.xata_project_id:
        XataToolSuite.delete_project(backend_info.xata_project_id)


class SharedBranchManager:
    """Thread-safe manager for branch IDs and branch list across threads."""

    def __init__(self, initial_branch_id: int = 1):
        self._next_branch_id = initial_branch_id
        self._branches: list[str] = []
        self._branch_limit_reached = False
        self._lock = threading.Lock()

    def get_next_branch_id(self) -> int:
        """Atomically get the next branch ID and increment the counter."""
        with self._lock:
            current = self._next_branch_id
            self._next_branch_id += 1
            return current

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared list."""
        with self._lock:
            self._branches.append(branch_name)

    def get_random_branch(self, rnd) -> Optional[str]:
        """Get a random branch from the list."""
        with self._lock:
            if not self._branches:
                return None
            return rnd.choice(self._branches)

    def get_all_branches(self) -> list[str]:
        """Get a copy of all branches."""
        with self._lock:
            return list(self._branches)

    def __len__(self) -> int:
        with self._lock:
            return len(self._branches)

    def is_branch_limit_reached(self) -> bool:
        """Check if the branch limit has been reached."""
        with self._lock:
            return self._branch_limit_reached

    def set_branch_limit_reached(self) -> None:
        """Mark that the branch limit has been reached."""
        with self._lock:
            self._branch_limit_reached = True


class BenchmarkSuite:
    def __init__(
        self,
        config: tp.TaskConfig,
        backend_info: BackendInfo,
        seed: int = None,
        thread_id: int = 0,
        result_collector: Optional[rc.ResultCollector] = None,
        branch_manager: Optional[SharedBranchManager] = None,
    ):
        self._db_name = config.database_setup.db_name
        self._config = config
        self._backend_info = backend_info
        self._seed = seed  # Optional seed for reproducibility
        self._thread_id = thread_id
        self._shared_result_collector = result_collector
        self._shared_branch_manager = branch_manager
        self._require_db_setup = (
            config.database_setup.WhichOneof("source") == "sql_dump"
        )

        # Mapping between table name and data generator.
        self._table_datagen = None

        # List of all branches created.
        self._all_branches = []

        # Cached keys to read from.

        # Mapping from branch ID to list of modified keys by this benchmark.
        self._modified_keys = {}

        # List of existing primary keys in the database for the current branch.
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
        if self._shared_result_collector is not None:
            result_collector = self._shared_result_collector
        else:
            result_collector = rc.ResultCollector(
                run_id=self._config.run_id,
                thread_id=self._thread_id,
            )

        try:
            # Use backend info passed to constructor.
            default_branch_id = self._backend_info.default_branch_id
            self._root_branch_name = self._backend_info.default_branch_name

            # Initialize the appropriate db_tools for the backend.
            if self._config.backend == tp.Backend.DOLT:
                db_tools = DoltToolSuite.init_for_bench(
                    result_collector, self._db_name, self._config.autocommit
                )
            elif self._config.backend == tp.Backend.KPG:
                db_tools = KpgToolSuite.init_for_bench(
                    result_collector, self._db_name, self._config.autocommit
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
        print(f"Exiting BenchmarkSuite context (thread {self._thread_id})...")

        # Only write parquet if not using shared result collector
        # (main thread will aggregate and write for all workers).
        if self._shared_result_collector is None:
            self.db_tools.result_collector.write_to_parquet()
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
            next_branch_name = f"branch_{next_bid}"
            self.db_tools.create_branch(
                branch_name=next_branch_name, parent_id=cur_id
            )
            self._add_branch(next_branch_name)

            # Toss a fair coin to connect to the new branch, or stay on the
            # current branch.
            if rnd.random() < 0.5:
                self.db_tools.connect_branch(next_branch_name, timed=True)
                # clear existing pks cache if switing to a different branch.
                self._existing_pks = []
        elif rnd.random() < 0.25:
            # 1/4 chance to connect to a random branch.
            to_connect = self._get_random_branch(rnd)
            if to_connect:
                self.db_tools.connect_branch(to_connect, timed=True)
                # clear existing pks cache if switing to a different branch.
                self._existing_pks = []

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

        selected_key = None
        if not existing_pks or (
            self._modified_keys.get(cur_branch_id) and rnd.random() < 0.5
        ):
            if self._modified_keys.get(cur_branch_id):
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
        self.db_tools.execute_sql(select_sql, key_to_read, timed=True)

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

    def update_op(self, rnd, benchmark_table) -> None:
        cur_branch_id, key_to_update = self._select_random_key(
            rnd, benchmark_table
        )

        if not key_to_update:
            raise ValueError("No existing keys found during update, do nothing")

        # Get all columns and filter out PK columns to get updatable columns.
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
        self.db_tools.result_collector.record_num_keys_touched(1)

        # Run the update.
        self.db_tools.execute_sql(update_sql, row_data, timed=True)
        # Track the modified key.
        if key_to_update not in self._modified_keys.get(cur_branch_id, []):
            self._modified_keys.setdefault(cur_branch_id, []).append(
                key_to_update
            )
        if not self.db_tools.autocommit:
            self.db_tools.commit_changes(timed=True, message="update")

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

        self.maybe_load_pk_columns(benchmark_table)

        # Get all PK values sorted by all PK columns to determine range bounds.
        existing_pks = dbh.get_pk_values(
            self.db_tools.get_current_connection(),
            benchmark_table,
            self._pk_columns,
        )

        if not existing_pks:
            raise ValueError(
                "No existing keys found during range update, do nothing"
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
        range_params = {}
        for i, val in enumerate(start_key):
            range_params[f"_start_{i}"] = val
        for i, val in enumerate(end_key):
            range_params[f"_end_{i}"] = val

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

        # Build WHERE clause using tuple comparison on all PK columns.
        where_clause = f"{pk_tuple_sql} >= ({placeholders_start}) AND {pk_tuple_sql} <= ({placeholders_end})"

        # Add range params to row_data.
        row_data.update(range_params)

        update_sql = (
            f"UPDATE {benchmark_table} SET {set_clause} WHERE {where_clause};"
        )

        # Record the accurate number of keys touched.
        self.db_tools.result_collector.record_num_keys_touched(
            len(keys_in_range)
        )

        # Run the range update.
        self.db_tools.execute_sql(update_sql, row_data, timed=True)

        # Track all actual keys in the range as modified.
        modified_list = self._modified_keys.setdefault(cur_branch_id, [])
        for key in keys_in_range:
            if key not in modified_list:
                modified_list.append(key)
        if not self.db_tools.autocommit:
            self.db_tools.commit_changes(timed=True, message="range update")
        return len(keys_in_range)

    def run_benchmark(self):
        # Get the benchmark table and load the data generator for the table.
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

        # Get the random seed for all remainder operations in the benchmark.
        # Use provided seed for reproducibility, otherwise use current time.
        seed = self._seed if self._seed is not None else int(time.time())
        # Create thread-local random instance to avoid interference between threads
        rnd = random.Random(seed)
        print(f"Using random seed: {seed}")

        initial_db_size = 0
        try:
            initial_db_size = dbh.get_db_size(
                self.db_tools.get_current_connection()
            )
        except Exception as e:
            print(f"Error getting initial DB size: {e}")

        # Set context for result proto collection.
        self.db_tools.result_collector.set_context(
            table_name=benchmark_table,
            table_schema=table_schema,
            initial_db_size=initial_db_size,
            seed=seed,
        )

        # Get the list of operations to perform, and the probability of each
        # operation.
        all_operations = self._config.operations
        ops_weights = [OPS_WEIGHT(op) for op in all_operations]

        # Main benchmark loop
        for _ in tqdm(range(self._config.num_ops)):
            # Get the operation
            cur_ops = rnd.choices(all_operations, ops_weights)[0]
            try:
                if cur_ops == tp.OperationType.BRANCH:
                    next_bid = self._shared_branch_manager.get_next_branch_id()

                    try:
                        self.maybe_branch_and_reconnect(next_bid, rnd)
                    except Exception as e:
                        if "branches limit exceeded" in str(e):
                            # Update shared state so all threads know
                            self._shared_branch_manager.set_branch_limit_reached()
                        raise e
                elif cur_ops == tp.OperationType.READ:
                    try:
                        self.read_op(rnd, benchmark_table)
                    except ValueError as e:
                        tqdm.write(f"Error reading key: {e}")
                elif cur_ops == tp.OperationType.INSERT:
                    self.insert_op(benchmark_table)
                elif cur_ops == tp.OperationType.UPDATE:
                    self.update_op(rnd, benchmark_table)
                elif cur_ops == tp.OperationType.RANGE_UPDATE:
                    self.range_update_op(rnd, benchmark_table)
            except Exception as e:
                tqdm.write(
                    f"[Thread {self._thread_id}] Error performing operation: {e}"
                )


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

    args = parser.parse_args()

    # Load and parse the textproto config file
    try:
        config = tp.TaskConfig()
        with open(args.config, "r") as f:
            text_format.Parse(f.read(), config)

        print(f"Loaded configuration from {args.config}")
        print(f"Run ID: {config.run_id}")
        print(f"Backend: {tp.Backend.Name(config.backend)}")
        print(
            f"Operations: {[tp.OperationType.Name(op) for op in config.operations]}"
        )

    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)

    validate_config(config)

    num_threads = config.num_threads if config.num_threads > 0 else 1
    print(f"Running benchmark with {num_threads} thread(s)")
    # Shared result collector for all worker threads.
    shared_result_collector = rc.ResultCollector(run_id=config.run_id)

    # Shared branch manager for unique branch IDs and branch list across threads.
    shared_branch_manager = SharedBranchManager()

    # Setup backend project, database, and schema.
    backend_info = create_backend_project(config)

    def worker_benchmark(thread_id: int) -> None:
        """Worker function that runs benchmark in its own thread with its own connection."""
        # Set thread-local thread ID for result collection
        rc.set_current_thread_id(thread_id)

        # Each worker gets its own seed based on the main seed for reproducibility.
        worker_seed = (args.seed + thread_id) if args.seed is not None else None

        # Create worker's own BenchmarkSuite with shared resources.
        worker_bench = BenchmarkSuite(
            config,
            backend_info,
            seed=worker_seed,
            thread_id=thread_id,
            result_collector=shared_result_collector,
            branch_manager=shared_branch_manager,
        )

        with worker_bench:
            worker_bench.run_benchmark()

    try:
        # Spawn worker threads.
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(worker_benchmark, thread_id=i)
                for i in range(num_threads)
            ]
            # Wait for all workers to complete and collect any exceptions.
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker thread failed: {e}")

        # Write all collected results to parquet.
        shared_result_collector.write_to_parquet()

    finally:
        # Cleanup backend resources.
        cleanup_backend(config, backend_info)
