"""
Runner2: Clean microbenchmark runner for single-operation benchmarks.

This module provides a cleaner, more maintainable architecture for running
database microbenchmarks with clear separation of concerns:
- Configuration loading and validation
- Backend setup and cleanup
- Untimed setup phase (branches + data)
- Timed execution phase (measure operations)
- Result collection and reporting

Usage:
    python -m microbench.runner2 --config path/to/config.textproto

Design principles:
- Single Responsibility: Each class has one clear purpose
- Extensibility: Easy to add new operation types
- Testability: Components can be tested independently
- Clarity: Clear execution flow from config to results
"""

import argparse
import asyncio
import json
import os
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from google.protobuf import text_format
from tqdm import tqdm

# Protobuf imports
from microbench import task2_pb2 as tp
from dblib import result_pb2 as rslt

# Database backend imports
from dblib.dolt import DoltToolSuite, commit_dolt_schema
from dblib.neon import NeonToolSuite
from dblib.kpg import KpgToolSuite
from dblib.file_copy import FileCopyToolSuite
from dblib.transaction import TxnToolSuite
from dblib.xata import XataToolSuite
from dblib.tiger import TigerToolSuite
from dblib import result_collector as rc

# Operation imports
from microbench.operations import (
    OperationRegistry,
    ReadOperation,
    InsertOperation,
    UpdateOperation,
    DeleteOperation,
    RangeReadOperation,
    RangeUpdateOperation,
    BranchCreateOperation,
    BranchConnectOperation,
    BranchDeleteOperation,
    ConnectFirstOperation,
    ConnectMidOperation,
    ConnectLastOperation,
    AddIndexOperation,
    RemoveIndexOperation,
    VacuumOperation,
)

# Utility imports
from microbench.datagen import DynamicDataGenerator
from util import db_helpers as dbh
from util.import_db import load_sql_file


# ============================================================================
# Configuration
# ============================================================================


class BenchmarkConfig:
    """Loads and validates task configuration from protobuf.

    This class encapsulates all configuration logic, making it easy to
    access configuration values throughout the benchmark code.
    """

    def __init__(self, config_proto: tp.TaskConfig):
        """Initialize from parsed protobuf config.

        Args:
            config_proto: Parsed TaskConfig protobuf message
        """
        self._proto = config_proto
        self._validate()

    @classmethod
    def load_from_file(cls, config_path: str) -> 'BenchmarkConfig':
        """Load configuration from a textproto file.

        Args:
            config_path: Path to the .textproto config file

        Returns:
            BenchmarkConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            Exception: If config parsing fails
        """
        config = tp.TaskConfig()
        with open(config_path, 'r') as f:
            text_format.Parse(f.read(), config)
        return cls(config)

    def _validate(self) -> None:
        """Validate configuration values.

        Raises:
            ValueError: If configuration is invalid
        """
        # Basic required fields
        if not self._proto.run_id:
            raise ValueError("run_id is required")
        if not self._proto.database_setup.db_name:
            raise ValueError("database_setup.db_name is required")

        # Validate benchmark mode
        if not self._proto.HasField("operation_benchmark"):
            raise ValueError("operation_benchmark is required")

        op_bench = self._proto.operation_benchmark
        if op_bench.num_ops <= 0:
            raise ValueError("operation_benchmark.num_ops must be > 0")

        # Validate storage measurement
        if self._proto.measure_storage and self._proto.num_threads > 1:
            raise ValueError(
                "measure_storage is incompatible with num_threads > 1. "
                "Concurrent writes pollute per-thread storage deltas."
            )

        # Validate async mode configuration
        if self._proto.concurrent_requests > 1:
            if not self._proto.autocommit:
                raise ValueError(
                    "concurrent_requests > 1 requires autocommit = true. "
                    "Async mode does not support transaction management."
                )

        # Backend-specific validation
        if self._proto.backend == tp.Backend.NEON:
            db_setup = self._proto.database_setup
            source_type = db_setup.WhichOneof("source")
            if source_type == "existing_db":
                if not db_setup.existing_db.neon_project_id:
                    raise ValueError(
                        "neon_project_id required when reusing existing Neon database"
                    )

        # Validate operation is registered
        op_type = op_bench.operation
        if not OperationRegistry.is_registered(op_type):
            raise ValueError(
                f"Operation type {tp.OperationType.Name(op_type)} not registered"
            )

    # Configuration properties
    @property
    def run_id(self) -> str:
        return self._proto.run_id

    @property
    def backend(self) -> tp.Backend:
        return self._proto.backend

    @property
    def backend_name(self) -> str:
        return tp.Backend.Name(self._proto.backend)

    @property
    def table_name(self) -> str:
        return self._proto.table_name

    @property
    def scale_factor(self) -> int:
        return self._proto.scale_factor

    @property
    def autocommit(self) -> bool:
        return self._proto.autocommit

    @property
    def num_threads(self) -> int:
        return max(1, self._proto.num_threads)

    @property
    def measure_storage(self) -> bool:
        return self._proto.measure_storage

    @property
    def concurrent_requests(self) -> int:
        """Number of concurrent requests per connection (default 1 = sync mode)."""
        return max(1, self._proto.concurrent_requests)

    @property
    def database_setup(self) -> tp.DatabaseSetup:
        return self._proto.database_setup

    @property
    def db_name(self) -> str:
        return self._proto.database_setup.db_name

    @property
    def cleanup(self) -> bool:
        return self._proto.database_setup.cleanup

    @property
    def operation_type(self) -> tp.OperationType:
        return self._proto.operation_benchmark.operation

    @property
    def operation_name(self) -> str:
        return tp.OperationType.Name(self._proto.operation_benchmark.operation)

    @property
    def num_ops(self) -> int:
        return self._proto.operation_benchmark.num_ops

    @property
    def warmup_ops(self) -> int:
        return self._proto.operation_benchmark.warmup_ops

    @property
    def setup_config(self) -> tp.SetupConfig:
        return self._proto.operation_benchmark.setup

    @property
    def num_branches(self) -> int:
        return self._proto.operation_benchmark.setup.num_branches

    @property
    def branch_shape(self) -> tp.BranchShape:
        return self._proto.operation_benchmark.setup.branch_shape

    @property
    def range_config(self) -> tp.RangeConfig:
        return self._proto.operation_benchmark.range_config

    @property
    def range_size(self) -> int:
        return self._proto.operation_benchmark.range_config.range_size or 10

    @property
    def ddl_config(self) -> tp.DdlConfig:
        return self._proto.operation_benchmark.ddl_config

    def __str__(self) -> str:
        return (
            f"BenchmarkConfig(run_id={self.run_id}, "
            f"backend={self.backend_name}, "
            f"operation={self.operation_name}, "
            f"num_ops={self.num_ops}, "
            f"num_branches={self.num_branches}, "
            f"scale_factor={self.scale_factor})"
        )


# ============================================================================
# Backend Management
# ============================================================================


@dataclass
class BackendInfo:
    """Backend-specific connection and project information."""

    default_uri: str = ""
    default_branch_id: str = ""
    default_branch_name: str = ""
    neon_project_id: Optional[str] = None
    xata_project_id: Optional[str] = None
    tiger: Optional[dict] = None
    file_copy_info: Optional[FileCopyToolSuite.FileCopyInfo] = None
    txn_conn: Optional[psycopg2.extensions.connection] = None
    txn_branch_state: Optional[dict] = None
    setup_branches: list = None


class BackendManager:
    """Manages backend-specific setup and cleanup.

    Extracted from create_backend_project() and cleanup_backend() for
    better organization and testability.
    """

    def __init__(self, config: BenchmarkConfig, output_dir: str = "/tmp/run_stats"):
        self.config = config
        self.output_dir = output_dir
        self.backend_info: Optional[BackendInfo] = None

    def setup(self) -> BackendInfo:
        """Create backend project and database, perform setup.

        Returns:
            BackendInfo with connection details

        Raises:
            ValueError: If backend is unsupported
            Exception: If setup fails
        """
        config = self.config
        backend = config.backend
        db_name = config.db_name
        require_db_setup = config.database_setup.WhichOneof("source") == "sql_dump"

        info = BackendInfo()

        # Backend-specific project/connection creation
        if backend == tp.Backend.DOLT:
            info.default_uri = DoltToolSuite.get_default_connection_uri()
            info.default_branch_name = "main"
            print(f"Default Dolt connection URI: {info.default_uri}")

        elif backend == tp.Backend.KPG:
            info.default_uri = KpgToolSuite.get_default_connection_uri()
            info.default_branch_name = "main"
            print(f"Default KPG connection URI: {info.default_uri}")

        elif backend == tp.Backend.TXN:
            info.default_uri = TxnToolSuite.get_default_connection_uri()
            info.default_branch_name = "main"
            print(f"Default PostgreSQL connection URI: {info.default_uri}")

        elif backend == tp.Backend.FILE_COPY:
            info.file_copy_info = FileCopyToolSuite.FileCopyInfo(db_name)
            info.default_uri = FileCopyToolSuite.get_default_connection_uri()
            info.default_branch_name = db_name
            print(f"Default FILE_COPY connection URI: {info.default_uri}")

        elif backend == tp.Backend.NEON:
            if require_db_setup:
                neon_project = NeonToolSuite.create_neon_project(f"project_{db_name}")
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
                info.neon_project_id = config.database_setup.existing_db.neon_project_id
                proj_branches = NeonToolSuite.get_project_branches(info.neon_project_id)
                for branch in proj_branches["branches"]:
                    if branch["default"]:
                        info.default_branch_name = branch["name"]
                        info.default_branch_id = branch["id"]
                        break

        elif backend == tp.Backend.TIGER:
            if require_db_setup:
                tiger_service = TigerToolSuite.create_tiger_service(
                    name=f"service_{db_name}"
                )
                info.tiger = dict()
                info.tiger["password"] = tiger_service["initial_password"]
                info.tiger["service_id"] = tiger_service["service_id"]
                info.tiger["project_id"] = tiger_service["project_id"]
                info.tiger["service_name"] = tiger_service["name"]
                info.tiger["region"] = tiger_service["region_code"]
                info.tiger["services"] = dict()
                tiger_service = TigerToolSuite.wait_for_service(
                    info.tiger["project_id"], info.tiger["service_id"]
                )
                info.default_uri = (
                    f"postgresql://tsdbadmin:{info.tiger['password']}"
                    f"@{tiger_service['endpoint']['host']}"
                    f":{tiger_service['endpoint']['port']}/tsdb"
                )
                info.default_branch_id = tiger_service["service_id"]
                info.default_branch_name = tiger_service["name"]
                print(f"Tiger service ID: {info.tiger['service_id']}")
                print(f"Default Tiger connection URI: {info.default_uri}")
            else:
                raise NotImplementedError("Tiger with existing service not implemented")

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
                print(f"Xata project ID: {info.xata_project_id}")
                print(f"Default Xata connection URI: {info.default_uri}")
            else:
                raise NotImplementedError("Xata requires database setup")

        else:
            raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")

        # Create database and load schema if needed
        if require_db_setup:
            if not info.tiger:
                self._create_database(info.default_uri, db_name)

            # Load SQL dump
            db_uri = self._get_connection_uri(info)
            sql_dump_path = config.database_setup.sql_dump.sql_dump_path
            load_sql_file(db_uri, sql_dump_path)

            # Commit schema changes for Dolt
            if backend == tp.Backend.DOLT:
                commit_dolt_schema(db_uri)

        self.backend_info = info
        return info

    def cleanup(self) -> None:
        """Clean up backend resources (databases, projects).

        This should be called after the benchmark completes.
        """
        if not self.config.cleanup or not self.backend_info:
            return

        info = self.backend_info
        db_name = self.config.db_name

        # Backend-specific cleanup
        if info.file_copy_info:
            FileCopyToolSuite.cleanup(info.file_copy_info)
        elif info.tiger:
            all_ids = info.tiger.get("services", [])
            root_id = info.tiger["service_id"]
            project_id = info.tiger["project_id"]
            # Delete forks first, root last
            for sname, (sid, pw) in all_ids:
                if sid != root_id:
                    try:
                        TigerToolSuite.delete_tiger_service(project_id, sid)
                    except Exception as e:
                        print(f"Warning: failed to delete Tiger service {sid}: {e}")
            TigerToolSuite.delete_tiger_service(project_id, root_id)
        elif info.default_uri and db_name:
            # Close TXN connection if exists
            if info.txn_conn:
                try:
                    info.txn_conn.rollback()
                except Exception:
                    pass
                info.txn_conn.close()

            # Drop database
            try:
                conn = psycopg2.connect(info.default_uri)
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
                print(f"Database '{db_name}' deleted successfully.")
                cur.close()
                conn.close()
            except Exception as e:
                print(f"Error deleting database: {e}")

        # Delete cloud projects
        if info.neon_project_id:
            NeonToolSuite.delete_project(info.neon_project_id)
        elif info.xata_project_id:
            XataToolSuite.delete_project(info.xata_project_id)

    def _create_database(self, uri: str, db_name: str) -> None:
        """Create the benchmark database."""
        try:
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            try:
                cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
            except Exception as drop_err:
                print(f"Warning: could not drop existing database: {drop_err}")
            cur.execute(f"CREATE DATABASE {db_name};")
            print(f"Database '{db_name}' created successfully.")
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error creating database: {e}")
            raise

    def _get_connection_uri(self, info: BackendInfo) -> str:
        """Get the database-specific connection URI."""
        backend = self.config.backend
        db_name = self.config.db_name

        if backend == tp.Backend.DOLT:
            return DoltToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.KPG:
            return KpgToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.FILE_COPY:
            return FileCopyToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.TXN:
            return TxnToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.NEON:
            return NeonToolSuite._get_neon_connection_uri(
                info.neon_project_id, info.default_branch_id, db_name
            )
        elif backend == tp.Backend.TIGER:
            return info.default_uri
        elif backend == tp.Backend.XATA:
            return XataToolSuite._get_xata_connection_uri(
                info.xata_project_id, info.default_branch_id, db_name
            )
        else:
            raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")


# ============================================================================
# Setup Phase (UNTIMED)
# ============================================================================


class SetupPhase:
    """Manages the untimed setup phase.

    Creates branches and performs data operations to prepare the database
    for measurement. Timing data is saved to a separate _setup.parquet file.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        output_dir: str = "/tmp/run_stats"
    ):
        self.config = config
        self.backend_info = backend_info
        self.output_dir = output_dir

    def execute(self) -> List[str]:
        """Run the setup phase and return list of created branches.

        Returns:
            List of branch names created during setup

        Raises:
            Exception: If setup fails
        """
        setup = self.config.setup_config
        num_branches = setup.num_branches
        shape = setup.branch_shape
        inserts_per_branch = setup.inserts_per_branch
        updates_per_branch = setup.updates_per_branch
        deletes_per_branch = setup.deletes_per_branch

        print(f"\n{'='*60}")
        print(f"Setup Phase (UNTIMED)")
        print(f"{'='*60}")
        print(f"Creating {num_branches} branches (shape: {tp.BranchShape.Name(shape)})")
        print(f"Per branch: {inserts_per_branch} inserts, {updates_per_branch} updates, {deletes_per_branch} deletes")

        # Create result collector for setup timing
        setup_result_collector = rc.ResultCollector(
            run_id=f"{self.config.run_id}_setup",
            output_dir=self.output_dir,
        )

        # Create shared branch manager
        setup_branch_manager = SharedBranchManager()

        # Create a worker context for setup operations
        with self._create_setup_context(
            setup_result_collector, setup_branch_manager
        ) as setup_ctx:
            last_branch_name, last_branch_id = self._perform_setup_operations(
                setup_ctx,
                num_branches,
                shape,
                inserts_per_branch,
                updates_per_branch,
                deletes_per_branch,
            )

            # Update backend info with last branch
            self.backend_info.default_branch_name = last_branch_name
            self.backend_info.default_branch_id = last_branch_id

            # Store Tiger services if applicable
            if self.config.backend == tp.Backend.TIGER:
                self.backend_info.tiger["services"] = setup_ctx.db_tools._services

            # Store TXN branch state if applicable
            if self.config.backend == tp.Backend.TXN:
                self.backend_info.txn_branch_state = setup_ctx.db_tools.get_branch_state()

        # Get all created branches
        setup_branches = setup_branch_manager.get_all_branches()
        self.backend_info.setup_branches = setup_branches

        # Write setup timing to separate parquet file
        setup_result_collector.write_to_parquet(
            filename=f"{self.config.run_id}_setup.parquet"
        )

        print(f"Setup complete: {len(setup_branches)} branches created")
        print(f"Last branch: {last_branch_name}")
        print(f"{'='*60}\n")

        return setup_branches

    @contextmanager
    def _create_setup_context(
        self,
        result_collector: rc.ResultCollector,
        branch_manager: 'SharedBranchManager'
    ):
        """Create a WorkerContext for setup operations."""
        # This is a simplified version just for setup
        # We'll define WorkerContext below
        from microbench.runner2 import WorkerContext

        ctx = WorkerContext(
            config=self.config,
            backend_info=self.backend_info,
            thread_id=0,
            seed=42,  # Fixed seed for reproducible setup
            result_collector=result_collector,
            branch_manager=branch_manager,
            shared_progress=None,
            assigned_branches=[],
        )

        try:
            ctx.__enter__()
            yield ctx
        finally:
            ctx.__exit__(None, None, None)

    def _perform_setup_operations(
        self,
        ctx: 'WorkerContext',
        num_branches: int,
        shape: tp.BranchShape,
        inserts: int,
        updates: int,
        deletes: int,
    ) -> Tuple[str, str]:
        """Perform branch creation and data operations.

        Returns:
            Tuple of (last_branch_name, last_branch_id)
        """
        # Get benchmark table
        benchmark_table = self.config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
            benchmark_table = ctx.rnd.choice(all_tables)

        # Initialize data generator
        table_schema = ctx.db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(f"Could not fetch DDL for table {benchmark_table}")

        ctx._table_datagen = DynamicDataGenerator(table_schema)
        ctx._pk_columns = dbh.get_pk_column_names(
            ctx.db_tools.get_current_connection(), benchmark_table
        )

        _, root_branch_id = ctx.db_tools.get_current_branch()
        current_parent_id = root_branch_id
        root_branch_name = self.backend_info.default_branch_name

        # Track branch IDs for BUSHY shape
        branch_ids = [(root_branch_name, root_branch_id)]

        # Perform setup on root branch
        print(f"Performing setup ops on root branch...")
        self._perform_branch_data_ops(
            ctx, benchmark_table, inserts, updates, deletes
        )

        # Create branches with progress bar
        for i in tqdm(range(num_branches), desc="Creating branches"):
            branch_name = f"setup_branch_{i + 1}"

            if shape == tp.BranchShape.SPINE:
                # Linear chain
                ctx.db_tools.create_branch(
                    branch_name, current_parent_id, timed=True,
                    storage=self.config.measure_storage
                )
                ctx.db_tools.connect_branch(branch_name, timed=False)
                _, current_parent_id = ctx.db_tools.get_current_branch()
                branch_ids.append((branch_name, current_parent_id))

            elif shape == tp.BranchShape.FAN_OUT:
                # All from root
                ctx.db_tools.create_branch(
                    branch_name, root_branch_id, timed=True,
                    storage=self.config.measure_storage
                )
                ctx.db_tools.connect_branch(branch_name, timed=False)
                _, new_branch_id = ctx.db_tools.get_current_branch()
                branch_ids.append((branch_name, new_branch_id))

            else:  # BUSHY
                # Random parent
                parent_name, parent_id = ctx.rnd.choice(branch_ids)
                ctx.db_tools.create_branch(
                    branch_name, parent_id, timed=True,
                    storage=self.config.measure_storage
                )
                ctx.db_tools.connect_branch(branch_name, timed=False)
                _, new_branch_id = ctx.db_tools.get_current_branch()
                branch_ids.append((branch_name, new_branch_id))

            # Perform data operations on this branch
            self._perform_branch_data_ops(
                ctx, benchmark_table, inserts, updates, deletes
            )

            # Track the branch
            ctx.add_branch(branch_name)

        # Get last branch info
        if self.config.backend == tp.Backend.FILE_COPY:
            if num_branches > 0:
                last_branch_name = f"setup_branch_{num_branches}"
                last_branch_id = last_branch_name
            else:
                last_branch_name = root_branch_name
                last_branch_id = root_branch_id
        else:
            last_branch_name, last_branch_id = ctx.db_tools.get_current_branch()

        return last_branch_name, last_branch_id

    def _perform_branch_data_ops(
        self,
        ctx: 'WorkerContext',
        table_name: str,
        inserts: int,
        updates: int,
        deletes: int
    ) -> None:
        """Perform inserts, updates, and deletes on current branch (untimed)."""
        # Perform inserts first
        for _ in range(inserts):
            ctx._insert_without_timing(table_name)

        if not ctx.db_tools.autocommit:
            ctx.db_tools.commit_changes(timed=False, message="setup_inserts")

        # Shuffle and perform updates/deletes
        ops = ["update"] * updates + ["delete"] * deletes
        ctx.rnd.shuffle(ops)

        for op in ops:
            if op == "update":
                ctx._update_without_timing(table_name)
            else:
                ctx._delete_without_timing(table_name)

        if not ctx.db_tools.autocommit and (updates > 0 or deletes > 0):
            ctx.db_tools.commit_changes(timed=False, message="setup_updates_deletes")


# ============================================================================
# Shared State Management
# ============================================================================


class SharedBranchManager:
    """Thread-safe manager for branch IDs and branch list."""

    def __init__(self, initial_branches: Optional[List[str]] = None):
        self._next_branch_id = 1
        self._branches: List[str] = list(initial_branches) if initial_branches else []
        self._lock = threading.Lock()

    def get_next_branch_id(self) -> int:
        """Atomically get next branch ID and increment."""
        with self._lock:
            current = self._next_branch_id
            self._next_branch_id += 1
            return current

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared list."""
        with self._lock:
            self._branches.append(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared list."""
        with self._lock:
            if branch_name in self._branches:
                self._branches.remove(branch_name)

    def get_random_branch(self, rnd: random.Random) -> Optional[str]:
        """Get a random branch from the list."""
        with self._lock:
            if not self._branches:
                return None
            return rnd.choice(self._branches)

    def get_all_branches(self) -> List[str]:
        """Get a copy of all branches."""
        with self._lock:
            return list(self._branches)

    def __len__(self) -> int:
        with self._lock:
            return len(self._branches)


class SharedProgress:
    """Thread-safe shared progress bar."""

    def __init__(self, total: int, desc: str = "Progress", disable: bool = False):
        self._pbar = tqdm(total=total, desc=desc, position=0, leave=True, disable=disable)
        self._lock = threading.Lock()

    def update(self, n: int = 1) -> None:
        """Thread-safe update of progress bar."""
        with self._lock:
            self._pbar.update(n)

    def close(self) -> None:
        """Close the progress bar."""
        self._pbar.close()

    def write(self, msg: str) -> None:
        """Thread-safe write message above progress bar."""
        tqdm.write(msg)


# ============================================================================
# Worker Context
# ============================================================================


class WorkerContext:
    """Context object providing services to operations.

    This replaces the monolithic BenchmarkSuite with a cleaner interface
    that operations can use to access database connections, random number
    generation, data generation, etc.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        thread_id: int,
        seed: int,
        result_collector: rc.ResultCollector,
        branch_manager: SharedBranchManager,
        shared_progress: Optional[SharedProgress],
        assigned_branches: List[str],
    ):
        self.config = config
        self.backend_info = backend_info
        self.thread_id = thread_id
        self.seed = seed
        self.result_collector = result_collector
        self.branch_manager = branch_manager
        self.shared_progress = shared_progress
        self.assigned_branches = assigned_branches
        self.measure_storage = config.measure_storage

        # Thread-local RNG
        self.rnd = random.Random(seed)

        # Database tools (initialized in __enter__)
        self.db_tools = None

        # Data generation and caching
        self._table_datagen: Optional[DynamicDataGenerator] = None
        self._pk_columns: List[str] = []
        self._existing_pks: List[Tuple] = []
        self._modified_keys: Dict[str, List[Tuple]] = {}

        # Index tracking for DDL operations
        self._created_indexes: Dict[str, List[str]] = {}  # table -> [index_names]

    def __enter__(self) -> 'WorkerContext':
        """Initialize database connection and tools."""
        config = self.config
        backend_info = self.backend_info

        # Set thread ID for result collection
        rc.set_current_thread_id(self.thread_id)

        # Create result collector
        result_collector = self.result_collector

        # Initialize backend-specific tools
        db_name = config.db_name
        default_branch_name = backend_info.default_branch_name
        backend = config.backend

        if backend == tp.Backend.DOLT:
            self.db_tools = DoltToolSuite.init_for_bench(
                result_collector, db_name, config.autocommit, default_branch_name
            )
        elif backend == tp.Backend.KPG:
            self.db_tools = KpgToolSuite.init_for_bench(
                result_collector, db_name, config.autocommit
            )
        elif backend == tp.Backend.TXN:
            backend_info.txn_conn = TxnToolSuite.get_connection(
                backend_info.txn_conn, db_name
            )
            self.db_tools = TxnToolSuite.init_for_bench(
                result_collector,
                db_name,
                config.autocommit,
                default_branch_name,
                backend_info.setup_branches,
                backend_info.txn_conn,
                backend_info.txn_branch_state,
            )
        elif backend == tp.Backend.FILE_COPY:
            self.db_tools = FileCopyToolSuite.init_for_bench(
                result_collector,
                db_name,
                config.autocommit,
                default_branch_name,
                backend_info.file_copy_info.branches,
                backend_info.file_copy_info.branches_lock,
                backend_info.file_copy_info.create_db_lock,
            )
            # Worker threads connect to first setup branch
            if config.num_threads > 1 and self.thread_id > 0:
                if backend_info.setup_branches:
                    self.db_tools.connect_branch(
                        backend_info.setup_branches[0], timed=False
                    )
        elif backend == tp.Backend.NEON:
            self.db_tools = NeonToolSuite.init_for_bench(
                result_collector,
                backend_info.neon_project_id,
                backend_info.default_branch_id,
                default_branch_name,
                db_name,
                config.autocommit,
            )
        elif backend == tp.Backend.TIGER:
            if not backend_info.tiger:
                raise Exception("Tiger backend info empty")
            self.db_tools = TigerToolSuite.init_for_bench(
                result_collector,
                backend_info.tiger["project_id"],
                backend_info.tiger["service_id"],
                backend_info.tiger["service_name"],
                backend_info.tiger["password"],
                backend_info.tiger["region"],
                config.autocommit,
                backend_info.tiger["services"],
            )
        elif backend == tp.Backend.XATA:
            self.db_tools = XataToolSuite.init_for_bench(
                result_collector,
                backend_info.xata_project_id,
                backend_info.default_branch_id,
                default_branch_name,
                db_name,
                config.autocommit,
            )
        else:
            raise ValueError(f"Unsupported backend: {tp.Backend.Name(backend)}")

        # Add root branch to manager (except FILE_COPY)
        if backend != tp.Backend.FILE_COPY:
            self.branch_manager.add_branch(default_branch_name)

        # Set storage function if measuring storage
        if self.measure_storage:
            self.db_tools.result_collector.set_storage_fn(
                self.db_tools.get_total_storage_bytes
            )

        # Connect to assigned branch if multi-threaded
        if self.assigned_branches:
            initial_branch = self.assigned_branches[0]
            if self.thread_id == 0:
                print(f"Thread {self.thread_id} connecting to: {initial_branch}")
            self.db_tools.connect_branch(initial_branch, timed=False)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection."""
        if self.db_tools and not self.backend_info.txn_conn:
            self.db_tools.close_connection()

        # Update Tiger services info
        if self.config.backend == tp.Backend.TIGER and self.backend_info.tiger:
            self.backend_info.tiger["services"] = self.db_tools.get_all_services()

    # ========================================================================
    # Services for Operations
    # ========================================================================

    def get_pk_columns(self, table_name: str) -> List[str]:
        """Get primary key column names for a table."""
        if not self._pk_columns:
            self._pk_columns = dbh.get_pk_column_names(
                self.db_tools.get_current_connection(), table_name
            )
        return self._pk_columns

    def select_random_key(self, table_name: str) -> Optional[Tuple]:
        """Select a random primary key from the table."""
        _, cur_branch_id = self.db_tools.get_current_branch()

        pk_columns = self.get_pk_columns(table_name)
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(), table_name, pk_columns
        )

        # Cache PKs
        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        if not existing_pks:
            return None

        # 50% chance to select from modified keys if available
        modified = self._modified_keys.get(cur_branch_id, [])
        if modified and self.rnd.random() < 0.5:
            return self.rnd.choice(modified)
        else:
            return self.rnd.choice(existing_pks)

    def generate_row(self, table_name: str) -> dict:
        """Generate a fake data row for the table."""
        if not self._table_datagen:
            table_schema = self.db_tools.get_table_schema(table_name)
            self._table_datagen = DynamicDataGenerator(table_schema)
        return self._table_datagen.generate_row()

    def track_modified_key(self, key: Tuple) -> None:
        """Track a modified primary key."""
        _, cur_branch_id = self.db_tools.get_current_branch()
        if key not in self._modified_keys.get(cur_branch_id, []):
            self._modified_keys.setdefault(cur_branch_id, []).append(key)

    def untrack_modified_key(self, key: Tuple) -> None:
        """Remove a key from modified tracking (e.g., after delete)."""
        _, cur_branch_id = self.db_tools.get_current_branch()
        if cur_branch_id in self._modified_keys:
            if key in self._modified_keys[cur_branch_id]:
                self._modified_keys[cur_branch_id].remove(key)

    def record_keys_touched(self, num_keys: int) -> None:
        """Record number of keys touched for this operation."""
        self.db_tools.result_collector.record_num_keys_touched(num_keys)

    def get_next_branch_id(self) -> int:
        """Get next unique branch ID."""
        return self.branch_manager.get_next_branch_id()

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared branch list."""
        self.branch_manager.add_branch(branch_name)

    def remove_branch(self, branch_name: str) -> None:
        """Remove a branch from the shared branch list."""
        self.branch_manager.remove_branch(branch_name)

    def get_random_branch(self) -> Optional[str]:
        """Get a random branch from the shared list."""
        return self.branch_manager.get_random_branch(self.rnd)

    def get_all_branches(self) -> List[str]:
        """Get all branches."""
        return self.branch_manager.get_all_branches()

    def clear_pk_cache(self) -> None:
        """Clear cached primary keys (e.g., after branch switch)."""
        self._existing_pks = []

    def prepare_range_query(
        self, table_name: str, range_size: int, operation_name: str
    ) -> dict:
        """Prepare range query components.

        Returns:
            dict with keys_in_range, where_clause, params
        """
        pk_columns = self.get_pk_columns(table_name)

        # Get all PKs sorted
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(), table_name, pk_columns
        )

        if not self._existing_pks and existing_pks:
            self._existing_pks = existing_pks

        if not existing_pks:
            raise ValueError(f"No existing keys for {operation_name}")

        sorted_pks = sorted(existing_pks)
        total_keys = len(sorted_pks)
        range_size = min(range_size, total_keys)

        # Pick random range
        max_start_idx = max(0, total_keys - range_size)
        start_idx = self.rnd.randint(0, max_start_idx)
        end_idx = min(start_idx + range_size - 1, total_keys - 1)

        start_key = sorted_pks[start_idx]
        end_key = sorted_pks[end_idx]
        keys_in_range = sorted_pks[start_idx : end_idx + 1]

        # Build tuple comparison for composite PKs
        pk_tuple_sql = f"({', '.join(pk_columns)})"
        placeholders_start = ", ".join(
            [f"%(_start_{i})s" for i in range(len(pk_columns))]
        )
        placeholders_end = ", ".join(
            [f"%(_end_{i})s" for i in range(len(pk_columns))]
        )

        # Build params
        params = {}
        for i, val in enumerate(start_key):
            params[f"_start_{i}"] = val
        for i, val in enumerate(end_key):
            params[f"_end_{i}"] = val

        where_clause = (
            f"{pk_tuple_sql} >= ({placeholders_start}) AND "
            f"{pk_tuple_sql} <= ({placeholders_end})"
        )

        return {
            "keys_in_range": keys_in_range,
            "where_clause": where_clause,
            "params": params,
        }

    def track_created_index(self, table_name: str, index_name: str) -> None:
        """Track a created index for later querying/dropping."""
        self._created_indexes.setdefault(table_name, []).append(index_name)

    def untrack_index(self, table_name: str, index_name: str) -> None:
        """Remove an index from tracking."""
        if table_name in self._created_indexes:
            if index_name in self._created_indexes[table_name]:
                self._created_indexes[table_name].remove(index_name)

    def get_random_index(self, table_name: str) -> Optional[str]:
        """Get a random index name from the table."""
        indexes = self._created_indexes.get(table_name, [])
        if indexes:
            return self.rnd.choice(indexes)

        # Query database for indexes
        # This is database-specific, simplified version:
        try:
            conn = self.db_tools.get_current_connection()
            cur = conn.cursor()
            cur.execute(f"""
                SELECT indexname FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%%_pkey'
            """, (table_name,))
            db_indexes = [row[0] for row in cur.fetchall()]
            if db_indexes:
                return self.rnd.choice(db_indexes)
        except Exception:
            pass

        return None

    # ========================================================================
    # Untimed operations for setup phase
    # ========================================================================

    def _insert_without_timing(self, table_name: str) -> None:
        """Insert without timing (for setup)."""
        _, cur_branch_id = self.db_tools.get_current_branch()

        col_names = dbh.get_all_columns(
            self.db_tools.get_current_connection(), table_name
        )
        pk_columns = self.get_pk_columns(table_name)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        for _ in range(5):
            row_data = self.generate_row(table_name)
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                self.db_tools.execute_sql(insert_sql, row_data, timed=False)
                self._modified_keys.setdefault(cur_branch_id, []).append(pk_tuple)
                return
            except Exception:
                continue

        raise ValueError("Failed to insert row after 5 attempts")

    def _update_without_timing(self, table_name: str) -> None:
        """Update without timing (for setup)."""
        key_to_update = self.select_random_key(table_name)
        if not key_to_update:
            return  # Skip if no keys

        pk_columns = self.get_pk_columns(table_name)
        all_columns = dbh.get_all_columns(
            self.db_tools.get_current_connection(), table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            return

        row_data = self.generate_row(table_name)
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in pk_columns]
        )
        update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"

        for i, pk_col in enumerate(pk_columns):
            row_data[pk_col] = key_to_update[i]

        self.db_tools.execute_sql(update_sql, row_data, timed=False)
        self.track_modified_key(key_to_update)

    def _delete_without_timing(self, table_name: str) -> None:
        """Delete without timing (for setup)."""
        key_to_delete = self.select_random_key(table_name)
        if not key_to_delete:
            return

        pk_columns = self.get_pk_columns(table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        delete_sql = f"DELETE FROM {table_name} WHERE {where_clause};"

        self.db_tools.execute_sql(delete_sql, key_to_delete, timed=False)
        self.untrack_modified_key(key_to_delete)


# ============================================================================
# Operation Execution
# ============================================================================


class OperationRunner:
    """Creates and executes operations using the registry pattern."""

    def __init__(self, config: BenchmarkConfig, context: WorkerContext):
        self.config = config
        self.context = context

        # Create the operation instance
        self.operation = self._create_operation()

    def _create_operation(self):
        """Create the operation instance based on config."""
        op_type = self.config.operation_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    def execute_single(self) -> None:
        """Execute the operation once."""
        try:
            self.operation.execute(self.context)
        except Exception as e:
            if self.context.shared_progress:
                self.context.shared_progress.write(
                    f"[Thread {self.context.thread_id}] Operation failed: {e}"
                )
            raise

    def execute_multiple(self, num_ops: int, warmup_ops: int = 0) -> None:
        """Execute the operation N times with optional warm-up.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
        """
        # Execute warm-up operations (if any)
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations per thread...")

            for i in range(warmup_ops):
                try:
                    self.operation.execute(self.context)
                except Exception as e:
                    # Build detailed error message for warmup
                    op_name = self.context.config.operation_name
                    error_type = type(e).__name__
                    msg = f"[Thread {self.context.thread_id}] Warm-up operation {i+1}/{warmup_ops} failed: {op_name} - {error_type}: {e}"
                    if self.context.shared_progress:
                        self.context.shared_progress.write(msg)
                    else:
                        print(msg)

            # Clear warm-up results from collector
            self.context.result_collector.reset()

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        # Execute timed operations
        for i in range(num_ops):
            try:
                self.operation.execute(self.context)

                # Update progress
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

            except Exception as e:
                # Record the failure with details
                self.context.result_collector.record_failure(error=e, operation_number=i+1)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Operation {i+1}/{num_ops} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)


class AsyncOperationRunner:
    """Executes operations asynchronously with configurable concurrency.

    This runner uses asyncio to execute multiple operations concurrently
    within a single connection, controlled by a semaphore to limit the
    number of concurrent requests.

    Each operation is timed independently with accurate start/end timestamps,
    ensuring correct performance measurement even with overlapping execution.
    """

    def __init__(self, config: BenchmarkConfig, context: WorkerContext):
        self.config = config
        self.context = context
        self.operation = self._create_operation()
        self.concurrent_limit = config.concurrent_requests

    def _create_operation(self):
        """Create the operation instance based on config."""
        # Reuse the same operation creation logic as OperationRunner
        op_type = self.config.operation_type
        table_name = self.config.table_name

        # Operation-specific parameters
        if op_type in [tp.OperationType.RANGE_READ, tp.OperationType.RANGE_UPDATE]:
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                range_size=self.config.range_size
            )
        elif op_type == tp.OperationType.DDL_ADD_INDEX:
            column_name = self.config.ddl_config.index_column_name or None
            return OperationRegistry.create(
                op_type,
                table_name=table_name,
                column_name=column_name
            )
        elif op_type == tp.OperationType.DDL_REMOVE_INDEX:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type == tp.OperationType.DDL_VACUUM:
            return OperationRegistry.create(op_type, table_name=table_name)
        elif op_type in [
            tp.OperationType.READ,
            tp.OperationType.INSERT,
            tp.OperationType.UPDATE,
            tp.OperationType.DELETE,
        ]:
            return OperationRegistry.create(op_type, table_name=table_name)
        else:
            # Branch operations and others without parameters
            return OperationRegistry.create(op_type)

    async def _execute_single_async(self, semaphore: asyncio.Semaphore, op_number: int) -> dict:
        """Execute a single operation asynchronously with semaphore control.

        Args:
            semaphore: Semaphore to limit concurrent execution
            op_number: Operation number for error reporting

        Returns:
            dict with status information for debugging
        """
        async with semaphore:
            try:
                # Check if operation has async version
                if hasattr(self.operation, 'execute_async'):
                    await self.operation.execute_async(self.context)
                else:
                    # Fall back to running sync operation in thread pool
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, self.operation.execute, self.context)

                # Update progress
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

                return {"status": "success", "op_number": op_number}

            except Exception as e:
                # Record the failure
                self.context.result_collector.record_failure(error=e, operation_number=op_number)

                # Build detailed error message
                op_name = self.context.config.operation_name
                error_type = type(e).__name__
                msg = f"[Thread {self.context.thread_id}] Async operation {op_number} failed: {op_name} - {error_type}: {e}"

                if self.context.shared_progress:
                    self.context.shared_progress.write(msg)
                else:
                    print(msg)

                # Update progress even for failed ops
                if self.context.shared_progress:
                    self.context.shared_progress.update(1)

                return {"status": "failed", "op_number": op_number, "error": str(e)}

    async def _ensure_async_connection(self):
        """Ensure async connection is initialized for the database tools."""
        if self.context.db_tools.async_conn:
            return  # Already initialized

        # Import psycopg for async connections
        try:
            import psycopg
        except ImportError:
            raise ImportError(
                "psycopg (v3) is required for async mode. Install with: pip install 'psycopg[binary]>=3.0'"
            )

        backend = self.config.backend
        db_name = self.config.database_setup.db_name

        # For Neon and Dolt, we need to get the full URI with credentials
        # because DSN from psycopg2 doesn't include the password
        if backend == tp.Backend.NEON:
            from dblib.neon import NeonToolSuite

            # NeonToolSuite stores project_id and current_branch_id
            if not isinstance(self.context.db_tools, NeonToolSuite):
                raise ValueError("Expected NeonToolSuite for Neon backend")

            neon_tools = self.context.db_tools
            uri = NeonToolSuite._get_neon_connection_uri(
                project_id=neon_tools.project_id,
                branch_id=neon_tools.current_branch_id,
                db_name=db_name
            )
        elif backend == tp.Backend.DOLT:
            # DoltgreSQL uses PostgreSQL protocol (psycopg2)
            # Reconstruct URI with credentials from environment variables
            from dblib.dolt import DoltToolSuite
            uri = DoltToolSuite.get_initial_connection_uri(db_name)
        elif backend == tp.Backend.KPG:
            # KPG also needs full URI with credentials
            from dblib.kpg import KpgToolSuite
            uri = KpgToolSuite.get_initial_connection_uri(db_name)
        else:
            # For other backends, try to get DSN from connection
            conn = self.context.db_tools.get_current_connection()

            if not conn:
                raise ValueError("No active connection found to create async connection from")

            # Get DSN (Data Source Name) from the connection
            # psycopg2 connections have a `dsn` attribute
            if hasattr(conn, 'dsn'):
                uri = conn.dsn
            elif hasattr(conn, 'info') and hasattr(conn.info, 'dsn'):
                uri = conn.info.dsn
            else:
                raise ValueError(
                    f"Cannot determine connection URI for backend: {backend}. "
                    f"Connection object has no DSN attribute."
                )

        # Create async connection with the same URI as the sync connection
        async_conn = await psycopg.AsyncConnection.connect(uri, autocommit=True)
        self.context.db_tools.async_conn = async_conn

    async def execute_multiple_async(self, num_ops: int, warmup_ops: int = 0) -> None:
        """Execute operations asynchronously with configurable concurrency.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first (not counted in results)
        """
        # Ensure async connection is initialized
        await self._ensure_async_connection()

        # Create semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(self.concurrent_limit)

        # Execute warm-up operations (if any)
        if warmup_ops > 0:
            if self.context.thread_id == 0:
                print(f"Executing {warmup_ops} warm-up operations with concurrency={self.concurrent_limit}...")

            warmup_tasks = [
                self._execute_single_async(semaphore, i+1)
                for i in range(warmup_ops)
            ]
            await asyncio.gather(*warmup_tasks, return_exceptions=True)

            # Clear warm-up results from collector
            self.context.result_collector.reset()

            if self.context.thread_id == 0:
                print(f"Warm-up complete. Starting timed measurement of {num_ops} operations...")

        # Execute timed operations concurrently
        tasks = [
            self._execute_single_async(semaphore, i+1)
            for i in range(num_ops)
        ]

        # Wait for all operations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Analyze results
        successes = [r for r in results if isinstance(r, dict) and r.get("status") == "success"]
        failures = [r for r in results if isinstance(r, dict) and r.get("status") == "failed"]
        exceptions = [r for r in results if isinstance(r, Exception)]
        nones = [r for r in results if r is None]

        # Record any uncaught exceptions as failures
        # These are exceptions that escaped the try/except in _execute_single_async
        if exceptions:
            for exc in exceptions:
                self.context.result_collector.record_failure(
                    error=exc,
                    operation_number=None
                )

        # Check for issues
        has_failures = len(failures) > 0
        has_exceptions = len(exceptions) > 0
        has_nones = len(nones) > 0
        total_returned = len(results)
        total_accounted = len(successes) + len(failures) + len(exceptions) + len(nones)
        has_accounting_error = total_accounted != total_returned
        has_missing_results = total_returned != num_ops

        # Only print detailed output if this thread has issues
        has_issues = (has_failures or has_exceptions or has_nones or
                     has_accounting_error or has_missing_results)

        if has_issues:
            print(f"[Thread {self.context.thread_id}] WARNING: Issues detected during async execution")
            print(f"  Tasks created: {num_ops}")
            print(f"  Successes: {len(successes)}/{num_ops} ({100*len(successes)/num_ops:.1f}%)")

            if has_failures:
                print(f"  Failures (caught): {len(failures)}")

            if has_exceptions:
                print(f"  Exceptions (uncaught): {len(exceptions)}")
                # Print first 5 exceptions with stack traces
                for i, exc in enumerate(exceptions[:5]):
                    print(f"    Exception {i+1}: {type(exc).__name__}: {exc}")
                    import traceback
                    traceback.print_exception(type(exc), exc, exc.__traceback__)

            if has_nones:
                print(f"  None values: {len(nones)} (unexpected!)")

            if has_accounting_error:
                print(f"  ERROR: Accounted results ({total_accounted}) != returned results ({total_returned})")

            if has_missing_results:
                print(f"  WARNING: Results returned ({total_returned}) != tasks created ({num_ops})")

        # Clean up task-local storage now that all operations are complete
        self.context.result_collector.cleanup_task_local_storage()

        # Return stats for aggregation
        return {
            "thread_id": self.context.thread_id,
            "num_ops": num_ops,
            "successes": len(successes),
            "failures": len(failures),
            "exceptions": len(exceptions),
            "nones": len(nones),
            "has_issues": has_issues
        }

    def execute_multiple(self, num_ops: int, warmup_ops: int = 0) -> dict:
        """Synchronous wrapper that runs async execution in event loop.

        Args:
            num_ops: Number of operations to execute (timed)
            warmup_ops: Number of warm-up operations to execute first

        Returns:
            dict with execution statistics (successes, failures, etc.)
        """
        # Run the async execution in the event loop
        return asyncio.run(self.execute_multiple_async(num_ops, warmup_ops))


# ============================================================================
# Benchmark Execution
# ============================================================================


class BenchmarkExecutor:
    """Orchestrates benchmark execution with thread management."""

    def __init__(
        self,
        config: BenchmarkConfig,
        backend_info: BackendInfo,
        setup_branches: List[str],
        result_collector: rc.ResultCollector,
    ):
        self.config = config
        self.backend_info = backend_info
        self.setup_branches = setup_branches
        self.result_collector = result_collector

        # Shared state
        self.branch_manager = SharedBranchManager(initial_branches=setup_branches)

    def execute(self) -> dict:
        """Execute the benchmark and return metrics.

        Returns:
            dict with timing and throughput metrics
        """
        num_threads = self.config.num_threads
        num_ops = self.config.num_ops
        total_ops = num_ops * num_threads

        print(f"\n{'='*60}")
        print(f"Benchmark Execution (TIMED)")
        print(f"{'='*60}")
        print(f"Operation: {self.config.operation_name}")
        print(f"Threads: {num_threads}")
        print(f"Operations per thread: {num_ops}")
        print(f"Total operations: {total_ops}")

        # Create shared progress bar
        shared_progress = SharedProgress(
            total=total_ops,
            desc=f"Benchmark ({num_threads} threads)",
            disable=False,
        )

        # Partition branches among threads
        thread_branch_assignments = self._assign_branches_to_threads()

        # Fixed seed for reproducibility
        fixed_seed = int(time.time())
        print(f"Seed: {fixed_seed}")
        print(f"{'='*60}\n")

        # Track start time
        start_time = time.time()

        try:
            if num_threads == 1:
                # Single-threaded execution
                self._execute_single_threaded(
                    fixed_seed, thread_branch_assignments.get(0, []), shared_progress
                )
            else:
                # Multi-threaded execution
                self._execute_multi_threaded(
                    fixed_seed, thread_branch_assignments, shared_progress
                )
        finally:
            shared_progress.close()

        # Calculate metrics
        end_time = time.time()
        elapsed_time = end_time - start_time

        metrics = self._calculate_metrics(total_ops, elapsed_time)
        self._print_metrics(metrics)

        return metrics

    def _assign_branches_to_threads(self) -> Dict[int, List[str]]:
        """Assign branches to threads for multi-threaded execution."""
        num_threads = self.config.num_threads
        if not self.setup_branches or num_threads == 1:
            return {}

        assignments = {}
        num_branches = len(self.setup_branches)

        if num_threads <= num_branches:
            # Round-robin assignment
            for tid in range(num_threads):
                assignments[tid] = [
                    self.setup_branches[idx]
                    for idx in range(tid, num_branches, num_threads)
                ]
        else:
            # More threads than branches: cyclic assignment
            for tid in range(num_threads):
                assigned_branch = self.setup_branches[tid % num_branches]
                assignments[tid] = [assigned_branch]

        return assignments

    def _execute_single_threaded(
        self, seed: int, assigned_branches: List[str], progress: SharedProgress
    ) -> None:
        """Execute benchmark in single thread."""
        rc.set_current_thread_id(0)

        with WorkerContext(
            config=self.config,
            backend_info=self.backend_info,
            thread_id=0,
            seed=seed,
            result_collector=self.result_collector,
            branch_manager=self.branch_manager,
            shared_progress=progress,
            assigned_branches=assigned_branches,
        ) as ctx:
            # Initialize table name if needed
            if not self.config.table_name:
                all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                self.config._proto.table_name = ctx.rnd.choice(all_tables)

            # Set context for result collection
            table_name = self.config.table_name
            table_schema = ctx.db_tools.get_table_schema(table_name)
            initial_db_size = 0
            try:
                initial_db_size = dbh.get_db_size(ctx.db_tools.get_current_connection())
            except Exception:
                pass

            ctx.db_tools.result_collector.set_context(
                table_name=table_name,
                table_schema=table_schema,
                initial_db_size=initial_db_size,
                seed=seed,
            )

            # Create and execute operations
            # Use async runner if concurrent_requests > 1, otherwise sync runner
            if self.config.concurrent_requests > 1:
                runner = AsyncOperationRunner(self.config, ctx)
                stats = runner.execute_multiple(self.config.num_ops, self.config.warmup_ops)
                # Print aggregate summary for async execution
                self._print_aggregate_summary([stats])
            else:
                runner = OperationRunner(self.config, ctx)
                runner.execute_multiple(self.config.num_ops, self.config.warmup_ops)

    def _execute_multi_threaded(
        self,
        fixed_seed: int,
        branch_assignments: Dict[int, List[str]],
        progress: SharedProgress,
    ) -> None:
        """Execute benchmark with multiple threads."""

        def worker_fn(thread_id: int, assigned_branches: List[str]):
            """Worker function for each thread.

            Returns:
                dict with stats if using async runner, None otherwise
            """
            rc.set_current_thread_id(thread_id)
            worker_seed = fixed_seed + thread_id

            with WorkerContext(
                config=self.config,
                backend_info=self.backend_info,
                thread_id=thread_id,
                seed=worker_seed,
                result_collector=self.result_collector,
                branch_manager=self.branch_manager,
                shared_progress=progress,
                assigned_branches=assigned_branches,
            ) as ctx:
                # Initialize table name if needed
                if not self.config.table_name:
                    all_tables = dbh.get_all_tables(ctx.db_tools.get_current_connection())
                    self.config._proto.table_name = ctx.rnd.choice(all_tables)

                # Set context
                table_name = self.config.table_name
                table_schema = ctx.db_tools.get_table_schema(table_name)
                initial_db_size = 0
                try:
                    initial_db_size = dbh.get_db_size(ctx.db_tools.get_current_connection())
                except Exception:
                    pass

                ctx.db_tools.result_collector.set_context(
                    table_name=table_name,
                    table_schema=table_schema,
                    initial_db_size=initial_db_size,
                    seed=worker_seed,
                )

                # Create and execute operations
                # Use async runner if concurrent_requests > 1, otherwise sync runner
                if self.config.concurrent_requests > 1:
                    runner = AsyncOperationRunner(self.config, ctx)
                    return runner.execute_multiple(self.config.num_ops, self.config.warmup_ops)
                else:
                    runner = OperationRunner(self.config, ctx)
                    runner.execute_multiple(self.config.num_ops, self.config.warmup_ops)
                    return None

        # Execute with thread pool
        num_threads = self.config.num_threads
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(
                    worker_fn,
                    thread_id=tid,
                    assigned_branches=branch_assignments.get(tid, []),
                )
                for tid in range(num_threads)
            ]

            # Wait for completion and collect stats
            thread_stats = []
            for future in as_completed(futures):
                try:
                    stats = future.result()
                    if stats is not None:  # Async runner returns stats
                        thread_stats.append(stats)
                except Exception as e:
                    print(f"Worker thread failed: {e}")

            # Print aggregate summary for async execution
            if thread_stats:
                self._print_aggregate_summary(thread_stats)

    def _calculate_metrics(self, total_ops: int, elapsed_time: float) -> dict:
        """Calculate throughput and latency metrics.

        Note: total_ops is the intended count (num_ops * num_threads).
        Actual successful operation count is len(self.result_collector.results).
        """
        # Count actual successful operations (not intended count)
        actual_ops = len(self.result_collector.results)
        failed_ops = len(self.result_collector.failed_operations)
        throughput = actual_ops / elapsed_time if elapsed_time > 0 else 0

        # Calculate data operation throughput (excluding branch ops)
        data_ops = [
            r
            for r in self.result_collector.results
            if r.op_type not in [rslt.OpType.BRANCH_CREATE, rslt.OpType.BRANCH_CONNECT]
        ]
        data_ops_count = len(data_ops)
        data_ops_time = sum(r.latency for r in data_ops) if data_ops else 0
        data_throughput = data_ops_count / data_ops_time if data_ops_time > 0 else 0

        return {
            "intended_ops": total_ops,  # Intended number of operations
            "total_ops": actual_ops,  # Actual successful operations (all types)
            "failed_ops": failed_ops,  # Number of failed operations
            "elapsed_time": elapsed_time,
            "throughput": throughput,  # Based on actual successful ops
            "data_ops_count": data_ops_count,
            "data_ops_time": data_ops_time,
            "data_throughput": data_throughput,
        }

    def _print_aggregate_summary(self, thread_stats: list) -> None:
        """Print aggregate summary of async execution across all threads.

        Args:
            thread_stats: List of dicts with per-thread statistics
        """
        if not thread_stats:
            return

        # Aggregate stats across all threads
        total_ops = sum(s['num_ops'] for s in thread_stats)
        total_successes = sum(s['successes'] for s in thread_stats)
        total_failures = sum(s['failures'] for s in thread_stats)
        total_exceptions = sum(s['exceptions'] for s in thread_stats)
        total_nones = sum(s['nones'] for s in thread_stats)
        threads_with_issues = sum(1 for s in thread_stats if s['has_issues'])

        print(f"\n{'='*80}")
        print(f"Async Execution Summary ({len(thread_stats)} thread(s)):")
        print(f"  Total operations: {total_ops:,}")
        print(f"  Successes: {total_successes:,} ({100*total_successes/total_ops:.2f}%)" if total_ops > 0 else "  Successes: 0")

        if total_failures > 0:
            print(f"  Failures (caught): {total_failures:,} ({100*total_failures/total_ops:.2f}%)")

        if total_exceptions > 0:
            print(f"  Exceptions (uncaught): {total_exceptions:,} ({100*total_exceptions/total_ops:.2f}%)")

        if total_nones > 0:
            print(f"  None values: {total_nones:,}")

        if threads_with_issues > 0:
            print(f"  Threads with issues: {threads_with_issues}/{len(thread_stats)}")

        print(f"{'='*80}\n")

    def _print_metrics(self, metrics: dict) -> None:
        """Print benchmark metrics."""
        print(f"\n{'='*60}")
        print(f"Benchmark Results:")
        print(f"  Intended operations: {metrics['intended_ops']}")
        print(f"  Successful operations: {metrics['total_ops']}")
        print(f"  Failed operations: {metrics['failed_ops']}", end="")
        if metrics['failed_ops'] > 0 and metrics['intended_ops'] > 0:
            print(f" ({100*metrics['failed_ops']/metrics['intended_ops']:.1f}%)")
        else:
            print()
        print(f"  Total time: {metrics['elapsed_time']:.2f}s")
        print(f"  Throughput: {metrics['throughput']:.2f} ops/sec (successful ops only)")
        print(f"\n  Data operations only (excluding branch ops):")
        print(f"    Count: {metrics['data_ops_count']}")
        print(f"    Time: {metrics['data_ops_time']:.2f}s")
        print(f"    Throughput: {metrics['data_throughput']:.2f} ops/sec")
        print(f"{'='*60}\n")


# ============================================================================
# Result Management
# ============================================================================


class ResultManager:
    """Manages result writing to parquet and JSON."""

    def __init__(
        self,
        config: BenchmarkConfig,
        result_collector: rc.ResultCollector,
        metrics: dict,
        output_dir: str,
    ):
        self.config = config
        self.result_collector = result_collector
        self.metrics = metrics
        self.output_dir = output_dir

    def write_results(self) -> None:
        """Write all results to files."""
        # Write main parquet file
        self.result_collector.write_to_parquet()

        # Write summary JSON
        self._write_summary_json()

        # Write failure details if any failures occurred
        if self.result_collector.failed_operations:
            self._write_failures_json()

    def _write_summary_json(self) -> None:
        """Write summary JSON with throughput metrics."""
        summary = {
            "run_id": self.config.run_id,
            "backend": self.config.backend_name,
            "operation": self.config.operation_name,
            "num_ops": self.config.num_ops,
            "num_threads": self.config.num_threads,
            "num_branches": self.config.num_branches,
            "scale_factor": self.config.scale_factor,
            "intended_ops": self.metrics["intended_ops"],  # num_ops * num_threads
            "total_ops": self.metrics["total_ops"],  # Actual successful operations
            "failed_ops": self.metrics["failed_ops"],  # Failed operations
            "elapsed_time": self.metrics["elapsed_time"],
            "throughput": self.metrics["throughput"],  # Based on successful ops only
            "data_ops_count": self.metrics["data_ops_count"],
            "data_ops_time": self.metrics["data_ops_time"],
            "data_throughput": self.metrics["data_throughput"],
        }

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        summary_filename = "_".join(filename_parts) + "_summary.json"
        summary_path = os.path.join(self.output_dir, summary_filename)

        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"Summary written to: {summary_path}")

    def _write_failures_json(self) -> None:
        """Write detailed failure information to a separate JSON file."""
        failures_data = {
            "run_id": self.config.run_id,
            "backend": self.config.backend_name,
            "operation": self.config.operation_name,
            "total_failures": len(self.result_collector.failed_operations),
            "failures": self.result_collector.failed_operations,
        }

        # Build filename
        filename_parts = [
            self.config.run_id,
            self.config.operation_name,
            f"threads{self.config.num_threads}",
        ]
        failures_filename = "_".join(filename_parts) + "_failures.json"
        failures_path = os.path.join(self.output_dir, failures_filename)

        with open(failures_path, "w") as f:
            json.dump(failures_data, f, indent=2)

        print(f"Failure details written to: {failures_path}")


# ============================================================================
# Operation Registration
# ============================================================================


def register_all_operations() -> None:
    """Register all available operations with the registry."""
    # CRUD operations
    OperationRegistry.register(tp.OperationType.READ, ReadOperation)
    OperationRegistry.register(tp.OperationType.INSERT, InsertOperation)
    OperationRegistry.register(tp.OperationType.UPDATE, UpdateOperation)
    OperationRegistry.register(tp.OperationType.DELETE, DeleteOperation)
    OperationRegistry.register(tp.OperationType.RANGE_READ, RangeReadOperation)
    OperationRegistry.register(tp.OperationType.RANGE_UPDATE, RangeUpdateOperation)

    # Branch operations
    OperationRegistry.register(tp.OperationType.BRANCH_CREATE, BranchCreateOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_CONNECT, BranchConnectOperation)
    OperationRegistry.register(tp.OperationType.BRANCH_DELETE, BranchDeleteOperation)
    OperationRegistry.register(tp.OperationType.CONNECT_FIRST, ConnectFirstOperation)
    OperationRegistry.register(tp.OperationType.CONNECT_MID, ConnectMidOperation)
    OperationRegistry.register(tp.OperationType.CONNECT_LAST, ConnectLastOperation)

    # DDL operations
    OperationRegistry.register(tp.OperationType.DDL_ADD_INDEX, AddIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_REMOVE_INDEX, RemoveIndexOperation)
    OperationRegistry.register(tp.OperationType.DDL_VACUUM, VacuumOperation)


# ============================================================================
# Main Orchestration
# ============================================================================


def main():
    """Main entry point for runner2."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Run database microbenchmarks from task2 config."
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to task2 configuration file (.textproto)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp/run_stats",
        help="Directory for output files (default: /tmp/run_stats)",
    )
    args = parser.parse_args()

    # Register all operations
    register_all_operations()

    # Load configuration
    try:
        config = BenchmarkConfig.load_from_file(args.config)
        print(f"Loaded configuration: {config}")
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Create shared result collector
    result_collector = rc.ResultCollector(
        run_id=config.run_id, output_dir=args.output_dir
    )

    try:
        # 1. Setup backend
        backend_mgr = BackendManager(config, args.output_dir)
        backend_info = backend_mgr.setup()

        # 2. Setup phase (UNTIMED)
        if config.num_branches > 0:
            setup_phase = SetupPhase(config, backend_info, args.output_dir)
            setup_branches = setup_phase.execute()
        else:
            setup_branches = []
            backend_info.setup_branches = []

        # 3. Execute benchmark (TIMED)
        executor = BenchmarkExecutor(
            config, backend_info, setup_branches, result_collector
        )
        metrics = executor.execute()

        # 4. Write results
        result_mgr = ResultManager(config, result_collector, metrics, args.output_dir)
        result_mgr.write_results()

    finally:
        # 5. Cleanup
        if backend_mgr:
            backend_mgr.cleanup()

    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()
