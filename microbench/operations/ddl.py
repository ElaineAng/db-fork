"""
DDL (Data Definition Language) operations for benchmarking.

This module implements database schema modification operations such as
adding/removing indexes, adding/removing columns, and maintenance operations
like VACUUM.
"""

from typing import TYPE_CHECKING, Optional
import threading

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation
from util import db_helpers as dbh

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class AddIndexOperation(Operation):
    """Add an index to a table column.

    Creates a new index on a specified column. Index names are automatically
    generated to avoid collisions using a thread-safe counter.
    """

    # Class-level counter for unique index names (thread-safe)
    _index_counter = 0
    _counter_lock = threading.Lock()

    def __init__(self, table_name: str, column_name: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name

    def _prepare_index_creation(self, context: 'WorkerContext'):
        """Shared logic: prepare index creation SQL.

        Returns:
            Tuple of (create_index_sql, index_name)
        """
        # Get column name from config if not provided
        column_name = self.column_name
        if not column_name:
            # If no column specified, pick a random non-PK column
            all_columns = dbh.get_all_columns(
                context.db_tools.get_current_connection(), self.table_name
            )
            pk_columns = context.get_pk_columns(self.table_name)
            non_pk_columns = [col for col in all_columns if col not in pk_columns]

            if not non_pk_columns:
                raise ValueError(f"No non-PK columns available for index in {self.table_name}")

            column_name = context.rnd.choice(non_pk_columns)

        # Generate unique index name (thread-safe)
        with AddIndexOperation._counter_lock:
            AddIndexOperation._index_counter += 1
            idx_num = AddIndexOperation._index_counter

        index_name = f"idx_{self.table_name}_{column_name}_{idx_num}"

        # Create the index SQL
        create_index_sql = f"CREATE INDEX {index_name} ON {self.table_name}({column_name})"

        return (create_index_sql, index_name)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed index creation operation."""
        create_index_sql, index_name = self._prepare_index_creation(context)

        # Create the index (timed)
        context.db_tools.execute_sql(create_index_sql, timed=True)

        # Track the created index
        context.track_created_index(self.table_name, index_name)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        create_index_sql, index_name = self._prepare_index_creation(context)

        # Create the index asynchronously (timed)
        await context.db_tools.execute_sql_async(create_index_sql, timed=True)

        # Track the created index
        context.track_created_index(self.table_name, index_name)

    def requires_setup_data(self) -> bool:
        return True  # Needs table to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class RemoveIndexOperation(Operation):
    """Remove an index from a table.

    Drops a random index from the table. Excludes primary key indexes
    and unique constraints.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed index drop operation."""
        # Get a random index from this table
        index_name = context.get_random_index(self.table_name)

        if not index_name:
            raise ValueError(f"No indexes available to drop on {self.table_name}")

        # Drop the index (timed)
        drop_index_sql = f"DROP INDEX {index_name}"
        context.db_tools.execute_sql(drop_index_sql, timed=True)

        # Remove from tracking
        context.untrack_index(self.table_name, index_name)

    def requires_setup_data(self) -> bool:
        return True  # Needs indexes to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class VacuumOperation(Operation):
    """Run VACUUM on a table or database.

    Performs maintenance to reclaim storage and update statistics.
    This operation can be expensive and is typically used to measure
    the cost of database maintenance.
    """

    def __init__(self, table_name: Optional[str] = None):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed VACUUM operation."""
        # VACUUM syntax varies by database
        # PostgreSQL: VACUUM [table_name]
        # MySQL doesn't have VACUUM, uses OPTIMIZE TABLE
        # Dolt supports VACUUM-like operations

        if self.table_name:
            vacuum_sql = f"VACUUM {self.table_name}"
        else:
            vacuum_sql = "VACUUM"

        try:
            # Execute the timed vacuum
            context.db_tools.execute_sql(vacuum_sql, timed=True)
        except Exception as e:
            # Some databases or configurations may not support VACUUM
            # Log and re-raise with more context
            raise ValueError(
                f"VACUUM operation failed (may not be supported by backend): {e}"
            )

    def requires_setup_data(self) -> bool:
        return True  # Needs data to vacuum

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class AddColumnOperation(Operation):
    """Add a new column to a table.

    Adds a column with a specified name and type. This operation measures
    the cost of schema evolution (ALTER TABLE ADD COLUMN).
    """

    # Class-level counter for unique column names
    _column_counter = 0
    _counter_lock = threading.Lock()

    def __init__(
        self,
        table_name: str,
        column_name: Optional[str] = None,
        column_type: str = "INTEGER"
    ):
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed column addition operation."""
        # Generate unique column name if not provided
        column_name = self.column_name
        if not column_name:
            with AddColumnOperation._counter_lock:
                AddColumnOperation._column_counter += 1
                col_num = AddColumnOperation._column_counter
            column_name = f"col_added_{col_num}"

        # Add the column (timed)
        add_column_sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD COLUMN {column_name} {self.column_type}"
        )
        context.db_tools.execute_sql(add_column_sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True  # Needs table to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class RemoveColumnOperation(Operation):
    """Remove a column from a table.

    Drops a specified column from the table. This operation measures
    the cost of schema evolution (ALTER TABLE DROP COLUMN).
    """

    def __init__(self, table_name: str, column_name: str):
        self.table_name = table_name
        self.column_name = column_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed column removal operation."""
        # Drop the column (timed)
        drop_column_sql = (
            f"ALTER TABLE {self.table_name} DROP COLUMN {self.column_name}"
        )
        context.db_tools.execute_sql(drop_column_sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True  # Needs table and column to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL
