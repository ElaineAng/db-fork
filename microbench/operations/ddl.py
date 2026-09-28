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

        # Track it so REMOVE_COLUMN only ever drops benchmark-created columns
        context.track_created_column(self.table_name, column_name)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version of column addition."""
        column_name = self.column_name
        if not column_name:
            with AddColumnOperation._counter_lock:
                AddColumnOperation._column_counter += 1
                col_num = AddColumnOperation._column_counter
            column_name = f"col_added_{col_num}"

        add_column_sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD COLUMN {column_name} {self.column_type}"
        )
        await context.db_tools.execute_sql_async(add_column_sql, timed=True)
        context.track_created_column(self.table_name, column_name)

    def requires_setup_data(self) -> bool:
        return True  # Needs table to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class RemoveColumnOperation(Operation):
    """Remove a column from a table.

    Drops a specified column from the table. This operation measures
    the cost of schema evolution (ALTER TABLE DROP COLUMN).
    """

    def __init__(self, table_name: str, column_name: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name

    def _resolve_column(self, context: 'WorkerContext') -> str:
        """Pick the column to drop.

        If no column is configured, drop a column this benchmark created.
        We deliberately never drop an original schema column: dropping a
        TPC-C column would corrupt the dataset for every subsequent
        operation in the run.
        """
        if self.column_name:
            return self.column_name

        column_name = context.get_random_created_column(self.table_name)
        if not column_name:
            raise ValueError(
                f"No benchmark-created column available to drop on "
                f"{self.table_name}. Run DDL_ADD_COLUMN first, or set "
                f"ddl_config.column_name explicitly."
            )
        return column_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed column removal operation."""
        column_name = self._resolve_column(context)

        # Drop the column (timed)
        drop_column_sql = (
            f"ALTER TABLE {self.table_name} DROP COLUMN {column_name}"
        )
        context.db_tools.execute_sql(drop_column_sql, timed=True)

        context.untrack_column(self.table_name, column_name)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version of column removal."""
        column_name = self._resolve_column(context)

        drop_column_sql = (
            f"ALTER TABLE {self.table_name} DROP COLUMN {column_name}"
        )
        await context.db_tools.execute_sql_async(drop_column_sql, timed=True)

        context.untrack_column(self.table_name, column_name)

    def requires_setup_data(self) -> bool:
        return True  # Needs table and column to exist

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class BackfillOperation(Operation):
    """Rewrite an existing column across a fraction of the table's rows.
 
    This is the row-rewriting half of schema evolution: adding a column is
    usually metadata-only, but populating it touches every affected row. In
    a mining study of 11,562 DDL operations across six production OSS
    projects, backfills were 10.5% of all schema changes - and they are the
    operation most responsible for storage amplification on copy-on-write
    and versioned storage engines.
 
    Cost is therefore parameterised by *rows rewritten*, not by statement
    count: backfill_fraction controls what share of the table is updated.
    """
 
    def __init__(
        self,
        table_name: str,
        backfill_fraction: float = 1.0,
        column_name: Optional[str] = None,
    ):
        self.table_name = table_name
        self.backfill_fraction = max(0.0, min(1.0, backfill_fraction))
        self.column_name = column_name
 
    _NUMERIC_TYPES = {"integer", "bigint", "smallint", "numeric",
                      "double precision", "real", "decimal"}
    _TEXT_TYPES = {"character varying", "character", "text"}

    def _resolve_column(self, context: 'WorkerContext') -> tuple:
        """Pick the column to rewrite, and return it with its data type.

        Never a primary key (breaks row identity), never a foreign key
        (the generated value would violate referential integrity), and only
        types we can generate a valid value for.
        """
        conn = context.db_tools.get_current_connection()
        col_types = dbh.get_column_types(conn, self.table_name)

        if self.column_name:
            return self.column_name, col_types.get(self.column_name, "integer")

        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked, col_types.get(tracked, "integer")

        pk_columns = set(context.get_pk_columns(self.table_name))
        fk_columns = set(dbh.get_foreign_key_columns(conn, self.table_name))
        supported = self._NUMERIC_TYPES | self._TEXT_TYPES

        for name, dtype in col_types.items():
            if name in pk_columns or name in fk_columns:
                continue
            if dtype in supported:
                return name, dtype

        raise ValueError(
            f"No backfillable column in {self.table_name}: every column is "
            f"part of a key or has a type this operation cannot generate "
            f"values for. Set ddl_config.column_name explicitly, or run "
            f"DDL_ADD_COLUMN first."
        )

    def _generate_value(self, context: 'WorkerContext', dtype: str):
        """Produce a value valid for the target column's type and width.

        Integer widths matter: smallint tops out at 32,767, so a generic
        random integer silently fails on narrow columns.
        """
        if dtype in self._TEXT_TYPES:
            return f"bf_{context.rnd.randint(0, 100_000)}"
        if dtype == "smallint":
            return context.rnd.randint(0, 32_000)
        if dtype == "integer":
            return context.rnd.randint(0, 1_000_000)
        if dtype in ("bigint",):
            return context.rnd.randint(0, 1_000_000_000)
        # numeric / real / double precision
        return context.rnd.randint(0, 1_000_000)

    def _prepare_backfill(self, context: 'WorkerContext'):
        """Build the UPDATE statement and its parameters."""
        column_name, dtype = self._resolve_column(context)

        total_keys = context.get_existing_key_count(self.table_name)
        if total_keys == 0:
            raise ValueError(
                f"No rows to backfill in {self.table_name}. The table is "
                f"empty - check inserts_per_branch in the setup config."
            )

        rows_targeted = max(1, int(round(self.backfill_fraction * total_keys)))
        range_info = context.prepare_range_query(
            self.table_name, rows_targeted, "backfill"
        )

        params = dict(range_info["params"])
        params["_backfill_value"] = self._generate_value(context, dtype)

        sql = (
            f"UPDATE {self.table_name} "
            f"SET {column_name} = %(_backfill_value)s "
            f"WHERE {range_info['where_clause']};"
        )
        return sql, params, len(range_info["keys_in_range"])
 
    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed backfill over a fraction of the table."""
        sql, params, rows = self._prepare_backfill(context)
        context.db_tools.execute_sql(sql, params, timed=True)
 
    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version of the backfill."""
        sql, params, rows = self._prepare_backfill(context)
        await context.db_tools.execute_sql_async(sql, params, timed=True)
 
    def requires_setup_data(self) -> bool:
        return True  # Needs rows to rewrite
 
    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL
 
 
class AddColumnWithDefaultOperation(Operation):
    """Add a column WITH a default value.
 
    The interesting twin of ADD COLUMN: without a default, most engines
    treat this as a catalog-only change; with a default, some engines must
    rewrite every existing row. Running both and comparing isolates that
    implicit rewrite - the difference between the two is the cost the engine
    hides behind identical-looking DDL.
    """
 
    _column_counter = 0
    _counter_lock = threading.Lock()
 
    def __init__(
        self,
        table_name: str,
        column_name: Optional[str] = None,
        column_type: str = "INTEGER",
        default_value: str = "0",
    ):
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.default_value = default_value or "0"
 
    def _next_column_name(self) -> str:
        if self.column_name:
            return self.column_name
        with AddColumnWithDefaultOperation._counter_lock:
            AddColumnWithDefaultOperation._column_counter += 1
            col_num = AddColumnWithDefaultOperation._column_counter
        return f"col_default_{col_num}"
 
    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed column addition with a default value."""
        column_name = self._next_column_name()
        sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD COLUMN {column_name} {self.column_type} "
            f"DEFAULT {self.default_value}"
        )
        context.db_tools.execute_sql(sql, timed=True)
        context.track_created_column(self.table_name, column_name)
 
    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version of column addition with a default."""
        column_name = self._next_column_name()
        sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD COLUMN {column_name} {self.column_type} "
            f"DEFAULT {self.default_value}"
        )
        await context.db_tools.execute_sql_async(sql, timed=True)
        context.track_created_column(self.table_name, column_name)
 
    def requires_setup_data(self) -> bool:
        return True  # Needs table to exist
 
    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL
    
class TypeChangeOperation(Operation):
    """Change an existing column's type.

    Doltgres reportedly does not support ALTER TABLE ... USING, which
    Postgres needs for most non-trivial type changes. If that is
    confirmed here, the failure itself is a support-matrix result, not
    a bug in this operation.
    """

    def __init__(self, table_name: str, column_name: Optional[str] = None,
                 new_type: str = "BIGINT"):
        self.table_name = table_name
        self.column_name = column_name
        self.new_type = new_type

    def _resolve_column(self, context: 'WorkerContext') -> str:
        if self.column_name:
            return self.column_name
        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked
        raise ValueError(
            f"No column configured for type change on {self.table_name}. "
            f"Run DDL_ADD_COLUMN first, or set ddl_config.column_name."
        )

    def execute(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} TYPE {self.new_type}"
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} TYPE {self.new_type}"
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class RenameColumnOperation(Operation):
    def __init__(self, table_name: str, column_name: Optional[str] = None,
                 new_column_name: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name
        self.new_column_name = new_column_name

    def _resolve_column(self, context: 'WorkerContext') -> str:
        if self.column_name:
            return self.column_name
        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked
        raise ValueError(
            f"No column configured to rename on {self.table_name}. "
            f"Run DDL_ADD_COLUMN first, or set ddl_config.column_name."
        )

    def execute(self, context: 'WorkerContext') -> None:
        old = self._resolve_column(context)
        new = self.new_column_name or f"{old}_renamed"
        sql = f"ALTER TABLE {self.table_name} RENAME COLUMN {old} TO {new}"
        context.db_tools.execute_sql(sql, timed=True)
        context.untrack_column(self.table_name, old)
        context.track_created_column(self.table_name, new)

    async def execute_async(self, context: 'WorkerContext') -> None:
        old = self._resolve_column(context)
        new = self.new_column_name or f"{old}_renamed"
        sql = f"ALTER TABLE {self.table_name} RENAME COLUMN {old} TO {new}"
        await context.db_tools.execute_sql_async(sql, timed=True)
        context.untrack_column(self.table_name, old)
        context.track_created_column(self.table_name, new)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class SetNotNullOperation(Operation):
    def __init__(self, table_name: str, column_name: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name

    def _resolve_column(self, context: 'WorkerContext') -> str:
        if self.column_name:
            return self.column_name
        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked
        raise ValueError(
            f"No column configured for SET NOT NULL on {self.table_name}."
        )

    def execute(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} SET NOT NULL"
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} SET NOT NULL"
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class RenameTableOperation(Operation):
    def __init__(self, table_name: str, new_table_name: Optional[str] = None):
        self.table_name = table_name
        self.new_table_name = new_table_name or f"{table_name}_renamed"

    def execute(self, context: 'WorkerContext') -> None:
        sql = f"ALTER TABLE {self.table_name} RENAME TO {self.new_table_name}"
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        sql = f"ALTER TABLE {self.table_name} RENAME TO {self.new_table_name}"
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class AddForeignKeyOperation(Operation):
    def __init__(self, table_name: str, column_name: Optional[str] = None,
                 ref_table_name: Optional[str] = None,
                 ref_column_name: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name
        self.ref_table_name = ref_table_name
        self.ref_column_name = ref_column_name

    def _resolve_column(self, context: 'WorkerContext') -> str:
        if self.column_name:
            return self.column_name
        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked
        raise ValueError(f"No column configured for ADD FK on {self.table_name}.")

    def _build_sql(self, context: 'WorkerContext') -> str:
        column_name = self._resolve_column(context)
        if not self.ref_table_name or not self.ref_column_name:
            raise ValueError(
                "ADD_FK needs ref_table_name and ref_column_name set in "
                "ddl_config, there is no sensible default for what to "
                "reference."
            )
        constraint_name = f"fk_{self.table_name}_{column_name}"
        return (
            f"ALTER TABLE {self.table_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({column_name}) "
            f"REFERENCES {self.ref_table_name}({self.ref_column_name})"
        )

    def execute(self, context: 'WorkerContext') -> None:
        sql = self._build_sql(context)
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        sql = self._build_sql(context)
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL


class AddCheckOperation(Operation):
    def __init__(self, table_name: str, check_expression: str = "1=1"):
        self.table_name = table_name
        self.check_expression = check_expression or "1=1"

    def execute(self, context: 'WorkerContext') -> None:
        constraint_name = f"chk_{self.table_name}_{id(self)}"
        sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"CHECK ({self.check_expression})"
        )
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        constraint_name = f"chk_{self.table_name}_{id(self)}"
        sql = (
            f"ALTER TABLE {self.table_name} "
            f"ADD CONSTRAINT {constraint_name} "
            f"CHECK ({self.check_expression})"
        )
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class SetDefaultOperation(Operation):
    def __init__(self, table_name: str, column_name: Optional[str] = None,
                 default_value: str = "0"):
        self.table_name = table_name
        self.column_name = column_name
        self.default_value = default_value or "0"

    def _resolve_column(self, context: 'WorkerContext') -> str:
        if self.column_name:
            return self.column_name
        tracked = context.get_random_created_column(self.table_name)
        if tracked:
            return tracked
        raise ValueError(f"No column configured for SET DEFAULT on {self.table_name}.")

    def execute(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} SET DEFAULT {self.default_value}"
        context.db_tools.execute_sql(sql, timed=True)

    async def execute_async(self, context: 'WorkerContext') -> None:
        column_name = self._resolve_column(context)
        sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} SET DEFAULT {self.default_value}"
        await context.db_tools.execute_sql_async(sql, timed=True)

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL

class ExpandContractOperation(Operation):
    """Add a column, rename it, then drop it, all as one timed sequence.

    This mirrors the pattern the mining study found production teams
    actually use instead of a direct rename: add, backfill, drop. This
    version tests the simpler add, rename, drop shape first, tracking
    success at each stage so a failure partway through is attributable
    to a specific step rather than the sequence as a whole.
    """

    _column_counter = 0
    _counter_lock = threading.Lock()

    def __init__(self, table_name: str, column_type: str = "INTEGER"):
        self.table_name = table_name
        self.column_type = column_type

    def _next_column_name(self) -> str:
        with ExpandContractOperation._counter_lock:
            ExpandContractOperation._column_counter += 1
            n = ExpandContractOperation._column_counter
        return f"seq_col_{n}"

    def execute(self, context: 'WorkerContext') -> None:
        col = self._next_column_name()
        renamed = f"{col}_renamed"
        stage = "add"
        try:
            add_sql = f"ALTER TABLE {self.table_name} ADD COLUMN {col} {self.column_type}"
            context.db_tools.execute_sql(add_sql, timed=True)

            stage = "rename"
            rename_sql = f"ALTER TABLE {self.table_name} RENAME COLUMN {col} TO {renamed}"
            context.db_tools.execute_sql(rename_sql, timed=True)

            stage = "drop"
            drop_sql = f"ALTER TABLE {self.table_name} DROP COLUMN {renamed}"
            context.db_tools.execute_sql(drop_sql, timed=True)
        except Exception as e:
            raise ValueError(f"Sequence failed at stage '{stage}': {e}")

    def requires_setup_data(self) -> bool:
        return True

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DDL