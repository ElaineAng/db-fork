"""
CRUD (Create, Read, Update, Delete) operations for benchmarking.

This module implements the standard database CRUD operations including
point operations (single row) and range operations (multiple rows).
"""

from typing import TYPE_CHECKING

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation
from util import db_helpers as dbh

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class ReadOperation(Operation):
    """Read a single row by primary key.

    Selects a random existing primary key and performs a SELECT operation.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed point read operation."""
        # Select a random existing key
        key_to_read = context.select_random_key(self.table_name)
        if not key_to_read:
            raise ValueError("No existing keys found for read operation")

        # Build the SELECT query
        pk_columns = context.get_pk_columns(self.table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        select_sql = f"SELECT * FROM {self.table_name} WHERE {where_clause};"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        # Execute the timed read
        result = context.db_tools.execute_sql(select_sql, key_to_read, timed=True)
        if not result:
            raise ValueError("Read operation returned no results")

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to read from

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.READ


class InsertOperation(Operation):
    """Insert a new row into the table.

    Generates a new row with fake data and inserts it. Retries a few times
    if there are primary key collisions.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed insert operation."""
        col_names = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        pk_columns = context.get_pk_columns(self.table_name)

        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {self.table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        # Try to insert with retries for PK collisions
        inserted = False
        for attempt in range(5):
            row_data = context.generate_row(self.table_name)
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            try:
                context.db_tools.execute_sql(insert_sql, row_data, timed=True)
                context.track_modified_key(pk_tuple)
                inserted = True

                # Commit if not in autocommit mode
                if not context.db_tools.autocommit:
                    context.db_tools.commit_changes(timed=True, message="insert")
                break
            except Exception as e:
                if attempt == 4:  # Last attempt
                    raise
                continue  # Retry with new data

        if not inserted:
            raise ValueError("Failed to insert row after 5 attempts")

    def requires_setup_data(self) -> bool:
        return False  # Can insert without existing data

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.INSERT


class UpdateOperation(Operation):
    """Update an existing row by primary key.

    Selects a random existing primary key and updates all non-PK columns
    with new generated values.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed point update operation."""
        # Select a random existing key
        key_to_update = context.select_random_key(self.table_name)
        if not key_to_update:
            raise ValueError("No existing keys found for update operation")

        # Get column information
        pk_columns = context.get_pk_columns(self.table_name)
        all_columns = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            raise ValueError(f"No non-PK columns to update in {self.table_name}")

        # Generate new values for non-PK columns
        row_data = context.generate_row(self.table_name)

        # Build UPDATE query
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        where_clause = " AND ".join(
            [f"{pk_name} = %({pk_name})s" for pk_name in pk_columns]
        )
        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause};"

        # Add PK values to row_data for WHERE clause
        for i, pk_col in enumerate(pk_columns):
            row_data[pk_col] = key_to_update[i]

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        # Execute the timed update
        context.db_tools.execute_sql(update_sql, row_data, timed=True)

        # Commit if not in autocommit mode
        if not context.db_tools.autocommit:
            context.db_tools.commit_changes(timed=False, message="update")

        # Track the modified key
        context.track_modified_key(key_to_update)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to update

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.UPDATE


class DeleteOperation(Operation):
    """Delete an existing row by primary key.

    Selects a random existing primary key and deletes the row.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed delete operation."""
        # Select a random existing key
        key_to_delete = context.select_random_key(self.table_name)
        if not key_to_delete:
            raise ValueError("No existing keys found for delete operation")

        # Build DELETE query
        pk_columns = context.get_pk_columns(self.table_name)
        where_clause = " AND ".join([f"{pk_name} = %s" for pk_name in pk_columns])
        delete_sql = f"DELETE FROM {self.table_name} WHERE {where_clause};"

        # Record that we're touching 1 key
        context.record_keys_touched(1)

        # Execute the timed delete
        context.db_tools.execute_sql(delete_sql, key_to_delete, timed=True)

        # Commit if not in autocommit mode
        if not context.db_tools.autocommit:
            context.db_tools.commit_changes(timed=False, message="delete")

        # Remove from modified keys tracking
        context.untrack_modified_key(key_to_delete)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to delete

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.UPDATE  # DELETE is categorized as UPDATE in result proto


class RangeReadOperation(Operation):
    """Read multiple rows using a range query.

    Selects a random range of rows based on primary key ordering and
    performs a range SELECT operation.
    """

    def __init__(self, table_name: str, range_size: int = 10):
        self.table_name = table_name
        self.range_size = range_size

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed range read operation."""
        # Prepare range query components
        range_info = context.prepare_range_query(
            self.table_name, self.range_size, "range read"
        )

        # Build SELECT query with range condition
        select_sql = f"SELECT * FROM {self.table_name} WHERE {range_info['where_clause']};"

        # Record the actual number of keys in the range
        num_keys = len(range_info["keys_in_range"])
        context.record_keys_touched(num_keys)

        # Execute the timed range read
        context.db_tools.execute_sql(select_sql, range_info["params"], timed=True)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to read from

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.READ


class RangeUpdateOperation(Operation):
    """Update multiple rows using a range query.

    Selects a random range of rows based on primary key ordering and
    updates all non-PK columns in that range.
    """

    def __init__(self, table_name: str, range_size: int = 10):
        self.table_name = table_name
        self.range_size = range_size

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed range update operation."""
        # Prepare range query components
        range_info = context.prepare_range_query(
            self.table_name, self.range_size, "range update"
        )

        # Get column information
        pk_columns = context.get_pk_columns(self.table_name)
        all_columns = dbh.get_all_columns(
            context.db_tools.get_current_connection(), self.table_name
        )
        non_pk_columns = [col for col in all_columns if col not in pk_columns]

        if not non_pk_columns:
            raise ValueError(f"No non-PK columns to update in {self.table_name}")

        # Generate new values for non-PK columns
        row_data = context.generate_row(self.table_name)

        # Build UPDATE query with range condition
        set_clause = ", ".join([f"{col} = %({col})s" for col in non_pk_columns])
        row_data.update(range_info["params"])
        update_sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {range_info['where_clause']};"

        # Record the actual number of keys in the range
        num_keys = len(range_info["keys_in_range"])
        context.record_keys_touched(num_keys)

        # Execute the timed range update
        context.db_tools.execute_sql(update_sql, row_data, timed=True)

        # Commit if not in autocommit mode
        if not context.db_tools.autocommit:
            context.db_tools.commit_changes(timed=False, message="range update")

        # Track all keys in the range as modified
        for key in range_info["keys_in_range"]:
            context.track_modified_key(key)

    def requires_setup_data(self) -> bool:
        return True  # Needs existing rows to update

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.UPDATE
