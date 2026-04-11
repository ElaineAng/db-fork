import os
import uuid
import time
import threading
import asyncio
from contextlib import contextmanager
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
from dblib import result_pb2 as rslt
from util.sql_parse import get_sql_operation_keyword


# Thread-local storage for thread_id
_thread_local = threading.local()


class _OperationState:
    """State for a single operation (used in both thread-local and task-local storage)."""
    def __init__(self):
        self.initialized = True
        self.current_table_name = ""
        self.current_table_schema = ""
        self.initial_db_size = 0
        self.seed = 0
        self.current_op_type = rslt.OpType.UNSPECIFIED
        self.current_latency = 0.0
        self.num_keys_touched = 0
        self.sql_query = ""
        self.disk_size_before = 0
        self.disk_size_after = 0
        self.branch_count = 0
        self.step_id = -1
        self.storage_fn = None
        self.start_time = 0.0
        self.end_time = 0.0


def set_current_thread_id(thread_id: int) -> None:
    """Set the thread ID for the current thread.

    This should be called once at the start of each worker thread.
    """
    _thread_local.thread_id = thread_id


def get_current_thread_id() -> int:
    """Get the thread ID for the current thread.

    Returns 0 if not set (main thread default).
    """
    return getattr(_thread_local, "thread_id", 0)


def GetOpTypeFromSQL(sql: str) -> rslt.OpType:
    """
    Determine the operation type from a SQL statement.

    Handles edge cases like:
    - CTEs (WITH clauses)
    - Subqueries in FROM, WHERE, SELECT clauses
    - SQL comments (-- and /* */)
    - Multiple statements (uses first statement)

    Args:
        sql: SQL statement to analyze

    Returns:
        OpType enum corresponding to the main operation
    """
    # Get the primary operation keyword
    keyword = get_sql_operation_keyword(sql)

    if not keyword:
        return rslt.OpType.UNSPECIFIED

    # Map keywords to OpType
    keyword_map = {
        "SELECT": rslt.OpType.READ,
        "INSERT": rslt.OpType.INSERT,
        "UPDATE": rslt.OpType.UPDATE,
        "DELETE": rslt.OpType.UPDATE,  # DELETE is a write operation like UPDATE
        "WITH": rslt.OpType.READ,  # If we still have WITH, it's likely a CTE query (read)
        "CREATE": rslt.OpType.DDL,
        "ALTER": rslt.OpType.DDL,
        "DROP": rslt.OpType.DDL,
    }

    return keyword_map.get(keyword, rslt.OpType.UNSPECIFIED)


def str_to_op_type(op_str: str) -> rslt.OpType:
    """
    Convert a string-based operation type to OpType enum.

    Args:
        op_str: String representation of the operation type.
                Must match enum name exactly (case-insensitive).

    Returns:
        Corresponding OpType enum value, or OpType.UNSPECIFIED if unknown.
    """
    try:
        return rslt.OpType[op_str.upper().strip()]
    except KeyError:
        return rslt.OpType.UNSPECIFIED


class ResultCollector:
    def __init__(
        self,
        run_id: str = None,
        output_dir: str = "/tmp/run_stats",
    ):
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir

        # Lock for thread-safe result collection
        self._lock = threading.Lock()

        # Thread-local storage for per-thread context and metrics
        self._thread_local = threading.local()

        # Task-local storage for async operations (dict[task_id -> state])
        # Protected by _lock for thread-safety
        self._task_local = {}

        # Shared results list (protected by lock)
        self.results = []
        self.iteration_counter = 0

        # Track failed operations
        self.failed_operations = []  # List of failure details

        # Debug counters for async mode
        self._flush_record_calls = 0
        self._task_states_created = 0

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def _get_thread_state(self):
        """Get or initialize state for the current thread or async task.

        For async operations, uses task-local storage (one state per concurrent task).
        For sync operations, uses thread-local storage (one state per thread).
        """
        # Check if we're in an async context
        try:
            task = asyncio.current_task()
            if task is not None:
                # Async mode: use task-local storage
                task_id = id(task)
                with self._lock:
                    if task_id not in self._task_local:
                        self._task_local[task_id] = _OperationState()
                    return self._task_local[task_id]
        except RuntimeError:
            # Not in async context, fall through to thread-local
            pass

        # Sync mode: use thread-local storage
        if not hasattr(self._thread_local, "state"):
            self._thread_local.state = _OperationState()
        return self._thread_local.state

    def _reset_metrics(self):
        """Reset all metric fields for a new record (thread-local)."""
        state = self._get_thread_state()
        state.current_op_type = rslt.OpType.UNSPECIFIED
        state.current_latency = 0.0
        state.num_keys_touched = 0
        state.sql_query = ""
        state.disk_size_before = 0
        state.disk_size_after = 0
        state.branch_count = 0
        state.start_time = 0.0
        state.end_time = 0.0

    def reset(self):
        """Reset all collected timing data and proto messages (shared state only)."""
        with self._lock:
            self.results = []
            self.iteration_counter = 0
            self.failed_operations = []

    def cleanup_task_local_storage(self):
        """Clean up task-local storage for completed async operations.

        Call this after all async operations are complete to free memory.
        """
        with self._lock:
            self._task_local.clear()

    def set_context(
        self,
        table_name: str,
        table_schema: str,
        initial_db_size: int,
        seed: int,
    ):
        """Set context information for the next operation to be timed (thread-local)."""
        state = self._get_thread_state()
        state.current_table_name = table_name
        state.current_table_schema = table_schema
        state.initial_db_size = initial_db_size
        state.seed = seed

    def _validate_and_set_op_type(self, op_type: rslt.OpType):
        state = self._get_thread_state()
        if (
            state.current_op_type != rslt.OpType.UNSPECIFIED
            and state.current_op_type != op_type
        ):
            raise ValueError(
                f"Operation type changed mid-operation: was {state.current_op_type}, now {op_type}"
            )
        state.current_op_type = op_type

    def set_storage_fn(self, fn):
        """Set the storage measurement function for the current thread."""
        state = self._get_thread_state()
        state.storage_fn = fn

    @contextmanager
    def maybe_measure_ops(self, timed: bool, op_type: rslt.OpType, storage: bool = False):
        state = self._get_thread_state()
        if storage and state.storage_fn:
            state.disk_size_before = state.storage_fn()
        if not timed and not storage:
            yield
            return
        start_perf = time.perf_counter() if timed else None
        start_wall = time.time() if timed else None
        try:
            yield
        except Exception as e:
            raise e
        else:
            if timed:
                end_perf = time.perf_counter()
                end_wall = time.time()
                self._validate_and_set_op_type(op_type)
                state.current_latency = end_perf - start_perf
                state.start_time = start_wall
                state.end_time = end_wall
            if storage and state.storage_fn:
                state.disk_size_after = state.storage_fn()

    def record_num_keys_touched(self, num_keys: int) -> None:
        state = self._get_thread_state()
        state.num_keys_touched = num_keys

    def record_disk_size_before(self, size: int) -> None:
        state = self._get_thread_state()
        state.disk_size_before = size

    def record_disk_size_after(self, size: int) -> None:
        state = self._get_thread_state()
        state.disk_size_after = size

    def record_branch_count(self, branch_count: int) -> None:
        state = self._get_thread_state()
        state.branch_count = branch_count

    def record_step_id(self, step_id: int) -> None:
        state = self._get_thread_state()
        state.step_id = step_id

    def record_sql_query(self, sql_query: str) -> None:
        state = self._get_thread_state()
        state.sql_query = sql_query

    def flush_record(self):
        """
        Create a Result proto with all current context and metrics, save it, and reset.

        Uses the thread-local thread_id set via set_current_thread_id().
        For async operations, cleans up task-local storage after recording.
        """
        try:
            state = self._get_thread_state()

            # Create and fill the Result proto
            result = rslt.Result()
            result.run_id = self.run_id
            # Note: iteration_number will be set inside the lock to avoid race conditions
            result.table_name = state.current_table_name
            result.table_schema = state.current_table_schema
            result.initial_db_size = state.initial_db_size
            result.random_seed = state.seed

            # Fill in collected metrics
            result.op_type = state.current_op_type
            result.num_keys_touched = state.num_keys_touched
            result.latency = state.current_latency
            result.sql_query = state.sql_query
            result.thread_id = get_current_thread_id()
            result.disk_size_before = state.disk_size_before
            result.disk_size_after = state.disk_size_after
            result.branch_count = state.branch_count
            result.step_id = state.step_id
            result.start_time = state.start_time
            result.end_time = state.end_time

            # Append to results (thread-safe)
            # Set iteration_number inside lock to avoid race condition
            with self._lock:
                result.iteration_number = self.iteration_counter
                self.results.append(result)
                self.iteration_counter += 1

            # Reset metrics for next operation
            # For async mode: task-local state persists (will be GC'd when task ends)
            # For sync mode: thread-local state persists (reused across operations)
            self._reset_metrics()

        except Exception as e:
            # Log but don't crash if flush_record fails
            import sys
            print(f"ERROR in flush_record: {type(e).__name__}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

    def record_failure(self, error: Exception, operation_number: int = None) -> None:
        """
        Record a failed operation with context details.

        Args:
            error: The exception that caused the failure
            operation_number: Optional operation number (e.g., 5 out of 1000)
        """
        state = self._get_thread_state()

        failure_info = {
            "thread_id": get_current_thread_id(),
            "op_type": rslt.OpType.Name(state.current_op_type) if state.current_op_type else "UNSPECIFIED",
            "sql_query": state.sql_query if state.sql_query else None,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "operation_number": operation_number,
            "timestamp": time.time(),
        }

        # Append to failed operations (thread-safe)
        with self._lock:
            self.failed_operations.append(failure_info)

        # Reset metric fields for next record
        self._reset_metrics()

    def write_to_parquet(self, filename: str = None):
        """Write all collected benchmark results to a parquet file.

        If the file already exists, appends to it instead of overwriting.
        """

        if not self.results:
            print("No results to write.")
            return

        filename = filename or f"{self.run_id}.parquet"
        filepath = os.path.join(self.output_dir, filename)

        # Convert proto messages to dictionary rows
        rows = []
        for result in self.results:
            row = {
                "run_id": result.run_id,
                "thread_id": result.thread_id,
                "random_seed": result.random_seed,
                "iteration_number": result.iteration_number,
                "op_type": result.op_type,  # Convert enum value to name
                "initial_db_size": result.initial_db_size,
                "table_name": result.table_name,
                "table_schema": result.table_schema,
                "num_keys_touched": result.num_keys_touched,
                "latency": result.latency,
                "disk_size_before": result.disk_size_before,
                "disk_size_after": result.disk_size_after,
                "sql_query": result.sql_query,
                "branch_count": result.branch_count,
                "step_id": result.step_id,
                "start_time": result.start_time,
                "end_time": result.end_time,
            }
            rows.append(row)

        # Create PyArrow table from new results
        new_table = pa.Table.from_pylist(rows)

        # If file exists, read existing data and concatenate
        if os.path.exists(filepath):
            try:
                existing_table = pq.read_table(filepath)
                combined_table = pa.concat_tables([existing_table, new_table])
                pq.write_table(combined_table, filepath)
                print(
                    f"Appended {len(rows)} results to {filepath} "
                    f"(total: {len(combined_table)} rows)"
                )
            except Exception as e:
                print(f"Error reading existing file, overwriting: {e}")
                pq.write_table(new_table, filepath)
                print(f"Wrote {len(rows)} benchmark results to {filepath}")
        else:
            pq.write_table(new_table, filepath)
            print(f"Wrote {len(rows)} benchmark results to {filepath}")
