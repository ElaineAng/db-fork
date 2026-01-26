import os
import uuid
import time
import threading
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
from dblib import result_pb2 as rslt
from util.sql_parse import get_sql_operation_keyword


# Thread-local storage for thread_id
_thread_local = threading.local()


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

        # Shared results list (protected by lock)
        self.results = []
        self.iteration_counter = 0

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def _get_thread_state(self):
        """Get or initialize thread-local state for the current thread."""
        if not hasattr(self._thread_local, "initialized"):
            # Initialize thread-local state
            self._thread_local.initialized = True
            self._thread_local.current_table_name = ""
            self._thread_local.current_table_schema = ""
            self._thread_local.initial_db_size = 0
            self._thread_local.seed = 0
            self._thread_local.current_op_type = rslt.OpType.UNSPECIFIED
            self._thread_local.current_latency = 0.0
            self._thread_local.num_keys_touched = 0
            self._thread_local.sql_query = ""
            self._thread_local.disk_size_before = 0
            self._thread_local.disk_size_after = 0
        return self._thread_local

    def _reset_metrics(self):
        """Reset all metric fields for a new record (thread-local)."""
        state = self._get_thread_state()
        state.current_op_type = rslt.OpType.UNSPECIFIED
        state.current_latency = 0.0
        state.num_keys_touched = 0
        state.sql_query = ""
        state.disk_size_before = 0
        state.disk_size_after = 0

    def reset(self):
        """Reset all collected timing data and proto messages (shared state only)."""
        with self._lock:
            self.results = []
            self.iteration_counter = 0

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

    @contextmanager
    def maybe_time_ops(self, timed: bool, op_type: rslt.OpType):
        # Return early if not timed.
        if not timed:
            yield
            return
        start_time = time.perf_counter()
        try:
            yield
        # Propagate exceptions.
        except Exception as e:
            raise e
        # Only collect elapsed time if no exceptions.
        else:
            end_time = time.perf_counter()
            self._validate_and_set_op_type(op_type)
            state = self._get_thread_state()
            state.current_latency = end_time - start_time

    def record_num_keys_touched(self, num_keys: int) -> None:
        state = self._get_thread_state()
        state.num_keys_touched = num_keys

    def record_sql_query(self, sql_query: str) -> None:
        state = self._get_thread_state()
        state.sql_query = sql_query

    def record_disk_size_before(self, size_bytes: int) -> None:
        """Record disk size before an operation (thread-local)."""
        state = self._get_thread_state()
        state.disk_size_before = size_bytes

    def record_disk_size_after(self, size_bytes: int) -> None:
        """Record disk size after an operation (thread-local)."""
        state = self._get_thread_state()
        state.disk_size_after = size_bytes

    def flush_record(self):
        """
        Create a Result proto with all current context and metrics, save it, and reset.

        Uses the thread-local thread_id set via set_current_thread_id().
        """
        state = self._get_thread_state()

        # Create and fill the Result proto
        result = rslt.Result()
        result.run_id = self.run_id
        result.iteration_number = self.iteration_counter
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

        # Append to results (thread-safe)
        with self._lock:
            self.results.append(result)
            self.iteration_counter += 1

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
