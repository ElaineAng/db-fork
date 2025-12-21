import os
import uuid
import time
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
from dblib import result_pb2 as rslt
from util.sql_parse import get_sql_operation_keyword


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
    def __init__(self, run_id: str = None, output_dir: str = "/tmp/run_stats"):
        self.reset()
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def _reset_metrics(self):
        """Reset all metric fields for a new record."""
        self._current_op_type = rslt.OpType.UNSPECIFIED
        self._current_latency = 0.0
        self._num_keys_touched = 0
        self._sql_query = ""

    def reset(self):
        """Reset all collected timing data and proto messages."""
        # Proto messages collected during benchmark
        self.results = []
        self.iteration_counter = 0

        # Reset metrics
        self._reset_metrics()

        # Reset context
        self.current_table_name = ""
        self.current_table_schema = ""
        self.initial_db_size = 0

    def set_context(
        self,
        table_name: str,
        table_schema: str,
        initial_db_size: int,
        seed: int,
    ):
        """Set context information for the next operation to be timed."""
        self.current_table_name = table_name
        self.current_table_schema = table_schema
        self.initial_db_size = initial_db_size
        self._seed = seed

    def _validate_and_set_op_type(self, op_type: rslt.OpType):
        if (
            self._current_op_type != rslt.OpType.UNSPECIFIED
            and self._current_op_type != op_type
        ):
            raise ValueError(
                f"Operation type changed mid-operation: was {self._current_op_type}, now {op_type}"
            )
        self._current_op_type = op_type

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
            self._current_latency = end_time - start_time

    def record_num_keys_touched(self, num_keys: int) -> None:
        self._num_keys_touched = num_keys

    def record_sql_query(self, sql_query: str) -> None:
        self._sql_query = sql_query

    def flush_record(self):
        """
        Create a Result proto with all current context and metrics, save it, and reset.
        """

        # Create and fill the Result proto
        result = rslt.Result()
        result.run_id = self.run_id
        result.iteration_number = self.iteration_counter
        result.table_name = self.current_table_name
        result.table_schema = self.current_table_schema
        result.initial_db_size = self.initial_db_size
        result.random_seed = self._seed

        # Fill in collected metrics
        result.op_type = self._current_op_type
        result.num_keys_touched = self._num_keys_touched
        result.latency = self._current_latency
        result.sql_query = self._sql_query

        # Append to results
        self.results.append(result)
        self.iteration_counter += 1

        # Reset metric fields for next record
        self._reset_metrics()

    def write_to_parquet(self, filename: str = None):
        """Write all collected benchmark results to a parquet file."""

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

        # Create PyArrow table and write to parquet
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, filepath)

        print(f"Wrote {len(rows)} benchmark results to {filepath}")
