import numpy as np
import os
import uuid
import time
from psycopg2.extensions import cursor as _pgcursor
from psycopg2.extensions import connection as _pgconn
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
from microbench.benchmark_result_pb2 import BenchmarkResult, OperationType, KeyTuple
from dblib.result_pb2 import Result

class OpType(Enum):
    UNSPECIFIED = 0
    BRANCH_CREATE = 1
    BRANCH_CONNECT = 2
    READ = 3
    INSERT = 4
    UPDATE = 5

def str_to_op_type(op_str: str) -> OpType:
    """
    Convert a string-based operation type to OpType enum.
    
    Args:
        op_str: String representation of the operation type.
                Must match enum name exactly (case-insensitive).
    
    Returns:
        Corresponding OpType enum value, or OpType.UNSPECIFIED if unknown.
    """
    try:
        return OpType[op_str.upper().strip()]
    except KeyError:
        return OpType.UNSPECIFIED



class ResultCollector:
    def __init__(self, run_id: str = None, output_dir: str = "../run_stats"):
        self.reset()
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir
        
        # Context information for the current operation
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def _reset_metrics(self):
        """Reset all metric fields for a new record."""
        self._current_op_type = OpType.UNSPECIFIED
        self._current_execute_latency = 0.0
        self._current_fetch_latency = 0.0
        self._current_commit_latency = 0.0
        self._current_generic_latency = 0.0
        self._num_keys_touched = 0

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
        table_name: str = "",
        table_schema: str = "",
        initial_db_size: int = 0,
    ):
        """Set context information for the next operation to be timed."""
        self.current_table_name = table_name
        self.current_table_schema = table_schema
        self.initial_db_size = initial_db_size
    
    def _validate_and_set_op_type(self, op_type: OpType):
        if self._current_op_type and self._current_op_type != op_type:
            raise ValueError("Operation type changed mid-operation")
        self._current_op_type = op_type
        
        # Commit latency needs to be reset.
        self._current_commit_latency = 0    

    @contextmanager
    def maybe_time_ops(self, op_type: OpType, timed: bool):
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
            self._current_generic_latency = end_time - start_time

    def record_commit_latency(self, duration: float) -> None:
        #  Commit assumes that the op type is already set and just use it.
        self._current_commit_latency = duration
    
    def record_execute_latency(self, duration: float, op_type: OpType) -> None:
        self._validate_and_set_op_type(op_type)
        self._current_execute_latency = duration
    
    def record_fetchall_latency(self, duration: float, op_type: OpType) -> None:
        self._validate_and_set_op_type(op_type)
        self._current_fetch_latency = duration
    
    def record_num_keys_touched(self, num_keys: int) -> None:
        self._num_keys_touched = num_keys
    
    def flush_record(self):
        """
        Create a Result proto with all current context and metrics, save it, and reset.
        """
        
        # Create and fill the Result proto
        result = Result()
        result.run_id = self.run_id
        result.iteration_number = self.iteration_counter
        result.table_name = self.current_table_name
        result.table_schema = self.current_table_schema
        result.initial_db_size = self.initial_db_size
        
        # Fill in collected metrics
        result.op_type = self._current_op_type.value
        result.num_keys_touched = self._num_keys_touched
        result.execute_latency = self._current_execute_latency
        result.fetch_all_latency = self._current_fetch_latency
        result.commit_latency = self._current_commit_latency
        result.generic_latency = self._current_generic_latency
        
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
                "iteration_number": result.iteration_number,
                "op_type": OpType(result.op_type).name,  # Convert enum value to name
                "initial_db_size": result.initial_db_size,
                "table_name": result.table_name,
                "table_schema": result.table_schema,
                "num_keys_touched": result.num_keys_touched,
                "execute_latency": result.execute_latency,
                "fetch_all_latency": result.fetch_all_latency,
                "commit_latency": result.commit_latency,
                "generic_latency": result.generic_latency,
                "disk_size_before": result.disk_size_before,
                "disk_size_after": result.disk_size_after,
            }
            rows.append(row)
        
        # Create PyArrow table and write to parquet
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, filepath)
        
        print(f"Wrote {len(rows)} benchmark results to {filepath}")


class TimedCursor(_pgcursor):
    def __init__(self, *args, **kwargs):
        self.collector = kwargs.pop("collector", None)
        self.op_type = kwargs.pop("op_type", OpType.UNSPECIFIED)
        super(TimedCursor, self).__init__(*args, **kwargs)

    def execute(self, query: str, vars=None):
        start_timestamp = time.perf_counter()
        try:
            super(TimedCursor, self).execute(query, vars)
        finally:
            end_timestamp = time.perf_counter()
            if self.collector:
                self.collector.record_execute_latency(
                    end_timestamp - start_timestamp, op_type=self.op_type
                )

    def fetchall(self):
        start_timestamp = time.perf_counter()
        try:
            return super().fetchall()
        finally:
            end_timestamp = time.perf_counter()
            if self.collector:
                self.collector.record_fetchall_latency(
                    end_timestamp - start_timestamp, op_type=self.op_type
                )


class TimedConnection(_pgconn):
    def __init__(self, *args, **kwargs):
        self.collector = kwargs.pop("collector", None)
        super(TimedConnection, self).__init__(*args, **kwargs)

    def commit(self):
        start_timestamp = time.perf_counter()
        try:
            super(TimedConnection, self).commit()
        finally:
            end_timestamp = time.perf_counter()
            if self.collector:
                self.collector.record_commit_latency(
                    end_timestamp - start_timestamp
                )
