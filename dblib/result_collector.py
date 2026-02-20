import json
import os
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq

from dblib import result_failure_pb2 as rf
from dblib import result_pb2 as rslt
from microbench.runner_support import OutcomePolicy
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
        "DELETE": rslt.OpType.UPDATE,  # DELETE is a write operation like UPDATE.
        "WITH": rslt.OpType.READ,  # WITH is likely a CTE query (read).
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
        output_dir: str = None,
    ):
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir or os.environ.get(
            "RUN_STATS_DIR",
            "/tmp/run_stats",
        )

        # Lock for thread-safe result collection
        self._lock = threading.Lock()

        # Thread-local storage for per-thread context and metrics
        self._thread_local = threading.local()

        # Shared results list (protected by lock)
        self.results = []
        self.iteration_counter = 0

        self._outcome_policy = OutcomePolicy()

        self._attempted_ops = 0
        self._successful_ops = 0
        self._failed_exception_ops = 0
        self._failed_slow_ops = 0
        self._failure_by_category = Counter()
        self._failure_by_phase = Counter()
        self._failure_by_reason = Counter()

        # Create output directory if it doesn't exist.
        os.makedirs(self.output_dir, exist_ok=True)

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
            self._thread_local.outcome_phase = rf.PHASE_EXECUTE
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
            self._attempted_ops = 0
            self._successful_ops = 0
            self._failed_exception_ops = 0
            self._failed_slow_ops = 0
            self._failure_by_category.clear()
            self._failure_by_phase.clear()
            self._failure_by_reason.clear()

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

    def set_outcome_phase(self, phase: int) -> None:
        """Set operation phase used for outcome.failure.phase on this thread."""
        state = self._get_thread_state()
        state.outcome_phase = phase

    def _validate_and_set_op_type(self, op_type: rslt.OpType):
        state = self._get_thread_state()
        if (
            state.current_op_type != rslt.OpType.UNSPECIFIED
            and state.current_op_type != op_type
        ):
            raise ValueError(
                "Operation type changed mid-operation: "
                f"was {state.current_op_type}, now {op_type}"
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

    def _build_result(
        self,
        state,
        *,
        op_type: int,
        latency: float,
        sql_query: str,
    ) -> rslt.Result:
        result = rslt.Result()
        result.run_id = self.run_id
        result.iteration_number = self.iteration_counter
        result.table_name = state.current_table_name
        result.table_schema = state.current_table_schema
        result.initial_db_size = state.initial_db_size
        result.random_seed = state.seed

        result.op_type = op_type
        result.num_keys_touched = state.num_keys_touched
        result.latency = latency
        result.sql_query = sql_query
        result.thread_id = get_current_thread_id()
        result.disk_size_before = state.disk_size_before
        result.disk_size_after = state.disk_size_after
        return result

    def _apply_latency_outcome(self, result: rslt.Result, decision) -> None:
        outcome = result.outcome
        outcome.success = decision.success

        latency = outcome.latency
        latency.observed_latency_seconds = decision.observed_latency_seconds
        latency.baseline_latency_seconds = decision.baseline_latency_seconds
        latency.threshold_latency_seconds = decision.threshold_latency_seconds
        latency.multiplier = decision.multiplier
        latency.threshold_exceeded = decision.threshold_exceeded
        latency.baseline_source = decision.baseline_source

    def _record_success_result(self, result: rslt.Result, slow_failure: bool) -> None:
        result.iteration_number = self.iteration_counter
        self.results.append(result)
        self.iteration_counter += 1

        self._attempted_ops += 1
        if slow_failure:
            self._failed_slow_ops += 1
            self._failure_by_category[rf.FAILURE_TIMEOUT] += 1
            self._failure_by_phase[result.outcome.failure.phase] += 1
            if result.outcome.failure.reason:
                self._failure_by_reason[result.outcome.failure.reason] += 1
        else:
            self._successful_ops += 1

    def _record_exception_result(self, result: rslt.Result) -> None:
        result.iteration_number = self.iteration_counter
        self.results.append(result)
        self.iteration_counter += 1

        self._attempted_ops += 1
        self._failed_exception_ops += 1
        self._failure_by_category[result.outcome.failure.category] += 1
        self._failure_by_phase[result.outcome.failure.phase] += 1
        if result.outcome.failure.reason:
            self._failure_by_reason[result.outcome.failure.reason] += 1

    def flush_record(self):
        """
        Create a Result proto with all current context and metrics, save it, and reset.

        Uses the thread-local thread_id set via set_current_thread_id().
        """
        state = self._get_thread_state()

        decision = self._outcome_policy.evaluate_success(
            op_type=state.current_op_type,
            observed_latency_seconds=state.current_latency,
        )

        result = self._build_result(
            state,
            op_type=state.current_op_type,
            latency=state.current_latency,
            sql_query=state.sql_query,
        )
        self._apply_latency_outcome(result, decision)

        if decision.threshold_exceeded:
            result.outcome.failure.category = rf.FAILURE_TIMEOUT
            result.outcome.failure.phase = state.outcome_phase
            result.outcome.failure.reason = (
                f"Slow operation: {decision.observed_latency_seconds:.6f}s > "
                f"{decision.threshold_latency_seconds:.6f}s"
            )
        else:
            result.outcome.failure.category = rf.FAILURE_NONE
            result.outcome.failure.phase = state.outcome_phase

        with self._lock:
            self._record_success_result(
                result,
                slow_failure=bool(decision.threshold_exceeded),
            )

        self._reset_metrics()

    def flush_failure_record(
        self,
        *,
        op_type: int = rslt.OpType.UNSPECIFIED,
        phase: int = rf.PHASE_EXECUTE,
        category: int = rf.FAILURE_UNKNOWN,
        reason: str = "",
        raw_error: str = "",
        sqlstate: str = "",
        observed_latency_seconds: float = 0.0,
        sql_query: str = "",
    ) -> None:
        """Append a structured failure result row without interrupting the run."""
        state = self._get_thread_state()

        decision = self._outcome_policy.describe_latency(
            op_type=op_type,
            observed_latency_seconds=observed_latency_seconds,
        )

        effective_sql = sql_query or state.sql_query
        result = self._build_result(
            state,
            op_type=op_type,
            latency=observed_latency_seconds,
            sql_query=effective_sql,
        )
        self._apply_latency_outcome(result, decision)

        result.outcome.success = False
        result.outcome.failure.category = category
        result.outcome.failure.phase = phase
        result.outcome.failure.reason = reason
        result.outcome.failure.raw_error = raw_error
        result.outcome.failure.sqlstate = sqlstate

        with self._lock:
            self._record_exception_result(result)

        self._reset_metrics()

    def _safe_enum_name(self, enum_type, value: int) -> str:
        try:
            return enum_type.Name(value)
        except ValueError:
            return str(value)

    def _build_summary_payload(self) -> dict:
        attempted = self._attempted_ops
        successful = self._successful_ops

        top_category = ""
        top_reason = ""

        if self._failure_by_category:
            top_category_code = self._failure_by_category.most_common(1)[0][0]
            top_category = self._safe_enum_name(
                rf.FailureCategory,
                top_category_code,
            )
        if self._failure_by_reason:
            top_reason = self._failure_by_reason.most_common(1)[0][0]

        return {
            "run_id": self.run_id,
            "attempted_ops": attempted,
            "successful_ops": successful,
            "failed_exception_ops": self._failed_exception_ops,
            "failed_slow_ops": self._failed_slow_ops,
            "success_rate": float(successful / attempted) if attempted > 0 else 0.0,
            "top_failure_category": top_category,
            "top_failure_reason": top_reason,
            "failure_by_category": {
                self._safe_enum_name(rf.FailureCategory, key): val
                for key, val in self._failure_by_category.items()
            },
            "failure_by_phase": {
                self._safe_enum_name(rf.FailurePhase, key): val
                for key, val in self._failure_by_phase.items()
            },
        }

    def write_summary_json(self, filename: str = None) -> str:
        filename = filename or f"{self.run_id}_summary.json"
        filepath = os.path.join(self.output_dir, filename)

        with self._lock:
            payload = self._build_summary_payload()

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)

        print(f"Wrote summary to {filepath}")
        return filepath

    def write_to_parquet(self, filename: str = None):
        """Write all collected benchmark results to a parquet file.

        If the file already exists, appends to it instead of overwriting.
        Also always writes a JSON summary for run-level outcome accounting.
        """
        filename = filename or f"{self.run_id}.parquet"
        filepath = os.path.join(self.output_dir, filename)

        if not self.results:
            print("No results to write.")
            self.write_summary_json()
            return

        # Convert proto messages to dictionary rows.
        rows = []
        for result in self.results:
            row = {
                "run_id": result.run_id,
                "thread_id": result.thread_id,
                "random_seed": result.random_seed,
                "iteration_number": result.iteration_number,
                "op_type": result.op_type,
                "initial_db_size": result.initial_db_size,
                "table_name": result.table_name,
                "table_schema": result.table_schema,
                "num_keys_touched": result.num_keys_touched,
                "latency": result.latency,
                "disk_size_before": result.disk_size_before,
                "disk_size_after": result.disk_size_after,
                "sql_query": result.sql_query,
                "outcome_success": result.outcome.success,
                "failure_category": result.outcome.failure.category,
                "failure_phase": result.outcome.failure.phase,
                "failure_reason": result.outcome.failure.reason,
                "failure_sqlstate": result.outcome.failure.sqlstate,
                "failure_raw_error": result.outcome.failure.raw_error,
                "observed_latency_seconds": result.outcome.latency.observed_latency_seconds,
                "baseline_latency_seconds": result.outcome.latency.baseline_latency_seconds,
                "threshold_latency_seconds": result.outcome.latency.threshold_latency_seconds,
                "latency_multiplier": result.outcome.latency.multiplier,
                "latency_threshold_exceeded": result.outcome.latency.threshold_exceeded,
                "baseline_source": result.outcome.latency.baseline_source,
            }
            rows.append(row)

        # Create PyArrow table from new results.
        new_table = pa.Table.from_pylist(rows)

        # If file exists, read existing data and concatenate.
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

        self.write_summary_json()
