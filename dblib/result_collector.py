import os
import threading
import time
import uuid
from contextlib import contextmanager

from dblib import result_failure_pb2 as rf
from dblib import result_pb2 as rslt
from dblib.results import (
    SummaryCounters,
    ThreadState,
    get_op_type_from_sql,
    new_thread_state,
    reset_thread_metrics,
    result_to_row,
    str_to_op_type as _str_to_op_type,
    write_rows_to_parquet,
    write_summary_json,
)
from microbench.runner_support import OutcomePolicy


# Thread-local storage for thread_id.
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
    """Backwards-compatible wrapper for SQL operation classification."""
    return get_op_type_from_sql(sql)


def str_to_op_type(op_str: str) -> rslt.OpType:
    """Backwards-compatible wrapper for enum-name operation conversion."""
    return _str_to_op_type(op_str)


class ResultCollector:
    def __init__(
        self,
        run_id: str = None,
        output_dir: str = None,
        slow_latency_multiplier: float | None = None,
    ):
        self.run_id = run_id or str(uuid.uuid4())
        self.output_dir = output_dir or os.environ.get(
            "RUN_STATS_DIR",
            "/tmp/run_stats",
        )

        # Lock for thread-safe result collection.
        self._lock = threading.Lock()
        # Thread-local storage for per-thread context and metrics.
        self._thread_local = threading.local()

        # Shared results list (protected by lock).
        self.results = []
        self.iteration_counter = 0

        if slow_latency_multiplier is None:
            self._outcome_policy = OutcomePolicy()
        else:
            self._outcome_policy = OutcomePolicy(
                multiplier=slow_latency_multiplier
            )
        self._summary = SummaryCounters()

        os.makedirs(self.output_dir, exist_ok=True)

    def _get_thread_state(self) -> ThreadState:
        """Get or initialize thread-local state for the current thread."""
        if not hasattr(self._thread_local, "state"):
            self._thread_local.state = new_thread_state()
        return self._thread_local.state

    def _reset_metrics(self) -> None:
        """Reset all metric fields for a new record (thread-local)."""
        reset_thread_metrics(self._get_thread_state())

    def reset(self) -> None:
        """Reset all collected timing data and summary counters."""
        with self._lock:
            self.results = []
            self.iteration_counter = 0
            self._summary.reset()

    def set_context(
        self,
        table_name: str,
        table_schema: str,
        initial_db_size: int,
        seed: int,
    ) -> None:
        """Set context information for the next operation (thread-local)."""
        state = self._get_thread_state()
        state.current_table_name = table_name
        state.current_table_schema = table_schema
        state.initial_db_size = initial_db_size
        state.seed = seed

    def set_outcome_phase(self, phase: int) -> None:
        """Set operation phase used for outcome.failure.phase on this thread."""
        self._get_thread_state().outcome_phase = phase

    def _validate_and_set_op_type(self, op_type: int) -> None:
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
    def maybe_time_ops(self, timed: bool, op_type: int):
        if not timed:
            yield
            return

        start_time = time.perf_counter()
        try:
            yield
        except Exception:
            raise
        else:
            self._validate_and_set_op_type(op_type)
            self._get_thread_state().current_latency = time.perf_counter() - start_time

    def record_num_keys_touched(self, num_keys: int) -> None:
        self._get_thread_state().num_keys_touched = num_keys

    def record_sql_query(self, sql_query: str) -> None:
        self._get_thread_state().sql_query = sql_query

    def record_disk_size_before(self, size_bytes: int) -> None:
        self._get_thread_state().disk_size_before = size_bytes

    def record_disk_size_after(self, size_bytes: int) -> None:
        self._get_thread_state().disk_size_after = size_bytes

    def _build_result(
        self,
        state: ThreadState,
        *,
        op_type: int,
        latency: float,
        sql_query: str,
    ) -> rslt.Result:
        result = rslt.Result()
        result.run_id = self.run_id
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

    def _finalize_result(
        self,
        result: rslt.Result,
        *,
        is_exception: bool,
        slow_failure: bool = False,
    ) -> None:
        with self._lock:
            result.iteration_number = self.iteration_counter
            self.results.append(result)
            self.iteration_counter += 1

            if is_exception:
                self._summary.record_exception(result)
            else:
                self._summary.record_success(
                    result,
                    slow_failure=slow_failure,
                )

    def flush_record(self) -> None:
        """Create a result row from current context/metrics and reset state."""
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

        self._finalize_result(
            result,
            is_exception=False,
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

        result = self._build_result(
            state,
            op_type=op_type,
            latency=observed_latency_seconds,
            sql_query=sql_query or state.sql_query,
        )
        self._apply_latency_outcome(result, decision)

        result.outcome.success = False
        result.outcome.failure.category = category
        result.outcome.failure.phase = phase
        result.outcome.failure.reason = reason
        result.outcome.failure.raw_error = raw_error
        result.outcome.failure.sqlstate = sqlstate

        self._finalize_result(result, is_exception=True)
        self._reset_metrics()

    def _build_summary_payload(self) -> dict:
        return self._summary.build_payload(self.run_id)

    def write_summary_json(self, filename: str = None) -> str:
        filename = filename or f"{self.run_id}_summary.json"
        filepath = os.path.join(self.output_dir, filename)

        with self._lock:
            payload = self._build_summary_payload()

        write_summary_json(payload, filepath)
        print(f"Wrote summary to {filepath}")
        return filepath

    def write_to_parquet(self, filename: str = None) -> None:
        """Write all collected benchmark results to parquet and summary JSON."""
        filename = filename or f"{self.run_id}.parquet"
        filepath = os.path.join(self.output_dir, filename)

        if not self.results:
            print("No results to write.")
            self.write_summary_json()
            return

        rows = [result_to_row(result) for result in self.results]
        print(write_rows_to_parquet(rows, filepath))
        self.write_summary_json()
