from dataclasses import dataclass

from dblib import result_failure_pb2 as rf
from dblib import result_pb2 as rslt


@dataclass
class ThreadState:
    current_table_name: str = ""
    current_table_schema: str = ""
    initial_db_size: int = 0
    seed: int = 0
    current_op_type: int = rslt.OpType.UNSPECIFIED
    current_latency: float = 0.0
    num_keys_touched: int = 0
    sql_query: str = ""
    disk_size_before: int = 0
    disk_size_after: int = 0
    outcome_phase: int = rf.PHASE_EXECUTE


def new_thread_state() -> ThreadState:
    return ThreadState()


def reset_thread_metrics(state: ThreadState) -> None:
    state.current_op_type = rslt.OpType.UNSPECIFIED
    state.current_latency = 0.0
    state.num_keys_touched = 0
    state.sql_query = ""
    state.disk_size_before = 0
    state.disk_size_after = 0
