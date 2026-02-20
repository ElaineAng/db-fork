import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

from dblib import result_pb2 as rslt


def result_to_row(result: rslt.Result) -> dict:
    return {
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


def write_rows_to_parquet(rows: list[dict], filepath: str) -> str:
    new_table = pa.Table.from_pylist(rows)

    if os.path.exists(filepath):
        try:
            existing_table = pq.read_table(filepath)
            combined_table = pa.concat_tables([existing_table, new_table])
            pq.write_table(combined_table, filepath)
            return (
                f"Appended {len(rows)} results to {filepath} "
                f"(total: {len(combined_table)} rows)"
            )
        except Exception as e:
            pq.write_table(new_table, filepath)
            return (
                f"Error reading existing file, overwriting: {e}\n"
                f"Wrote {len(rows)} benchmark results to {filepath}"
            )

    pq.write_table(new_table, filepath)
    return f"Wrote {len(rows)} benchmark results to {filepath}"


def write_summary_json(payload: dict, filepath: str) -> None:
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
