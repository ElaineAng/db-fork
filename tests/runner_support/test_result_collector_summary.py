from __future__ import annotations

import json

from dblib import result_failure_pb2 as rf
from dblib import result_pb2 as rslt
from dblib.result_collector import ResultCollector


def test_summary_generated_for_empty_success_run(tmp_path) -> None:
    collector = ResultCollector(run_id="exp3_test", output_dir=str(tmp_path))
    collector.set_context("orders", "CREATE TABLE orders(id int);", 0, 1)

    collector.flush_failure_record(
        op_type=rslt.OpType.READ,
        phase=rf.PHASE_EXECUTE,
        category=rf.FAILURE_TIMEOUT,
        reason="Timeout: statement timeout",
        raw_error="statement timeout",
        sqlstate="57014",
        observed_latency_seconds=0.5,
    )
    collector.write_to_parquet()

    summary_path = tmp_path / "exp3_test_summary.json"
    assert summary_path.exists()

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    assert summary["attempted_ops"] == 1
    assert summary["successful_ops"] == 0
    assert summary["failed_exception_ops"] == 1
    assert summary["failed_slow_ops"] == 0


def test_summary_generated_even_when_no_rows(tmp_path) -> None:
    collector = ResultCollector(run_id="exp3_empty", output_dir=str(tmp_path))
    collector.write_to_parquet()

    summary_path = tmp_path / "exp3_empty_summary.json"
    assert summary_path.exists()

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    assert summary["attempted_ops"] == 0
    assert summary["successful_ops"] == 0
