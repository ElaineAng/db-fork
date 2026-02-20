from __future__ import annotations

from dblib import result_failure_pb2 as rf
from microbench.runner_support import classify_failure


def test_classifier_maps_lock_contention() -> None:
    classification = classify_failure(
        Exception("deadlock detected while waiting for lock")
    )
    assert classification.category == rf.FAILURE_LOCK_CONTENTION
    assert "Lock contention" in classification.reason


def test_classifier_maps_constraint_data() -> None:
    classification = classify_failure(
        Exception("numeric value out of range SQLSTATE 22003")
    )
    assert classification.category == rf.FAILURE_CONSTRAINT_OR_DATA
    assert classification.sqlstate == "22003"


def test_classifier_maps_backend_state_conflict() -> None:
    classification = classify_failure(
        Exception('source database "microbench" is being accessed by other users')
    )
    assert classification.category == rf.FAILURE_BACKEND_STATE_CONFLICT
