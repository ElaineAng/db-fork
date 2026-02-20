from __future__ import annotations

import pytest

from microbench.runner_support import (
    BASELINE_SOURCE_CONSTANT,
    DEFAULT_BASELINE_LATENCY_SECONDS,
    OutcomePolicy,
)


def test_threshold_marks_slow_operation_unsuccessful() -> None:
    policy = OutcomePolicy(multiplier=10.0)
    threshold = DEFAULT_BASELINE_LATENCY_SECONDS * 10.0

    decision = policy.evaluate_success(
        op_type=3,
        observed_latency_seconds=threshold * 1.2,
    )

    assert decision.threshold_exceeded is True
    assert decision.success is False
    assert decision.baseline_source == BASELINE_SOURCE_CONSTANT
    assert decision.threshold_latency_seconds == pytest.approx(threshold)


def test_fast_operation_remains_successful() -> None:
    policy = OutcomePolicy(multiplier=10.0)
    threshold = DEFAULT_BASELINE_LATENCY_SECONDS * 10.0

    decision = policy.evaluate_success(
        op_type=5,
        observed_latency_seconds=threshold * 0.5,
    )

    assert decision.threshold_exceeded is False
    assert decision.success is True
    assert decision.baseline_latency_seconds == pytest.approx(
        DEFAULT_BASELINE_LATENCY_SECONDS
    )


def test_describe_latency_never_flips_success_flag() -> None:
    policy = OutcomePolicy(multiplier=10.0)
    decision = policy.describe_latency(
        op_type=1,
        observed_latency_seconds=999.0,
    )

    assert decision.success is True
    assert decision.threshold_exceeded is False
    assert decision.baseline_source == BASELINE_SOURCE_CONSTANT
