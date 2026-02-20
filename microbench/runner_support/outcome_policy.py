from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SLOW_LATENCY_MULTIPLIER = 10.0

# One-time fixed baseline from existing Exp3 parquet history:
# - Source: /Users/garfield/PycharmProjects/db-fork/experiments/experiment-3-throughput/results/data
# - Filter: T=1 runs, successful rows only, op_type in {BRANCH_CREATE, BRANCH_CONNECT, READ, UPDATE}
# - Metric: global P95 latency over filtered rows (computed once on 2026-02-20)
# - Raw computed P95 before rounding: 0.010005447990261016 seconds
# Anomaly threshold is always: baseline_latency_seconds * slow_latency_multiplier.
DEFAULT_BASELINE_LATENCY_SECONDS = 0.01
BASELINE_SOURCE_CONSTANT = "CONSTANT"


@dataclass(frozen=True)
class LatencyDecision:
    success: bool
    observed_latency_seconds: float
    baseline_latency_seconds: float
    threshold_latency_seconds: float
    multiplier: float
    threshold_exceeded: bool
    baseline_source: str


class OutcomePolicy:
    def __init__(
        self,
        multiplier: float = DEFAULT_SLOW_LATENCY_MULTIPLIER,
        baseline_latency_seconds: float = DEFAULT_BASELINE_LATENCY_SECONDS,
    ):
        self._multiplier = normalize_slow_latency_multiplier(multiplier)
        self._baseline_latency_seconds = float(baseline_latency_seconds or 0.0)

    @property
    def multiplier(self) -> float:
        return self._multiplier

    def evaluate_success(
        self,
        op_type: int,
        observed_latency_seconds: float,
    ) -> LatencyDecision:
        del op_type
        return self._build_decision(
            observed_latency_seconds=observed_latency_seconds,
            enforce_threshold=True,
        )

    def describe_latency(
        self,
        op_type: int,
        observed_latency_seconds: float,
    ) -> LatencyDecision:
        del op_type
        return self._build_decision(
            observed_latency_seconds=observed_latency_seconds,
            enforce_threshold=False,
        )

    def _build_decision(
        self,
        observed_latency_seconds: float,
        enforce_threshold: bool,
    ) -> LatencyDecision:
        observed = float(observed_latency_seconds or 0.0)
        baseline = self._baseline_latency_seconds

        threshold = 0.0
        threshold_exceeded = False

        if baseline > 0:
            # Fixed anomaly rule for Exp3:
            # threshold = 10x baseline by default (or custom multiplier if set).
            threshold = baseline * self._multiplier
            threshold_exceeded = bool(enforce_threshold and observed > threshold)

        return LatencyDecision(
            success=not threshold_exceeded,
            observed_latency_seconds=observed,
            baseline_latency_seconds=baseline,
            threshold_latency_seconds=threshold,
            multiplier=self._multiplier,
            threshold_exceeded=threshold_exceeded,
            baseline_source=BASELINE_SOURCE_CONSTANT,
        )


def normalize_slow_latency_multiplier(value: float) -> float:
    value = float(value or 0.0)
    return value if value > 0 else DEFAULT_SLOW_LATENCY_MULTIPLIER
