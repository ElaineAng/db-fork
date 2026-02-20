from collections import Counter
from dataclasses import dataclass, field

from dblib import result_failure_pb2 as rf
from dblib import result_pb2 as rslt


def _safe_enum_name(enum_type, value: int) -> str:
    try:
        return enum_type.Name(value)
    except ValueError:
        return str(value)


@dataclass
class SummaryCounters:
    attempted_ops: int = 0
    successful_ops: int = 0
    failed_exception_ops: int = 0
    failed_slow_ops: int = 0
    failure_by_category: Counter[int] = field(default_factory=Counter)
    failure_by_phase: Counter[int] = field(default_factory=Counter)
    failure_by_reason: Counter[str] = field(default_factory=Counter)

    def reset(self) -> None:
        self.attempted_ops = 0
        self.successful_ops = 0
        self.failed_exception_ops = 0
        self.failed_slow_ops = 0
        self.failure_by_category.clear()
        self.failure_by_phase.clear()
        self.failure_by_reason.clear()

    def record_success(self, result: rslt.Result, *, slow_failure: bool) -> None:
        self.attempted_ops += 1
        if slow_failure:
            self.failed_slow_ops += 1
            self.failure_by_category[rf.FAILURE_TIMEOUT] += 1
            self.failure_by_phase[result.outcome.failure.phase] += 1
            if result.outcome.failure.reason:
                self.failure_by_reason[result.outcome.failure.reason] += 1
            return
        self.successful_ops += 1

    def record_exception(self, result: rslt.Result) -> None:
        self.attempted_ops += 1
        self.failed_exception_ops += 1
        self.failure_by_category[result.outcome.failure.category] += 1
        self.failure_by_phase[result.outcome.failure.phase] += 1
        if result.outcome.failure.reason:
            self.failure_by_reason[result.outcome.failure.reason] += 1

    def build_payload(self, run_id: str) -> dict:
        top_category = ""
        if self.failure_by_category:
            top_category_code = self.failure_by_category.most_common(1)[0][0]
            top_category = _safe_enum_name(rf.FailureCategory, top_category_code)

        top_reason = ""
        if self.failure_by_reason:
            top_reason = self.failure_by_reason.most_common(1)[0][0]

        attempted = self.attempted_ops
        successful = self.successful_ops

        return {
            "run_id": run_id,
            "attempted_ops": attempted,
            "successful_ops": successful,
            "failed_exception_ops": self.failed_exception_ops,
            "failed_slow_ops": self.failed_slow_ops,
            "success_rate": float(successful / attempted) if attempted > 0 else 0.0,
            "top_failure_category": top_category,
            "top_failure_reason": top_reason,
            "failure_by_category": {
                _safe_enum_name(rf.FailureCategory, key): val
                for key, val in self.failure_by_category.items()
            },
            "failure_by_phase": {
                _safe_enum_name(rf.FailurePhase, key): val
                for key, val in self.failure_by_phase.items()
            },
        }
