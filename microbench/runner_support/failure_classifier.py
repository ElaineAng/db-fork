from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from dblib import result_failure_pb2 as rf


@dataclass(frozen=True)
class FailureClassification:
    category: int
    reason: str
    sqlstate: str = ""


class FailureClassifier:
    """Classifies exceptions into stable benchmark failure categories."""

    _LOCK_CODES = {"40P01", "55P03"}
    _TIMEOUT_CODES = {"57014"}
    _RESOURCE_CODES = {"53100", "53200", "53300", "53400"}
    _STATE_CONFLICT_CODES = {"40001", "42P04"}

    _RULES: tuple[tuple[int, str, tuple[re.Pattern[str], ...]], ...] = (
        (
            rf.FAILURE_LOCK_CONTENTION,
            "Lock contention",
            (
                re.compile(r"deadlock", re.IGNORECASE),
                re.compile(r"lock\s+wait", re.IGNORECASE),
                re.compile(r"could not obtain lock", re.IGNORECASE),
                re.compile(r"lock(?:ed)?\s+not\s+available", re.IGNORECASE),
            ),
        ),
        (
            rf.FAILURE_TIMEOUT,
            "Timeout",
            (
                re.compile(r"timeout", re.IGNORECASE),
                re.compile(r"timed out", re.IGNORECASE),
                re.compile(r"statement timeout", re.IGNORECASE),
                re.compile(r"canceling statement due to statement timeout", re.IGNORECASE),
                re.compile(r"context deadline exceeded", re.IGNORECASE),
            ),
        ),
        (
            rf.FAILURE_RESOURCE_LIMIT,
            "Resource limit",
            (
                re.compile(r"out of memory", re.IGNORECASE),
                re.compile(r"cannot allocate memory", re.IGNORECASE),
                re.compile(r"no space left on device", re.IGNORECASE),
                re.compile(r"disk full", re.IGNORECASE),
                re.compile(r"too many connections", re.IGNORECASE),
                re.compile(r"resource temporarily unavailable", re.IGNORECASE),
            ),
        ),
        (
            rf.FAILURE_CONNECTION,
            "Connection failure",
            (
                re.compile(r"connection refused", re.IGNORECASE),
                re.compile(r"connection reset", re.IGNORECASE),
                re.compile(r"connection closed", re.IGNORECASE),
                re.compile(r"server closed the connection", re.IGNORECASE),
                re.compile(r"could not connect", re.IGNORECASE),
            ),
        ),
        (
            rf.FAILURE_BACKEND_STATE_CONFLICT,
            "Backend state conflict",
            (
                re.compile(r"already exists", re.IGNORECASE),
                re.compile(r"conflict", re.IGNORECASE),
                re.compile(r"serialization failure", re.IGNORECASE),
                re.compile(r"branches limit exceeded", re.IGNORECASE),
                re.compile(r"being accessed by other users", re.IGNORECASE),
            ),
        ),
        (
            rf.FAILURE_CONSTRAINT_OR_DATA,
            "Constraint or data issue",
            (
                re.compile(r"duplicate key", re.IGNORECASE),
                re.compile(r"violates .* constraint", re.IGNORECASE),
                re.compile(r"invalid input syntax", re.IGNORECASE),
                re.compile(r"out of range", re.IGNORECASE),
                re.compile(r"not-null constraint", re.IGNORECASE),
            ),
        ),
    )

    @classmethod
    def classify(cls, error: Exception) -> FailureClassification:
        message = cls._error_message(error)
        sqlstate = cls._extract_sqlstate(error, message)

        for category, reason, patterns in cls._RULES:
            if cls._matches(category, sqlstate, message, patterns):
                return FailureClassification(
                    category=category,
                    reason=cls._compose_reason(reason, message),
                    sqlstate=sqlstate,
                )

        if isinstance(error, (AssertionError, NotImplementedError)):
            return FailureClassification(
                category=rf.FAILURE_INTERNAL_BUG,
                reason=cls._compose_reason("Internal bug", message),
                sqlstate=sqlstate,
            )

        if sqlstate.startswith("XX"):
            return FailureClassification(
                category=rf.FAILURE_INTERNAL_BUG,
                reason=cls._compose_reason("Internal bug", message),
                sqlstate=sqlstate,
            )

        return FailureClassification(
            category=rf.FAILURE_UNKNOWN,
            reason=cls._compose_reason("Unknown failure", message),
            sqlstate=sqlstate,
        )

    @classmethod
    def _matches(
        cls,
        category: int,
        sqlstate: str,
        message: str,
        patterns: Iterable[re.Pattern[str]],
    ) -> bool:
        if category == rf.FAILURE_LOCK_CONTENTION and sqlstate in cls._LOCK_CODES:
            return True
        if category == rf.FAILURE_TIMEOUT and sqlstate in cls._TIMEOUT_CODES:
            return True
        if category == rf.FAILURE_RESOURCE_LIMIT and sqlstate in cls._RESOURCE_CODES:
            return True
        if category == rf.FAILURE_CONNECTION and sqlstate.startswith("08"):
            return True
        if (
            category == rf.FAILURE_BACKEND_STATE_CONFLICT
            and sqlstate in cls._STATE_CONFLICT_CODES
        ):
            return True
        if category == rf.FAILURE_CONSTRAINT_OR_DATA and (
            sqlstate.startswith("22") or sqlstate.startswith("23")
        ):
            return True

        return any(pattern.search(message) for pattern in patterns)

    @staticmethod
    def _error_message(error: Exception) -> str:
        message = str(error).strip()
        return message if message else repr(error)

    @staticmethod
    def _extract_sqlstate(error: Exception, message: str) -> str:
        pgcode = getattr(error, "pgcode", None)
        if pgcode:
            return str(pgcode).strip()

        match = re.search(
            r"(?:SQLSTATE\s*[:=]?\s*|pgcode\s*[:=]?\s*)([0-9A-Z]{5})",
            message,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).upper()

        diag = getattr(error, "diag", None)
        sqlstate = getattr(diag, "sqlstate", None) if diag else None
        return sqlstate.upper() if isinstance(sqlstate, str) else ""

    @staticmethod
    def _compose_reason(prefix: str, message: str) -> str:
        first_line = message.splitlines()[0] if message else ""
        trimmed = first_line[:200]
        return f"{prefix}: {trimmed}" if trimmed else prefix


def classify_failure(error: Exception) -> FailureClassification:
    return FailureClassifier.classify(error)
