#!/usr/bin/env python3
"""Validate key numerical claims in Experiment 2 REPORT.md.

Reads measurement parquet files, recomputes headline metrics, and writes a
machine-checkable JSON artifact.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PATTERN = re.compile(
    r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_(update|range_update)(?:_r(\d+))?$"
)


@dataclass
class CheckResult:
    check_id: str
    passed: bool
    expected: object
    actual: object
    note: str


EXPECTED_TOTALS = {
    "dolt": {"total": 3190, "non_zero": 21, "non_zero_fraction_pct_1dp": 0.7},
    "file_copy": {
        "total": 3190,
        "non_zero": 22,
        "non_zero_fraction_pct_1dp": 0.7,
    },
    "neon": {"total": 1160, "non_zero": 72, "non_zero_fraction_pct_1dp": 6.2},
    "xata": {"total": 1014, "non_zero": 74, "non_zero_fraction_pct_1dp": 7.3},
}

EXPECTED_TOPO_ZERO = {
    "dolt": {"spine": 99.4, "bushy": 99.1, "fan_out": 99.4, "spread": 0.3},
    "file_copy": {
        "spine": 99.7,
        "bushy": 99.9,
        "fan_out": 100.0,
        "spread": 0.3,
    },
    "neon": {"spine": 96.1, "bushy": 94.6, "fan_out": 95.4, "spread": 1.5},
    "xata": {"spine": 94.9, "bushy": 94.4, "fan_out": 95.1, "spread": 0.7},
}

EXPECTED_PER_KEY = {
    "dolt": {1: 0.0, 10: 506.4, 20: 14.9, 50: 101.3, 100: 0.0},
    "file_copy": {1: 0.0, 10: 0.0, 20: 3.7, 50: 15.6, 100: 59.0},
    "neon": {1: 614.4, 10: 51.2, 20: 25.6, 50: 22.5, 100: 13.3},
    "xata": {1: -409.6, 10: 184973.3, 20: 13594.8, 50: 24601.7, 100: 17344.8},
}


def load_data(data_dir: str) -> pd.DataFrame:
    dfs = []
    for path in sorted(glob.glob(os.path.join(data_dir, "*.parquet"))):
        stem = Path(path).stem
        if stem.endswith("_setup"):
            continue
        m = PATTERN.match(stem)
        if not m:
            continue
        df = pd.read_parquet(path)
        df["backend"] = m.group(1)
        df["N"] = int(m.group(2))
        df["topology"] = m.group(3)
        df["operation"] = m.group(4).upper()
        df["range_size"] = int(m.group(5)) if m.group(5) else None
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No measurement parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


def close_1dp(a: float, b: float) -> bool:
    return round(float(a), 1) == round(float(b), 1)


def check_totals(df: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    for backend, expected in EXPECTED_TOTALS.items():
        sub = df[df.backend == backend]
        total = int(len(sub))
        non_zero = int((sub.storage_delta != 0).sum())
        frac = round(non_zero / total * 100, 1) if total else 0.0
        passed = (
            total == expected["total"]
            and non_zero == expected["non_zero"]
            and frac == expected["non_zero_fraction_pct_1dp"]
        )
        checks.append(
            CheckResult(
                check_id=f"totals_{backend}",
                passed=passed,
                expected=expected,
                actual={
                    "total": total,
                    "non_zero": non_zero,
                    "non_zero_fraction_pct_1dp": frac,
                },
                note="Headline totals/non-zero table",
            )
        )
    return checks


def check_topology_zero(df: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    exp2a = df[(df.operation == "UPDATE") | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))]

    for backend, expected in EXPECTED_TOPO_ZERO.items():
        sub = exp2a[exp2a.backend == backend]
        actual = {}
        values = []
        for topo in ("spine", "bushy", "fan_out"):
            tsub = sub[sub.topology == topo]
            pct = (tsub.storage_delta == 0).mean() * 100 if len(tsub) else float("nan")
            pct = round(float(pct), 1)
            actual[topo] = pct
            values.append(pct)
        spread = round(max(values) - min(values), 1)
        actual["spread"] = spread

        passed = all(close_1dp(actual[k], expected[k]) for k in ("spine", "bushy", "fan_out", "spread"))
        checks.append(
            CheckResult(
                check_id=f"topology_zero_{backend}",
                passed=passed,
                expected=expected,
                actual=actual,
                note="Zero-delta by topology table",
            )
        )
    return checks


def check_specific_claims(df: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []

    # Dolt UPDATE non-zero exists at N=16/32/64.
    d = df[(df.backend == "dolt") & (df.operation == "UPDATE")]
    nz_ns = sorted(int(n) for n in d[d.storage_delta != 0]["N"].unique().tolist())
    required = [16, 32, 64]
    checks.append(
        CheckResult(
            check_id="dolt_update_n_ge_8_not_all_zero",
            passed=all(n in nz_ns for n in required),
            expected={"must_include_non_zero_N": required},
            actual={"non_zero_N": nz_ns},
            note="Refutes old claim that Dolt UPDATE is zero for all N>=8",
        )
    )

    # Xata N=16 split by operation.
    x_update = df[(df.backend == "xata") & (df.operation == "UPDATE") & (df.N == 16)]
    x_r20 = df[
        (df.backend == "xata")
        & (df.operation == "RANGE_UPDATE")
        & (df.range_size == 20)
        & (df.N == 16)
    ]
    checks.append(
        CheckResult(
            check_id="xata_n16_update",
            passed=(len(x_update) == 3 and round(float(x_update.storage_delta.mean()), 1) == 0.0),
            expected={"rows": 3, "mean": 0.0},
            actual={
                "rows": int(len(x_update)),
                "mean": round(float(x_update.storage_delta.mean()), 1) if len(x_update) else None,
            },
            note="Xata UPDATE at N=16",
        )
    )
    checks.append(
        CheckResult(
            check_id="xata_n16_range20",
            passed=(len(x_r20) == 22 and round(float(x_r20.storage_delta.mean()), 1) == 1493119.8),
            expected={"rows": 22, "mean": 1493119.8},
            actual={
                "rows": int(len(x_r20)),
                "mean": round(float(x_r20.storage_delta.mean()), 1) if len(x_r20) else None,
            },
            note="Xata RANGE_UPDATE(r=20) at N=16",
        )
    )

    return checks


def check_per_key(df: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    ru = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")].copy()
    ru["per_key"] = ru.storage_delta / ru.num_keys_touched

    for backend, expected in EXPECTED_PER_KEY.items():
        sub = ru[ru.backend == backend]
        actual = {}
        passed = True
        for r in (1, 10, 20, 50, 100):
            s = sub[sub.range_size == r]["per_key"]
            val = round(float(s.mean()), 1) if len(s) else None
            actual[r] = val
            if val is None or val != round(expected[r], 1):
                passed = False
        checks.append(
            CheckResult(
                check_id=f"per_key_{backend}",
                passed=passed,
                expected={str(k): round(v, 1) for k, v in expected.items()},
                actual={str(k): v for k, v in actual.items()},
                note="Per-key mean table (spine, all N)",
            )
        )
    return checks


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Exp2 report claims")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
    )
    parser.add_argument(
        "--output-json",
        default=os.path.join(os.path.dirname(__file__), "..", "data", "report_claim_checks.json"),
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_json = os.path.abspath(args.output_json)

    df = load_data(data_dir)

    checks: list[CheckResult] = []
    checks.extend(check_totals(df))
    checks.extend(check_topology_zero(df))
    checks.extend(check_specific_claims(df))
    checks.extend(check_per_key(df))

    passed = all(c.passed for c in checks)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_dir": data_dir,
        "report_path": str(
            Path(__file__).resolve().parent.parent / "REPORT.md"
        ),
        "passed": passed,
        "num_checks": len(checks),
        "num_failed": sum(1 for c in checks if not c.passed),
        "checks": [
            {
                "check_id": c.check_id,
                "passed": c.passed,
                "expected": c.expected,
                "actual": c.actual,
                "note": c.note,
            }
            for c in checks
        ],
    }

    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    print(f"Wrote: {output_json}")
    print(f"passed={passed} failed={payload['num_failed']}/{payload['num_checks']}")


if __name__ == "__main__":
    main()
