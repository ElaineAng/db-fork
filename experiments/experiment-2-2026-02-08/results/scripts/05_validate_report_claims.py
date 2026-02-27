#!/usr/bin/env python3
"""Validate key numerical claims in Experiment 2 REPORT.md.

Recomputes metrics directly from measurement parquets and writes a JSON result.
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


EXPECTED_ROW_COUNTS_TOTAL = {
    "dolt": 3190,
    "file_copy": 3190,
    "neon": 1160,
    "xata": 1014,
}

EXPECTED_ROW_COUNTS_EXP_BACKEND = {
    ("2a", "dolt"): 2310,
    ("2a", "file_copy"): 2310,
    ("2a", "neon"): 840,
    ("2a", "xata"): 710,
    ("2b", "dolt"): 880,
    ("2b", "file_copy"): 880,
    ("2b", "neon"): 320,
    ("2b", "xata"): 304,
}

EXPECTED_XATA_FILTER = {"total": 1014, "invalid": 32, "valid": 982}

EXPECTED_OVERALL_FILTERED = {
    "dolt": {"total": 3190, "zero": 3169, "non_zero": 21},
    "file_copy": {"total": 3190, "zero": 3168, "non_zero": 22},
    "neon": {"total": 1160, "zero": 1088, "non_zero": 72},
    "xata": {"total": 982, "zero": 910, "non_zero": 72},
}

EXPECTED_TOPO_ZERO_EXP2A_FILTERED = {
    "dolt": {"spine": 99.35, "bushy": 99.09, "fan_out": 99.35, "spread": 0.26},
    "file_copy": {"spine": 99.74, "bushy": 99.87, "fan_out": 100.00, "spread": 0.26},
    "neon": {"spine": 96.07, "bushy": 94.64, "fan_out": 95.36, "spread": 1.43},
    "xata": {"spine": 94.71, "bushy": 94.74, "fan_out": 94.90, "spread": 0.19},
}

EXPECTED_PER_KEY_MEAN_FILTERED = {
    "dolt": {1: 0.0, 10: 506.4, 20: 14.9, 50: 101.3, 100: 0.0},
    "file_copy": {1: 0.0, 10: 0.0, 20: 3.7, 50: 15.6, 100: 59.0},
    "neon": {1: 614.4, 10: 51.2, 20: 25.6, 50: 22.5, 100: 13.3},
    "xata": {1: -409.6, 10: 187314.7, 20: 13594.8, 50: 24389.6, 100: 17344.8},
}


def load_data(data_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    dfs = []
    for path in sorted(glob.glob(os.path.join(data_dir, "*.parquet"))):
        stem = Path(path).stem
        if stem.endswith("_setup"):
            continue
        m = PATTERN.match(stem)
        if not m:
            continue
        backend = m.group(1)
        n = int(m.group(2))
        topology = m.group(3)
        op = m.group(4).upper()
        r = int(m.group(5)) if m.group(5) else (20 if op == "RANGE_UPDATE" else None)
        exp = "2a" if (op == "UPDATE" or (op == "RANGE_UPDATE" and r == 20)) else "2b"

        df = pd.read_parquet(path)
        df["backend"] = backend
        df["N"] = n
        df["topology"] = topology
        df["operation"] = op
        df["range_size"] = r
        df["exp"] = exp
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        df["is_xata_invalid"] = (
            (df["backend"] == "xata")
            & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0))
        )
        dfs.append(df)

    if not dfs:
        raise RuntimeError(f"No measurement parquet files found in {data_dir}")

    raw = pd.concat(dfs, ignore_index=True)
    filtered = raw[~raw["is_xata_invalid"]].copy()
    return raw, filtered


def _to_2(v: float) -> float:
    return round(float(v), 2)


def _to_1(v: float) -> float:
    return round(float(v), 1)


def check_row_counts(raw: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    actual_total = raw.groupby("backend").size().to_dict()
    checks.append(
        CheckResult(
            check_id="row_counts_total_by_backend",
            passed=actual_total == EXPECTED_ROW_COUNTS_TOTAL,
            expected=EXPECTED_ROW_COUNTS_TOTAL,
            actual=actual_total,
            note="Total measurement rows by backend",
        )
    )

    actual_exp_backend = raw.groupby(["exp", "backend"]).size().to_dict()
    checks.append(
        CheckResult(
            check_id="row_counts_exp_backend",
            passed=actual_exp_backend == EXPECTED_ROW_COUNTS_EXP_BACKEND,
            expected={f"{k[0]}::{k[1]}": v for k, v in EXPECTED_ROW_COUNTS_EXP_BACKEND.items()},
            actual={f"{k[0]}::{k[1]}": v for k, v in actual_exp_backend.items()},
            note="Exp2a/Exp2b row counts by backend",
        )
    )
    return checks


def check_xata_filter(raw: pd.DataFrame, filtered: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    xraw = raw[raw.backend == "xata"]
    xflt = filtered[filtered.backend == "xata"]
    actual = {
        "total": int(len(xraw)),
        "invalid": int(xraw["is_xata_invalid"].sum()),
        "valid": int(len(xflt)),
    }
    checks.append(
        CheckResult(
            check_id="xata_filter_counts",
            passed=actual == EXPECTED_XATA_FILTER,
            expected=EXPECTED_XATA_FILTER,
            actual=actual,
            note="Xata rows filtered by zero disk metrics",
        )
    )
    return checks


def check_overall_filtered(filtered: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    actual = {}
    for backend, g in filtered.groupby("backend"):
        zero = int((g["storage_delta"] == 0).sum())
        actual[backend] = {
            "total": int(len(g)),
            "zero": zero,
            "non_zero": int(len(g) - zero),
        }
    checks.append(
        CheckResult(
            check_id="overall_zero_nonzero_filtered",
            passed=actual == EXPECTED_OVERALL_FILTERED,
            expected=EXPECTED_OVERALL_FILTERED,
            actual=actual,
            note="Overall zero/non-zero counts after Xata filtering",
        )
    )
    return checks


def check_topology_zero_exp2a(filtered: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    exp2a = filtered[
        (filtered["operation"] == "UPDATE")
        | ((filtered["operation"] == "RANGE_UPDATE") & (filtered["range_size"] == 20))
    ]

    actual = {}
    for backend, bsub in exp2a.groupby("backend"):
        vals = {}
        for topo in ("spine", "bushy", "fan_out"):
            tsub = bsub[bsub.topology == topo]
            vals[topo] = _to_2((tsub["storage_delta"] == 0).mean() * 100)
        vals["spread"] = _to_2(max(vals.values()) - min(vals.values()))
        actual[backend] = vals

    passed = True
    for backend, expvals in EXPECTED_TOPO_ZERO_EXP2A_FILTERED.items():
        actvals = actual.get(backend, {})
        for k, v in expvals.items():
            if actvals.get(k) != v:
                passed = False
                break
        if not passed:
            break

    checks.append(
        CheckResult(
            check_id="exp2a_zero_by_topology_filtered",
            passed=passed,
            expected=EXPECTED_TOPO_ZERO_EXP2A_FILTERED,
            actual=actual,
            note="Zero-delta percentages by topology and spread (pp), filtered",
        )
    )
    return checks


def check_quantization(raw: pd.DataFrame, filtered: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []

    # Dolt
    d_nz = sorted(raw[(raw.backend == "dolt") & (raw.storage_delta != 0)]["storage_delta"].unique().tolist())
    checks.append(
        CheckResult(
            check_id="quant_dolt_nonzero_values",
            passed=d_nz == [16384, 65536, 1048576],
            expected=[16384, 65536, 1048576],
            actual=d_nz,
            note="Dolt non-zero values",
        )
    )

    # file_copy
    f_nz = raw[(raw.backend == "file_copy") & (raw.storage_delta != 0)]["storage_delta"]
    f_values = sorted(f_nz.unique().tolist())
    f_8k = int((f_nz % 8192 == 0).sum())
    f_4k_not8 = int(((f_nz % 4096 == 0) & (f_nz % 8192 != 0)).sum())
    checks.append(
        CheckResult(
            check_id="quant_file_copy_values_and_split",
            passed=(f_values == [8192, 12288, 16384, 65536, 69632, 1069056] and f_8k == 17 and f_4k_not8 == 5),
            expected={"values": [8192, 12288, 16384, 65536, 69632, 1069056], "8k_count": 17, "4k_not8_count": 5},
            actual={"values": f_values, "8k_count": f_8k, "4k_not8_count": f_4k_not8},
            note="file_copy non-zero set and 17/5 quantization split",
        )
    )

    # Neon
    n_nz = raw[(raw.backend == "neon") & (raw.storage_delta != 0)]["storage_delta"]
    n_counts = n_nz.value_counts().sort_index().to_dict()
    checks.append(
        CheckResult(
            check_id="quant_neon_70x8k_2x16k",
            passed=n_counts == {8192: 70, 16384: 2},
            expected={8192: 70, 16384: 2},
            actual=n_counts,
            note="Neon non-zero quantization counts",
        )
    )

    # Xata unfiltered and filtered splits
    x_nz_raw = raw[(raw.backend == "xata") & (raw.storage_delta != 0)]["storage_delta"]
    x_nz_flt = filtered[(filtered.backend == "xata") & (filtered.storage_delta != 0)]["storage_delta"]

    x_raw_8k = int(((x_nz_raw > 0) & (x_nz_raw % 8192 == 0)).sum())
    x_raw_not8_pos = int(((x_nz_raw > 0) & (x_nz_raw % 8192 != 0)).sum())
    x_raw_neg = int((x_nz_raw < 0).sum())

    x_flt_8k = int(((x_nz_flt > 0) & (x_nz_flt % 8192 == 0)).sum())
    x_flt_not8_pos = int(((x_nz_flt > 0) & (x_nz_flt % 8192 != 0)).sum())
    x_flt_neg = int((x_nz_flt < 0).sum())

    checks.append(
        CheckResult(
            check_id="quant_xata_unfiltered_split",
            passed=(x_raw_8k == 35 and x_raw_not8_pos == 33 and x_raw_neg == 6),
            expected={"8k_pos": 35, "not8_pos": 33, "neg": 6},
            actual={"8k_pos": x_raw_8k, "not8_pos": x_raw_not8_pos, "neg": x_raw_neg},
            note="Xata unfiltered split",
        )
    )
    checks.append(
        CheckResult(
            check_id="quant_xata_filtered_split",
            passed=(x_flt_8k == 35 and x_flt_not8_pos == 31 and x_flt_neg == 6),
            expected={"8k_pos": 35, "not8_pos": 31, "neg": 6},
            actual={"8k_pos": x_flt_8k, "not8_pos": x_flt_not8_pos, "neg": x_flt_neg},
            note="Xata filtered split",
        )
    )
    return checks


def check_per_key(filtered: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []
    ru = filtered[
        (filtered.operation == "RANGE_UPDATE")
        & (filtered.topology == "spine")
        & (filtered.range_size.isin([1, 10, 20, 50, 100]))
        & (filtered.num_keys_touched > 0)
    ].copy()
    ru["per_key"] = ru["storage_delta"] / ru["num_keys_touched"]

    actual = {}
    passed = True
    for backend, ranges in EXPECTED_PER_KEY_MEAN_FILTERED.items():
        actual[backend] = {}
        for r, expected_v in ranges.items():
            s = ru[(ru.backend == backend) & (ru.range_size == r)]["per_key"]
            val = _to_1(s.mean()) if len(s) else None
            actual[backend][r] = val
            if val != expected_v:
                passed = False

    checks.append(
        CheckResult(
            check_id="per_key_means_filtered",
            passed=passed,
            expected=EXPECTED_PER_KEY_MEAN_FILTERED,
            actual=actual,
            note="Per-key mean table values (filtered)",
        )
    )
    return checks


def check_xata_n16(filtered: pd.DataFrame, raw: pd.DataFrame) -> list[CheckResult]:
    checks: list[CheckResult] = []

    x16_update_raw = raw[(raw.backend == "xata") & (raw.operation == "UPDATE") & (raw.N == 16)]
    x16_update_flt = filtered[(filtered.backend == "xata") & (filtered.operation == "UPDATE") & (filtered.N == 16)]
    x16_r20_raw = raw[
        (raw.backend == "xata")
        & (raw.operation == "RANGE_UPDATE")
        & (raw.range_size == 20)
        & (raw.N == 16)
    ]
    x16_r20_flt = filtered[
        (filtered.backend == "xata")
        & (filtered.operation == "RANGE_UPDATE")
        & (filtered.range_size == 20)
        & (filtered.N == 16)
    ]

    checks.append(
        CheckResult(
            check_id="xata_n16_update_missing_after_filter",
            passed=(len(x16_update_raw) == 3 and int(x16_update_raw["is_xata_invalid"].sum()) == 3 and len(x16_update_flt) == 0),
            expected={"raw_rows": 3, "raw_invalid": 3, "filtered_rows": 0},
            actual={"raw_rows": int(len(x16_update_raw)), "raw_invalid": int(x16_update_raw["is_xata_invalid"].sum()), "filtered_rows": int(len(x16_update_flt))},
            note="Xata N=16 UPDATE is fully removed by filtering",
        )
    )

    checks.append(
        CheckResult(
            check_id="xata_n16_r20_valid_and_mean",
            passed=(len(x16_r20_raw) == 22 and int(x16_r20_raw["is_xata_invalid"].sum()) == 2 and len(x16_r20_flt) == 20 and _to_1(x16_r20_flt["storage_delta"].mean()) == 1642431.8),
            expected={"raw_rows": 22, "raw_invalid": 2, "filtered_rows": 20, "filtered_mean": 1642431.8},
            actual={"raw_rows": int(len(x16_r20_raw)), "raw_invalid": int(x16_r20_raw["is_xata_invalid"].sum()), "filtered_rows": int(len(x16_r20_flt)), "filtered_mean": _to_1(x16_r20_flt["storage_delta"].mean()) if len(x16_r20_flt) else None},
            note="Xata N=16 RANGE_UPDATE r=20 validity and mean",
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

    raw, filtered = load_data(data_dir)

    checks: list[CheckResult] = []
    checks.extend(check_row_counts(raw))
    checks.extend(check_xata_filter(raw, filtered))
    checks.extend(check_overall_filtered(filtered))
    checks.extend(check_topology_zero_exp2a(filtered))
    checks.extend(check_quantization(raw, filtered))
    checks.extend(check_per_key(filtered))
    checks.extend(check_xata_n16(filtered, raw))

    passed = all(c.passed for c in checks)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_dir": data_dir,
        "report_path": str(Path(__file__).resolve().parent.parent / "REPORT.md"),
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
