#!/usr/bin/env python3
"""Compute numerical metrics for Experiment 3 throughput runs."""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from pathlib import Path

import pandas as pd


NEW_NAME_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_[a-zA-Z0-9]+$"
)
LEGACY_NAME_RE = re.compile(
    r"^(dolt|file_copy|neon)_\w+_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_throughput$"
)

BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy (PostgreSQL CoW)",
    "neon": "Neon",
}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]


CATEGORY_NAMES = {
    0: "FAILURE_NONE",
    1: "FAILURE_LOCK_CONTENTION",
    2: "FAILURE_TIMEOUT",
    3: "FAILURE_RESOURCE_LIMIT",
    4: "FAILURE_CONNECTION",
    5: "FAILURE_BACKEND_STATE_CONFLICT",
    6: "FAILURE_CONSTRAINT_OR_DATA",
    7: "FAILURE_INTERNAL_BUG",
    8: "FAILURE_UNKNOWN",
}


def parse_run_stem(stem: str) -> dict | None:
    if stem.endswith("_setup"):
        return None

    match = NEW_NAME_RE.match(stem)
    if not match:
        match = LEGACY_NAME_RE.match(stem)
    if not match:
        return None

    return {
        "backend": match.group(1),
        "topology": match.group(2),
        "T": int(match.group(3)),
        "mode": match.group(4),
    }


def load_all_parquet(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []

    for path in files:
        meta = parse_run_stem(Path(path).stem)
        if meta is None:
            continue

        frame = pd.read_parquet(path)
        frame["backend"] = meta["backend"]
        frame["topology"] = meta["topology"]
        frame["T"] = meta["T"]
        frame["mode"] = meta["mode"]

        if "outcome_success" not in frame.columns:
            frame["outcome_success"] = True
        frame["outcome_success"] = frame["outcome_success"].fillna(False).astype(bool)

        if "failure_category" not in frame.columns:
            frame["failure_category"] = 0
        frame["failure_category"] = frame["failure_category"].fillna(0).astype(int)

        if "latency_threshold_exceeded" not in frame.columns:
            frame["latency_threshold_exceeded"] = False
        frame["latency_threshold_exceeded"] = (
            frame["latency_threshold_exceeded"].fillna(False).astype(bool)
        )

        dfs.append(frame)

    if not dfs:
        raise RuntimeError(f"No throughput parquet files found in {data_dir}")

    return pd.concat(dfs, ignore_index=True)


def load_summaries(data_dir: str) -> pd.DataFrame:
    rows: list[dict] = []
    for path in sorted(glob.glob(os.path.join(data_dir, "*_summary.json"))):
        stem = Path(path).stem
        run_stem = stem[:-8] if stem.endswith("_summary") else stem
        meta = parse_run_stem(run_stem)
        if meta is None:
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                summary = json.load(f)
        except Exception:
            continue

        rows.append(
            {
                **meta,
                "run_id": summary.get("run_id", run_stem),
                "attempted_ops": int(summary.get("attempted_ops", 0) or 0),
                "successful_ops": int(summary.get("successful_ops", 0) or 0),
                "failed_exception_ops": int(
                    summary.get("failed_exception_ops", 0) or 0
                ),
                "failed_slow_ops": int(summary.get("failed_slow_ops", 0) or 0),
                "success_rate": float(summary.get("success_rate", 0.0) or 0.0),
                "top_failure_category": str(
                    summary.get("top_failure_category", "") or ""
                ),
                "top_failure_reason": str(
                    summary.get("top_failure_reason", "") or ""
                ),
            }
        )

    return pd.DataFrame(rows)


def compute_goodput(df: pd.DataFrame, duration: float) -> pd.DataFrame:
    success_df = df[df["outcome_success"]].copy()
    grouped = (
        success_df.groupby(["backend", "topology", "T", "mode", "thread_id"])
        .size()
        .reset_index(name="ops_count")
    )
    grouped["goodput_ops_sec"] = grouped["ops_count"] / duration
    return grouped


def compute_aggregate_goodput(per_thread: pd.DataFrame) -> pd.DataFrame:
    return (
        per_thread.groupby(["backend", "topology", "T", "mode"])
        .agg(
            total_ops=("ops_count", "sum"),
            aggregate_goodput=("goodput_ops_sec", "sum"),
            mean_per_thread=("goodput_ops_sec", "mean"),
            std_per_thread=("goodput_ops_sec", "std"),
            num_threads_actual=("thread_id", "nunique"),
        )
        .reset_index()
    )


def compute_failure_table(df: pd.DataFrame) -> pd.DataFrame:
    staged = df.copy()
    staged["failed"] = ~staged["outcome_success"]
    staged["failed_slow"] = staged["latency_threshold_exceeded"] & staged["failed"]
    staged["failed_exception"] = staged["failed"] & ~staged["failed_slow"]

    summary = (
        staged.groupby(["backend", "topology", "T", "mode"])
        .agg(
            attempted=("failed", "size"),
            failed_total=("failed", "sum"),
            failed_exception=("failed_exception", "sum"),
            failed_slow=("failed_slow", "sum"),
        )
        .reset_index()
    )
    summary["failure_rate"] = summary["failed_total"] / summary["attempted"]
    return summary


def print_overview(df: pd.DataFrame, summaries: pd.DataFrame) -> None:
    print("=" * 70)
    print("SECTION 1: Data Overview")
    print("=" * 70)
    print()
    print(f"Total operation rows: {len(df)}")
    print(f"Successful operation rows: {int(df['outcome_success'].sum())}")
    print(f"Failed operation rows: {int((~df['outcome_success']).sum())}")
    print(f"Summary JSON files: {len(summaries)}")
    print()


def print_throughput(agg: pd.DataFrame) -> None:
    print("=" * 70)
    print("SECTION 2: Throughput (Successful Ops Only)")
    print("=" * 70)
    print()

    for mode in ["branch", "crud"]:
        sub = agg[agg["mode"] == mode]
        if sub.empty:
            continue
        mode_label = "Exp 3a (branch)" if mode == "branch" else "Exp 3b (CRUD)"
        print(f"--- {mode_label} ---")

        for backend in BACKENDS:
            bsub = sub[sub["backend"] == backend]
            if bsub.empty:
                continue

            print(f"{BACKEND_LABELS[backend]}:")
            print(
                f"{'T':>5}  {'topology':<10}  {'agg ops/s':>12}  {'mean/thread':>12}  {'total ops':>10}"
            )
            for _, row in bsub.sort_values(["T", "topology"]).iterrows():
                print(
                    f"{int(row['T']):>5}  {row['topology']:<10}  {row['aggregate_goodput']:>12.1f}  "
                    f"{row['mean_per_thread']:>12.1f}  {int(row['total_ops']):>10}"
                )
            print()


def print_failure_rates(failure_table: pd.DataFrame) -> None:
    print("=" * 70)
    print("SECTION 3: Failure Rates")
    print("=" * 70)
    print()

    if failure_table.empty:
        print("No failure data available.")
        print()
        return

    for backend in BACKENDS:
        bsub = failure_table[failure_table["backend"] == backend]
        if bsub.empty:
            continue

        print(f"{BACKEND_LABELS[backend]}:")
        print(
            f"{'T':>5}  {'mode':<6}  {'topology':<10}  {'attempted':>10}  {'failed':>8}  {'exc':>8}  {'slow':>8}  {'rate':>8}"
        )
        for _, row in bsub.sort_values(["T", "mode", "topology"]).iterrows():
            print(
                f"{int(row['T']):>5}  {row['mode']:<6}  {row['topology']:<10}  {int(row['attempted']):>10}  "
                f"{int(row['failed_total']):>8}  {int(row['failed_exception']):>8}  {int(row['failed_slow']):>8}  "
                f"{row['failure_rate']:>7.2%}"
            )
        print()


def print_top_failures(df: pd.DataFrame, summaries: pd.DataFrame) -> None:
    print("=" * 70)
    print("SECTION 4: Top Failure Categories and Reasons")
    print("=" * 70)
    print()

    failed = df[~df["outcome_success"]]
    if failed.empty:
        print("No failed operations in parquet rows.")
        print()
    else:
        category_counts = (
            failed["failure_category"]
            .value_counts()
            .rename_axis("failure_category")
            .reset_index(name="count")
        )
        print("Parquet failure categories:")
        for _, row in category_counts.head(10).iterrows():
            category_name = CATEGORY_NAMES.get(
                int(row["failure_category"]),
                str(row["failure_category"]),
            )
            print(f"  {category_name:<35} {int(row['count'])}")
        print()

    if summaries.empty:
        print("No summary JSON files found for top-failure extraction.")
        print()
        return

    cat_series = summaries["top_failure_category"].fillna("").astype(str)
    cat_series = cat_series[cat_series != ""]
    reason_series = summaries["top_failure_reason"].fillna("").astype(str)
    reason_series = reason_series[reason_series != ""]

    if cat_series.empty:
        print("Summary top_failure_category: none")
    else:
        print("Summary top_failure_category counts:")
        for category, count in cat_series.value_counts().head(10).items():
            print(f"  {category:<35} {int(count)}")
    print()

    if reason_series.empty:
        print("Summary top_failure_reason: none")
    else:
        print("Summary top_failure_reason counts:")
        for reason, count in reason_series.value_counts().head(10).items():
            print(f"  [{int(count)}] {reason}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 3 throughput analysis")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Duration of each throughput run in seconds (default: 30)",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    print(f"Loading data from: {data_dir}")

    df = load_all_parquet(data_dir)
    summaries = load_summaries(data_dir)

    per_thread = compute_goodput(df, args.duration)
    agg = compute_aggregate_goodput(per_thread)
    failure_table = compute_failure_table(df)

    print_overview(df, summaries)
    print_throughput(agg)
    print_failure_rates(failure_table)
    print_top_failures(df, summaries)


if __name__ == "__main__":
    main()
