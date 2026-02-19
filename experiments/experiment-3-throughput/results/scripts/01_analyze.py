#!/usr/bin/env python3
"""Compute numerical metrics for Experiment 3: Operation Throughput Under Branching.

Loads throughput parquet files from the data directory, computes goodput
(ops/sec) per thread and aggregate, and prints structured sections.

Covers:
  Exp 3a: Branch creation throughput (T concurrent threads, fixed duration)
  Exp 3b: CRUD throughput under branching (N branches, N threads, fixed duration)

Filename convention:
  {backend}_{sql_prefix}_{topology}_{T}t_{mode}_throughput.parquet
  e.g. dolt_tpcc_spine_4t_branch_throughput.parquet
       file_copy_tpcc_fan_out_16t_crud_throughput.parquet

Usage:
    python 01_analyze.py [--data-dir ../data] [--duration 30]
"""

import argparse
import glob
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_throughput_filename(filepath: str) -> dict | None:
    """Extract metadata from throughput filenames."""
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None

    m = re.match(
        r"^(dolt|file_copy|neon)_\w+_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_throughput$",
        stem,
    )
    if not m:
        return None
    return {
        "backend": m.group(1),
        "topology": m.group(2),
        "T": int(m.group(3)),
        "mode": m.group(4),
    }


def load_all(data_dir: str) -> pd.DataFrame:
    """Load all throughput parquet files into a single DataFrame."""
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []
    for f in files:
        meta = parse_throughput_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["topology"] = meta["topology"]
        df["T"] = meta["T"]
        df["mode"] = meta["mode"]
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No throughput parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL CoW)", "neon": "Neon"}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
T_VALUES = [1, 2, 4, 8, 16, 32, 64, 128]


# ---------------------------------------------------------------------------
# Goodput computation
# ---------------------------------------------------------------------------
def compute_goodput(df: pd.DataFrame, duration: float) -> pd.DataFrame:
    """Compute per-thread and aggregate goodput.

    Returns a DataFrame with columns:
        backend, topology, T, mode, thread_id, ops_count, goodput_ops_sec
    """
    grouped = (
        df.groupby(["backend", "topology", "T", "mode", "thread_id"])
        .size()
        .reset_index(name="ops_count")
    )
    grouped["goodput_ops_sec"] = grouped["ops_count"] / duration
    return grouped


def compute_aggregate_goodput(per_thread: pd.DataFrame) -> pd.DataFrame:
    """Sum per-thread goodput → aggregate goodput per (backend, topology, T, mode)."""
    return (
        per_thread
        .groupby(["backend", "topology", "T", "mode"])
        .agg(
            total_ops=("ops_count", "sum"),
            aggregate_goodput=("goodput_ops_sec", "sum"),
            mean_per_thread=("goodput_ops_sec", "mean"),
            std_per_thread=("goodput_ops_sec", "std"),
            min_per_thread=("goodput_ops_sec", "min"),
            max_per_thread=("goodput_ops_sec", "max"),
            num_threads_actual=("thread_id", "nunique"),
        )
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
def section_overview(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 1: Data Overview")
    print("=" * 70)
    print()

    print(f"Total operation rows: {len(df)}")
    print(f"Unique run_ids: {df['run_id'].nunique()}")
    print()

    for mode in ["branch", "crud"]:
        sub = df[df["mode"] == mode]
        if sub.empty:
            continue
        label = "Exp 3a (branch creation)" if mode == "branch" else "Exp 3b (CRUD)"
        print(f"{label}:")
        summary = sub.groupby(["backend", "topology", "T"]).agg(
            rows=("run_id", "count"),
            threads=("thread_id", "nunique"),
        ).reset_index()
        for _, row in summary.iterrows():
            print(f"  {row.backend:<12} {row.topology:<10} T={row.T:>3}  "
                  f"rows={row.rows:>6}  threads={row.threads}")
        print()


def section_branch_throughput(agg: pd.DataFrame):
    print("=" * 70)
    print("SECTION 2: Branch Creation Throughput (Exp 3a)")
    print("=" * 70)
    print()

    branch = agg[agg["mode"] == "branch"]
    if branch.empty:
        print("  No branch throughput data found.")
        print()
        return

    for backend in BACKENDS:
        bsub = branch[branch.backend == backend]
        if bsub.empty:
            continue
        print(f"--- {BACKEND_LABELS[backend]} ---")
        print(f"{'T':>5}  {'agg ops/s':>12}  {'mean/thread':>12}  {'std':>10}  {'total ops':>10}")
        for _, row in bsub.sort_values("T").iterrows():
            std_str = f"{row.std_per_thread:.1f}" if pd.notna(row.std_per_thread) else "—"
            print(f"{row.T:>5}  {row.aggregate_goodput:>12.1f}  "
                  f"{row.mean_per_thread:>12.1f}  {std_str:>10}  {row.total_ops:>10}")
        print()


def section_crud_throughput(agg: pd.DataFrame):
    print("=" * 70)
    print("SECTION 3: CRUD Throughput Under Branching (Exp 3b)")
    print("=" * 70)
    print()

    crud = agg[agg["mode"] == "crud"]
    if crud.empty:
        print("  No CRUD throughput data found.")
        print()
        return

    for backend in BACKENDS:
        bsub = crud[crud.backend == backend]
        if bsub.empty:
            continue
        print(f"--- {BACKEND_LABELS[backend]} ---")
        print(f"{'T':>5}  {'topology':<10}  {'agg ops/s':>12}  "
              f"{'mean/thread':>12}  {'std':>10}  {'total ops':>10}")
        for _, row in bsub.sort_values(["topology", "T"]).iterrows():
            std_str = f"{row.std_per_thread:.1f}" if pd.notna(row.std_per_thread) else "—"
            print(f"{row.T:>5}  {row.topology:<10}  {row.aggregate_goodput:>12.1f}  "
                  f"{row.mean_per_thread:>12.1f}  {std_str:>10}  {row.total_ops:>10}")
        print()


def section_latency(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 4: Per-Operation Latency Summary")
    print("=" * 70)
    print()

    for mode in ["branch", "crud"]:
        sub = df[df["mode"] == mode]
        if sub.empty:
            continue
        label = "Exp 3a (branch)" if mode == "branch" else "Exp 3b (CRUD)"
        print(f"--- {label} ---")

        for backend in BACKENDS:
            bsub = sub[sub.backend == backend]
            if bsub.empty:
                continue
            print(f"  {BACKEND_LABELS[backend]}:")
            key_ts = [1, 4, 16, 64, 128]
            print(f"  {'T':>5}", end="")
            for topo in TOPO_ORDER:
                print(f"  {topo + ' (ms)':>15}", end="")
            print()

            for t in key_ts:
                print(f"  {t:>5}", end="")
                for topo in TOPO_ORDER:
                    tsub = bsub[(bsub.topology == topo) & (bsub.T == t)]
                    if tsub.empty:
                        print(f"  {'—':>15}", end="")
                    else:
                        mean_ms = tsub.latency.mean() * 1000
                        print(f"  {mean_ms:>15.2f}", end="")
                print()
            print()


def section_scaling(agg: pd.DataFrame):
    print("=" * 70)
    print("SECTION 5: Scaling Efficiency")
    print("=" * 70)
    print()

    for mode in ["branch", "crud"]:
        sub = agg[agg["mode"] == mode]
        if sub.empty:
            continue
        label = "Exp 3a (branch)" if mode == "branch" else "Exp 3b (CRUD)"
        print(f"--- {label} ---")

        for backend in BACKENDS:
            bsub = sub[sub.backend == backend]
            if bsub.empty:
                continue
            print(f"  {BACKEND_LABELS[backend]}:")

            for topo in TOPO_ORDER:
                tsub = bsub[bsub.topology == topo].sort_values("T")
                if tsub.empty:
                    continue
                baseline = tsub.iloc[0]["aggregate_goodput"]
                if baseline == 0:
                    continue
                print(f"    {topo}:")
                print(f"    {'T':>5}  {'agg ops/s':>12}  {'speedup':>8}  {'efficiency':>10}")
                for _, row in tsub.iterrows():
                    speedup = row.aggregate_goodput / baseline
                    efficiency = speedup / row.T * 100
                    print(f"    {row.T:>5}  {row.aggregate_goodput:>12.1f}  "
                          f"{speedup:>8.2f}  {efficiency:>9.1f}%")
                print()


def section_research_questions(agg: pd.DataFrame, per_thread: pd.DataFrame):
    print("=" * 70)
    print("SECTION 6: Research Question Answers")
    print("=" * 70)
    print()

    # RQ1: How does branch creation throughput scale with concurrent threads?
    print("RQ1: How does branch creation throughput scale with concurrent threads?")
    branch = agg[agg["mode"] == "branch"]
    for backend in BACKENDS:
        bsub = branch[branch.backend == backend]
        if bsub.empty:
            continue
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo].sort_values("T")
            if len(tsub) < 2:
                continue
            t1 = tsub.iloc[0]
            t_max = tsub.iloc[-1]
            speedup = t_max.aggregate_goodput / t1.aggregate_goodput if t1.aggregate_goodput > 0 else 0
            print(f"  {BACKEND_LABELS[backend]} {topo}: "
                  f"T=1 → T={int(t_max.T)}: {speedup:.1f}x speedup "
                  f"({t1.aggregate_goodput:.0f} → {t_max.aggregate_goodput:.0f} ops/s)")
    print()

    # RQ2: Does per-branch CRUD throughput degrade as N grows?
    print("RQ2: Does per-branch CRUD throughput degrade as N grows?")
    crud = agg[agg["mode"] == "crud"]
    for backend in BACKENDS:
        bsub = crud[crud.backend == backend]
        if bsub.empty:
            continue
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo].sort_values("T")
            if len(tsub) < 2:
                continue
            t1 = tsub.iloc[0]
            t_max = tsub.iloc[-1]
            if t1.mean_per_thread > 0:
                degradation = (1 - t_max.mean_per_thread / t1.mean_per_thread) * 100
                print(f"  {BACKEND_LABELS[backend]} {topo}: "
                      f"per-thread degradation T=1→T={int(t_max.T)}: {degradation:.1f}% "
                      f"({t1.mean_per_thread:.0f} → {t_max.mean_per_thread:.0f} ops/s)")
    print()

    # RQ3: Does topology affect throughput distribution?
    print("RQ3: Does topology affect throughput distribution?")
    for backend in BACKENDS:
        bsub = per_thread[(per_thread.backend == backend) & (per_thread.mode == "crud")]
        if bsub.empty:
            continue
        max_t = bsub.T.max()
        at_max = bsub[bsub.T == max_t]
        print(f"  {BACKEND_LABELS[backend]} at T={max_t}:")
        for topo in TOPO_ORDER:
            tsub = at_max[at_max.topology == topo]
            if tsub.empty:
                continue
            cv = tsub.goodput_ops_sec.std() / tsub.goodput_ops_sec.mean() * 100 if tsub.goodput_ops_sec.mean() > 0 else 0
            print(f"    {topo}: CV={cv:.1f}%, "
                  f"min={tsub.goodput_ops_sec.min():.0f}, "
                  f"max={tsub.goodput_ops_sec.max():.0f} ops/s")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 3 analysis")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Duration of each throughput run in seconds (default: 30)")
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    print(f"Loading data from: {data_dir}")
    print(f"Assumed duration: {args.duration}s")
    print()

    df = load_all(data_dir)
    per_thread = compute_goodput(df, args.duration)
    agg = compute_aggregate_goodput(per_thread)

    section_overview(df)
    section_branch_throughput(agg)
    section_crud_throughput(agg)
    section_latency(df)
    section_scaling(agg)
    section_research_questions(agg, per_thread)


if __name__ == "__main__":
    main()
