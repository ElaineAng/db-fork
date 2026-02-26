#!/usr/bin/env python3
"""Compute numerical metrics for Experiment 1: Branch Creation Storage Overhead.

Loads all *_setup.parquet files from the data directory, computes storage deltas,
and prints structured sections for the report.

Usage:
    python 01_analyze.py [--data-dir ../data]
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
def fmt_bytes(b: float) -> str:
    """Human-readable byte string."""
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    elif abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    elif abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


def parse_filename(filepath: str) -> dict:
    """Extract backend, N, topology from filename like
    `dolt_tpcc_64_spine_branch_setup.parquet`."""
    stem = Path(filepath).stem  # e.g. dolt_tpcc_64_spine_branch_setup
    m = re.match(r"^(dolt|file_copy|neon)_tpcc_(\d+)_(spine|bushy|fan_out)_branch_setup$", stem)
    if not m:
        return None
    return {
        "backend": m.group(1),
        "N": int(m.group(2)),
        "topology": m.group(3),
    }


def load_all(data_dir: str) -> pd.DataFrame:
    """Load all *_setup.parquet files into a single DataFrame with metadata columns."""
    files = sorted(glob.glob(os.path.join(data_dir, "*_branch_setup.parquet")))
    dfs = []
    for f in files:
        meta = parse_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        # Assign repetition ID: iteration_number resets to 0 for each rep
        df["rep_id"] = (df["iteration_number"] == 0).cumsum() - 1
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No setup parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL CoW)", "neon": "Neon"}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
def section_overview(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 1: Data Overview")
    print("=" * 70)
    print()

    setup_files = df.groupby(["backend", "topology", "N"]).ngroups
    print(f"Total setup files loaded: {setup_files}")
    print(f"Total rows: {len(df)}")
    print()

    print(f"{'Backend':<30} {'Topology':<10} {'N values':>30} {'Rows':>8} {'Reps':>5}")
    print("-" * 85)
    for backend in BACKENDS:
        for topo in TOPO_ORDER:
            sub = df[(df["backend"] == backend) & (df["topology"] == topo)]
            if sub.empty:
                continue
            n_vals = sorted(sub["N"].unique())
            n_str = ",".join(str(n) for n in n_vals)
            reps = sub.groupby("N")["rep_id"].nunique().iloc[0]
            print(f"{BACKEND_LABELS[backend]:<30} {topo:<10} {n_str:>30} {len(sub):>8} {reps:>5}")
    print()


def section_mean_marginal_delta(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 2: Mean Marginal Storage Delta (bytes)")
    print("=" * 70)
    print()
    print("storage_delta = disk_size_after - disk_size_before, averaged across iterations and reps.")
    print()

    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = df[df["backend"] == backend]
        # Group by (topology, N), compute mean delta
        agg = sub.groupby(["topology", "N"])["storage_delta"].agg(["mean", "median", "std"]).reset_index()
        agg = agg.sort_values(["topology", "N"])

        # Pivot for nice table: rows=N, columns=topology
        n_vals = sorted(sub["N"].unique())
        print(f"{'N':>6}", end="")
        for topo in TOPO_ORDER:
            print(f"  {topo + ' (mean)':>20}  {topo + ' (std)':>18}", end="")
        print()
        print("-" * (6 + 3 * 42))

        for n in n_vals:
            print(f"{n:>6}", end="")
            for topo in TOPO_ORDER:
                row = agg[(agg["topology"] == topo) & (agg["N"] == n)]
                if row.empty:
                    print(f"  {'—':>20}  {'—':>18}", end="")
                else:
                    mean_val = row["mean"].iloc[0]
                    std_val = row["std"].iloc[0]
                    print(f"  {fmt_bytes(mean_val):>20}  {fmt_bytes(std_val):>18}", end="")
            print()
        print()


def section_cumulative_storage(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 3: Cumulative Storage at Max N per Backend×Topology")
    print("=" * 70)
    print()
    print("Total disk_size_after at the last branch creation, averaged across reps.")
    print()

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = df[df["backend"] == backend]

        print(f"{'N':>6}", end="")
        for topo in TOPO_ORDER:
            print(f"  {topo:>20}", end="")
        print()
        print("-" * (6 + 3 * 22))

        n_vals = sorted(sub["N"].unique())
        for n in n_vals:
            print(f"{n:>6}", end="")
            for topo in TOPO_ORDER:
                rows = sub[(sub["topology"] == topo) & (sub["N"] == n)]
                if rows.empty:
                    print(f"  {'—':>20}", end="")
                else:
                    # Last iteration per rep, then average
                    last_per_rep = rows.groupby("rep_id")["disk_size_after"].last()
                    avg_final = last_per_rep.mean()
                    print(f"  {fmt_bytes(avg_final):>20}", end="")
            print()
        print()


def section_topology_ratios(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 4: Topology Comparison Ratios")
    print("=" * 70)
    print()
    print("Ratio of mean marginal delta: spine/fan_out, bushy/fan_out")
    print()

    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = df[df["backend"] == backend]
        agg = sub.groupby(["topology", "N"])["storage_delta"].mean().reset_index()

        n_vals = sorted(sub["N"].unique())
        key_ns = [n for n in [1, 8, 64, 256, 512, 1024] if n in n_vals]

        print(f"{'N':>6}  {'spine/fan_out':>15}  {'bushy/fan_out':>15}  {'spine mean':>15}  {'fan_out mean':>15}")
        print("-" * 72)

        for n in key_ns:
            spine_val = agg[(agg["topology"] == "spine") & (agg["N"] == n)]["storage_delta"]
            fan_val = agg[(agg["topology"] == "fan_out") & (agg["N"] == n)]["storage_delta"]
            bushy_val = agg[(agg["topology"] == "bushy") & (agg["N"] == n)]["storage_delta"]

            if spine_val.empty or fan_val.empty:
                continue
            s = spine_val.iloc[0]
            f = fan_val.iloc[0]
            b = bushy_val.iloc[0] if not bushy_val.empty else float("nan")

            s_f = s / f if f != 0 else float("inf")
            b_f = b / f if f != 0 else float("inf")
            print(f"{n:>6}  {s_f:>15.3f}  {b_f:>15.3f}  {fmt_bytes(s):>15}  {fmt_bytes(f):>15}")
        print()


def section_latency(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 5: Branch Creation Latency Summary")
    print("=" * 70)
    print()

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = df[df["backend"] == backend]
        agg = sub.groupby(["topology", "N"])["latency"].agg(["mean", "std"]).reset_index()

        n_vals = sorted(sub["N"].unique())
        key_ns = [n for n in [1, 8, 64, 256, 512, 1024] if n in n_vals]

        print(f"{'N':>6}", end="")
        for topo in TOPO_ORDER:
            print(f"  {topo + ' (ms)':>15}", end="")
        print()
        print("-" * (6 + 3 * 17))

        for n in key_ns:
            print(f"{n:>6}", end="")
            for topo in TOPO_ORDER:
                row = agg[(agg["topology"] == topo) & (agg["N"] == n)]
                if row.empty:
                    print(f"  {'—':>15}", end="")
                else:
                    mean_ms = row["mean"].iloc[0] * 1000
                    print(f"  {mean_ms:>15.1f}", end="")
            print()
        print()


def section_research_questions(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 6: Research Question Answers")
    print("=" * 70)
    print()

    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
    agg = df.groupby(["backend", "topology", "N"])["storage_delta"].mean().reset_index()

    # RQ1: Does marginal cost differ across topologies?
    print("RQ1: Does the marginal storage cost of the nth branch differ across")
    print("     topologies for the same backend?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        max_n = sub["N"].max()
        at_max = sub[sub["N"] == max_n]
        print(f"  {BACKEND_LABELS[backend]} at N={max_n}:")
        for topo in TOPO_ORDER:
            val = at_max[at_max["topology"] == topo]["storage_delta"]
            if not val.empty:
                print(f"    {topo:<10}: {fmt_bytes(val.iloc[0])}")
    print()

    # RQ2: Do any backends exhibit constant marginal cost regardless of topology?
    print("RQ2: Do any backends exhibit constant marginal cost regardless of topology?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        # Check coefficient of variation across topologies at each N
        cv = sub.groupby("N")["storage_delta"].agg(lambda x: x.std() / x.mean() if x.mean() != 0 else 0)
        max_cv = cv.max()
        print(f"  {BACKEND_LABELS[backend]}: max CV across topologies = {max_cv:.3f}")
    print()

    # RQ3: Does fan-out produce lower or higher overhead than spine?
    print("RQ3: Does fan-out (shallow, wide) produce lower or higher overhead")
    print("     than spine (deep, narrow)?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        max_n = sub["N"].max()
        spine_at_max = sub[(sub["topology"] == "spine") & (sub["N"] == max_n)]["storage_delta"].iloc[0]
        fan_at_max = sub[(sub["topology"] == "fan_out") & (sub["N"] == max_n)]["storage_delta"].iloc[0]
        ratio = spine_at_max / fan_at_max if fan_at_max != 0 else float("inf")
        direction = "higher" if ratio > 1 else "lower"
        print(f"  {BACKEND_LABELS[backend]} at N={max_n}: spine is {ratio:.2f}x fan_out ({direction})")
        print(f"    spine = {fmt_bytes(spine_at_max)}, fan_out = {fmt_bytes(fan_at_max)}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 1 analysis")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    print(f"Loading data from: {data_dir}")
    print()

    df = load_all(data_dir)

    section_overview(df)
    section_mean_marginal_delta(df)
    section_cumulative_storage(df)
    section_topology_ratios(df)
    section_latency(df)
    section_research_questions(df)


if __name__ == "__main__":
    main()
