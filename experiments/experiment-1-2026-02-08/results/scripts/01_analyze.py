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
    m = re.match(r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_branch_setup$", stem)
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


BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy (PostgreSQL CoW)",
    "neon": "Neon",
    "xata": "Xata",
}
BACKENDS = ["dolt", "file_copy", "neon", "xata"]
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

    print(f"{'Backend':<30} {'Topology':<10} {'N values':>30} {'Rows':>8} {'Reps':>10}")
    print("-" * 90)
    for backend in BACKENDS:
        for topo in TOPO_ORDER:
            sub = df[(df["backend"] == backend) & (df["topology"] == topo)]
            if sub.empty:
                continue
            n_vals = sorted(sub["N"].unique())
            n_str = ",".join(str(n) for n in n_vals)
            reps_per_n = sub.groupby("N")["rep_id"].nunique()
            rep_min, rep_max = int(reps_per_n.min()), int(reps_per_n.max())
            reps_str = str(rep_min) if rep_min == rep_max else f"{rep_min}-{rep_max}"
            print(f"{BACKEND_LABELS[backend]:<30} {topo:<10} {n_str:>30} {len(sub):>8} {reps_str:>10}")
    print()


def section_mean_marginal_delta(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 2: Mean Marginal Storage Delta (bytes)")
    print("=" * 70)
    print()
    print("storage_delta = disk_size_after - disk_size_before, averaged across iterations and reps.")
    print()

    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
    # Exclude rows where Xata metrics API returned no data (disk_size == 0).
    # Other backends can legitimately have disk_size_before=0 (e.g. Dolt N=1).
    valid = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = valid[valid["backend"] == backend]
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

    # Exclude Xata rows with missing storage metrics (API lag)
    valid = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = valid[valid["backend"] == backend]

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
    # Exclude Xata rows with missing storage metrics (API lag)
    valid = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = valid[valid["backend"] == backend]
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
    # Exclude Xata rows with missing storage metrics (API lag)
    valid = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]
    agg = valid.groupby(["backend", "topology", "N"])["storage_delta"].mean().reset_index()

    def max_common_n(sub: pd.DataFrame, topologies: list[str]) -> int | None:
        common = None
        for topo in topologies:
            ns = set(sub[sub["topology"] == topo]["N"].unique())
            common = ns if common is None else (common & ns)
        if not common:
            return None
        return int(max(common))

    # RQ1: Does marginal cost differ across topologies?
    print("RQ1: Does the marginal storage cost of the nth branch differ across")
    print("     topologies for the same backend?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        if sub.empty:
            print(f"  {BACKEND_LABELS[backend]}: no rows")
            continue
        n = max_common_n(sub, TOPO_ORDER)
        if n is None:
            print(f"  {BACKEND_LABELS[backend]}: no common N across spine/bushy/fan_out")
            continue
        at_n = sub[sub["N"] == n]
        vals = {}
        print(f"  {BACKEND_LABELS[backend]} at common comparable N={n}:")
        for topo in TOPO_ORDER:
            series = at_n[at_n["topology"] == topo]["storage_delta"]
            if series.empty:
                continue
            vals[topo] = float(series.iloc[0])
            print(f"    {topo:<10}: {fmt_bytes(vals[topo])}")
        if len(vals) >= 2:
            spread = max(vals.values()) - min(vals.values())
            print(f"    spread     : {fmt_bytes(spread)}")
    print()

    # RQ2: Do any backends exhibit constant marginal cost regardless of topology?
    print("RQ2: Do any backends exhibit constant marginal cost regardless of topology?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        if sub.empty:
            print(f"  {BACKEND_LABELS[backend]}: no rows")
            continue
        n = max_common_n(sub, TOPO_ORDER)
        if n is None:
            print(f"  {BACKEND_LABELS[backend]}: no common N across all three topologies")
            continue
        at_n = sub[sub["N"] == n]
        vals = {}
        for topo in TOPO_ORDER:
            series = at_n[at_n["topology"] == topo]["storage_delta"]
            if not series.empty:
                vals[topo] = float(series.iloc[0])
        if len(vals) < 3:
            print(f"  {BACKEND_LABELS[backend]} at N={n}: incomplete topology set")
            continue
        spread = max(vals.values()) - min(vals.values())
        fan = vals["fan_out"]
        s_f = vals["spine"] / fan if fan != 0 else float("inf")
        b_f = vals["bushy"] / fan if fan != 0 else float("inf")
        print(f"  {BACKEND_LABELS[backend]} at common comparable N={n}:")
        print(f"    spread        = {fmt_bytes(spread)}")
        print(f"    spine/fan_out = {s_f:.2f}x")
        print(f"    bushy/fan_out = {b_f:.2f}x")
    print()

    # RQ3: Does fan-out produce lower or higher overhead than spine?
    print("RQ3: Does fan-out (shallow, wide) produce lower or higher overhead")
    print("     than spine (deep, narrow)?")
    print()
    for backend in BACKENDS:
        sub = agg[agg["backend"] == backend]
        if sub.empty:
            print(f"  {BACKEND_LABELS[backend]}: no rows")
            continue
        n = max_common_n(sub, ["spine", "fan_out"])
        if n is None:
            print(f"  {BACKEND_LABELS[backend]}: no common N for spine vs fan_out")
            continue
        at_n = sub[sub["N"] == n]
        spine_series = at_n[at_n["topology"] == "spine"]["storage_delta"]
        fan_series = at_n[at_n["topology"] == "fan_out"]["storage_delta"]
        if spine_series.empty or fan_series.empty:
            print(f"  {BACKEND_LABELS[backend]} at N={n}: missing spine/fan_out data")
            continue
        spine = float(spine_series.iloc[0])
        fan = float(fan_series.iloc[0])
        ratio = spine / fan if fan != 0 else float("inf")
        if fan == 0:
            direction = "undefined (fan_out = 0)"
        elif ratio > 1:
            direction = "higher"
        elif ratio < 1:
            direction = "lower"
        else:
            direction = "equal"
        print(
            f"  {BACKEND_LABELS[backend]} at common comparable N={n}: "
            f"spine is {ratio:.2f}x fan_out ({direction})"
        )
        print(f"    spine = {fmt_bytes(spine)}, fan_out = {fmt_bytes(fan)}")
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
