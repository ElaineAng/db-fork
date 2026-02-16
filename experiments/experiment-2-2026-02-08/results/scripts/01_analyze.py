#!/usr/bin/env python3
"""Compute numerical metrics for Experiment 2: Per-Operation Storage Overhead.

Loads all measurement parquet files (non-setup) from the data directory,
computes storage deltas, and prints structured sections for the report.

Covers:
  Exp 2a: UPDATE + RANGE_UPDATE(r=20) across topologies
  Exp 2b: RANGE_UPDATE with varying range_size (spine only)

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


def parse_measurement_filename(filepath: str) -> dict | None:
    """Extract metadata from measurement filenames like:
      dolt_tpcc_64_spine_update.parquet
      file_copy_tpcc_256_fan_out_range_update_r20.parquet
      dolt_tpcc_128_bushy_range_update_r50.parquet
    Excludes *_setup.parquet files.
    """
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None

    # Match: backend_tpcc_N_topology_operation[_rRANGE]
    m = re.match(
        r"^(dolt|file_copy|neon)_tpcc_(\d+)_(spine|bushy|fan_out)_(update|range_update)(?:_r(\d+))?$",
        stem,
    )
    if not m:
        return None
    return {
        "backend": m.group(1),
        "N": int(m.group(2)),
        "topology": m.group(3),
        "operation": m.group(4).upper(),
        "range_size": int(m.group(5)) if m.group(5) else None,
    }


def load_all(data_dir: str) -> pd.DataFrame:
    """Load all measurement (non-setup) parquet files into a single DataFrame."""
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []
    for f in files:
        meta = parse_measurement_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        df["operation"] = meta["operation"]
        df["range_size"] = meta["range_size"]
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No measurement parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL CoW)", "neon": "Neon"}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
N_VALUES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
def section_overview(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 1: Data Overview")
    print("=" * 70)
    print()

    print(f"Total measurement rows: {len(df)}")
    print(f"Unique run_ids: {df['run_id'].nunique()}")
    print()

    # Exp 2a: UPDATE + RANGE_UPDATE(r=20), all topologies
    exp2a = df[(df.operation == "UPDATE") | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))]
    print("Exp 2a (topology comparison, UPDATE + RANGE_UPDATE r=20):")
    summary = exp2a.groupby(["backend", "topology", "operation"]).agg(
        runs=("run_id", "nunique"), rows=("run_id", "count")
    ).reset_index()
    for _, row in summary.iterrows():
        print(f"  {row.backend:<12} {row.topology:<10} {row.operation:<15} "
              f"runs={row.runs:>3}  rows={row.rows:>5}")
    print()

    # Exp 2b: RANGE_UPDATE varying range_size, spine only
    exp2b = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]
    print("Exp 2b (range size sweep, spine only):")
    summary = exp2b.groupby(["backend", "range_size"]).agg(
        runs=("run_id", "nunique"), rows=("run_id", "count")
    ).reset_index()
    for _, row in summary.iterrows():
        rs = int(row.range_size) if pd.notna(row.range_size) else "none"
        print(f"  {row.backend:<12} r={rs:<5} runs={row.runs:>3}  rows={row.rows:>5}")
    print()


def section_delta_distribution(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 2: Storage Delta Distribution")
    print("=" * 70)
    print()

    for backend in BACKENDS:
        sub = df[df.backend == backend]
        total = len(sub)
        nz = int((sub.storage_delta != 0).sum())
        print(f"--- {BACKEND_LABELS[backend]} ---")
        print(f"  Total operation rows: {total}")
        print(f"  Non-zero deltas: {nz} ({nz/total*100:.1f}%)")
        print(f"  Zero deltas: {total - nz} ({(total-nz)/total*100:.1f}%)")
        print()

        # Unique non-zero delta values
        nz_vals = sub[sub.storage_delta != 0]["storage_delta"].value_counts().sort_index()
        if len(nz_vals) > 0:
            print("  Non-zero delta value distribution:")
            for val, cnt in nz_vals.items():
                pages = val / 8192 if backend == "file_copy" else None
                page_str = f"  ({pages:.0f} pages)" if pages and pages == int(pages) else ""
                print(f"    {fmt_bytes(val):>12}: {cnt:>4} occurrences{page_str}")
        print()


def section_update_delta_vs_N(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 3: Point UPDATE — Mean Storage Delta vs Branch Count")
    print("=" * 70)
    print()

    updates = df[df.operation == "UPDATE"]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = updates[updates.backend == backend]

        print(f"{'N':>6}", end="")
        for topo in TOPO_ORDER:
            print(f"  {'mean':>10}  {'p50':>10}  {'p99':>10}  {'nz%':>6}", end="")
        print()
        header = "  " + "  ".join([f"--- {t} ---" + " " * (22 - len(t)) for t in TOPO_ORDER])
        print(f"{'':>6}{header}")

        for n in N_VALUES:
            print(f"{n:>6}", end="")
            for topo in TOPO_ORDER:
                tsub = sub[(sub.topology == topo) & (sub.N == n)]
                if tsub.empty:
                    print(f"  {'—':>10}  {'—':>10}  {'—':>10}  {'—':>6}", end="")
                else:
                    mean_d = tsub.storage_delta.mean()
                    p50 = tsub.storage_delta.median()
                    p99 = tsub.storage_delta.quantile(0.99)
                    nz_pct = (tsub.storage_delta != 0).mean() * 100
                    print(f"  {fmt_bytes(mean_d):>10}  {fmt_bytes(p50):>10}  {fmt_bytes(p99):>10}  {nz_pct:>5.1f}%", end="")
            print()
        print()


def section_range_update_delta_vs_N(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 4: RANGE_UPDATE (r=20) — Mean Storage Delta vs Branch Count")
    print("=" * 70)
    print()

    ru = df[(df.operation == "RANGE_UPDATE") & (df.range_size == 20)]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = ru[ru.backend == backend]

        print(f"{'N':>6}", end="")
        for topo in TOPO_ORDER:
            print(f"  {'mean':>10}  {'p50':>10}  {'p99':>10}  {'nz%':>6}", end="")
        print()

        for n in N_VALUES:
            print(f"{n:>6}", end="")
            for topo in TOPO_ORDER:
                tsub = sub[(sub.topology == topo) & (sub.N == n)]
                if tsub.empty:
                    print(f"  {'—':>10}  {'—':>10}  {'—':>10}  {'—':>6}", end="")
                else:
                    mean_d = tsub.storage_delta.mean()
                    p50 = tsub.storage_delta.median()
                    p99 = tsub.storage_delta.quantile(0.99)
                    nz_pct = (tsub.storage_delta != 0).mean() * 100
                    print(f"  {fmt_bytes(mean_d):>10}  {fmt_bytes(p50):>10}  {fmt_bytes(p99):>10}  {nz_pct:>5.1f}%", end="")
            print()
        print()


def section_range_size_sweep(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 5: RANGE_UPDATE — Per-Key Delta vs Range Size (spine only)")
    print("=" * 70)
    print()

    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]

    for backend in BACKENDS:
        print(f"--- {BACKEND_LABELS[backend]} ---")
        sub = ru_spine[ru_spine.backend == backend]

        range_sizes = sorted(sub.range_size.dropna().unique())
        print(f"{'N':>6}", end="")
        for rs in range_sizes:
            print(f"  {'r=' + str(int(rs)):>10}", end="")
        print()

        for n in N_VALUES:
            print(f"{n:>6}", end="")
            for rs in range_sizes:
                tsub = sub[(sub.N == n) & (sub.range_size == rs)]
                if tsub.empty:
                    print(f"  {'—':>10}", end="")
                else:
                    mean_d = tsub.storage_delta.mean()
                    print(f"  {fmt_bytes(mean_d):>10}", end="")
            print()
        print()

        # Per-key normalization
        print("  Per-key delta (mean delta / num_keys_touched):")
        print(f"  {'N':>6}", end="")
        for rs in range_sizes:
            print(f"  {'r=' + str(int(rs)):>10}", end="")
        print()

        for n in N_VALUES:
            print(f"  {n:>6}", end="")
            for rs in range_sizes:
                tsub = sub[(sub.N == n) & (sub.range_size == rs)]
                if tsub.empty:
                    print(f"  {'—':>10}", end="")
                else:
                    per_key = tsub.storage_delta / tsub.num_keys_touched
                    mean_pk = per_key.mean()
                    print(f"  {fmt_bytes(mean_pk):>10}", end="")
            print()
        print()


def section_latency(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 6: Operation Latency Summary")
    print("=" * 70)
    print()

    for op in ["UPDATE", "RANGE_UPDATE"]:
        print(f"--- {op} ---")
        sub = df[df.operation == op]
        if op == "RANGE_UPDATE":
            sub = sub[sub.range_size == 20]  # default range size for comparison

        for backend in BACKENDS:
            bsub = sub[sub.backend == backend]
            print(f"  {BACKEND_LABELS[backend]}:")
            key_ns = [1, 8, 64, 256, 1024]
            print(f"  {'N':>6}", end="")
            for topo in TOPO_ORDER:
                print(f"  {topo + ' (ms)':>15}", end="")
            print()

            for n in key_ns:
                print(f"  {n:>6}", end="")
                for topo in TOPO_ORDER:
                    tsub = bsub[(bsub.topology == topo) & (bsub.N == n)]
                    if tsub.empty:
                        print(f"  {'—':>15}", end="")
                    else:
                        mean_ms = tsub.latency.mean() * 1000
                        print(f"  {mean_ms:>15.2f}", end="")
                print()
            print()


def section_research_questions(df: pd.DataFrame):
    print("=" * 70)
    print("SECTION 7: Research Question Answers")
    print("=" * 70)
    print()

    # RQ1: Does per-operation storage overhead grow with branch count?
    print("RQ1: Does per-operation storage overhead grow with the number of branches?")
    print()
    for op in ["UPDATE", "RANGE_UPDATE"]:
        sub = df[df.operation == op]
        if op == "RANGE_UPDATE":
            sub = sub[sub.range_size == 20]
        for backend in BACKENDS:
            bsub = sub[sub.backend == backend]
            agg = bsub.groupby("N")["storage_delta"].mean()
            n1 = agg.get(1, 0)
            n1024 = agg.get(1024, 0)
            print(f"  {BACKEND_LABELS[backend]} {op}:")
            print(f"    N=1: mean delta = {fmt_bytes(n1)}")
            print(f"    N=1024: mean delta = {fmt_bytes(n1024)}")
            nz_by_n = bsub.groupby("N").apply(
                lambda g: (g.storage_delta != 0).mean() * 100, include_groups=False
            )
            print(f"    Non-zero fraction range: {nz_by_n.min():.1f}% — {nz_by_n.max():.1f}%")
        print()

    # RQ2: Is the growth rate backend-dependent or topology-dependent?
    print("RQ2: Is the growth rate backend-dependent or topology-dependent?")
    print()
    updates = df[df.operation == "UPDATE"]
    for backend in BACKENDS:
        bsub = updates[updates.backend == backend]
        agg = bsub.groupby(["topology", "N"])["storage_delta"].mean().reset_index()
        max_n = agg.N.max()
        at_max = agg[agg.N == max_n]
        print(f"  {BACKEND_LABELS[backend]} at N={max_n}:")
        for topo in TOPO_ORDER:
            val = at_max[at_max.topology == topo]["storage_delta"]
            if not val.empty:
                print(f"    {topo:<10}: {fmt_bytes(val.iloc[0])}")
    print()

    # RQ3: Is per-key overhead constant across range sizes?
    print("RQ3: Is per-key storage overhead constant across range sizes?")
    print()
    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]
    for backend in BACKENDS:
        bsub = ru_spine[ru_spine.backend == backend]
        bsub = bsub.copy()
        bsub["per_key_delta"] = bsub["storage_delta"] / bsub["num_keys_touched"]
        agg = bsub.groupby("range_size")["per_key_delta"].mean()
        print(f"  {BACKEND_LABELS[backend]} (spine, all N values averaged):")
        for rs, val in agg.items():
            print(f"    r={int(rs):>3}: per-key delta = {fmt_bytes(val)}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 2 analysis")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    print(f"Loading data from: {data_dir}")
    print()

    df = load_all(data_dir)

    section_overview(df)
    section_delta_distribution(df)
    section_update_delta_vs_N(df)
    section_range_update_delta_vs_N(df)
    section_range_size_sweep(df)
    section_latency(df)
    section_research_questions(df)


if __name__ == "__main__":
    main()
