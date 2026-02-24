#!/usr/bin/env python3
"""Compute numerical metrics for Experiment 2 (Linux run).

Adapted from results/scripts/01_analyze.py — auto-detects available backends
instead of hardcoding [dolt, file_copy, neon].
"""

import argparse
import glob
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd


def fmt_bytes(b: float) -> str:
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    elif abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    elif abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


def parse_measurement_filename(filepath: str) -> dict | None:
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None
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


BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL FILE_COPY)"}
TOPO_ORDER = ["spine", "bushy", "fan_out"]
N_VALUES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]


def get_backends(df):
    return [b for b in ["dolt", "file_copy", "neon"] if b in df["backend"].unique()]


def section_overview(df, backends):
    print("=" * 70)
    print("SECTION 1: Data Overview")
    print("=" * 70)
    print()
    print(f"Total measurement rows: {len(df)}")
    print(f"Unique run_ids: {df['run_id'].nunique()}")
    print()

    exp2a = df[(df.operation == "UPDATE") | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))]
    print("Exp 2a (topology comparison, UPDATE + RANGE_UPDATE r=20):")
    summary = exp2a.groupby(["backend", "topology", "operation"]).agg(
        runs=("run_id", "nunique"), rows=("run_id", "count")
    ).reset_index()
    for _, row in summary.iterrows():
        print(f"  {row.backend:<12} {row.topology:<10} {row.operation:<15} "
              f"runs={row.runs:>3}  rows={row.rows:>5}")
    print()

    exp2b = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]
    print("Exp 2b (range size sweep, spine only):")
    summary = exp2b.groupby(["backend", "range_size"]).agg(
        runs=("run_id", "nunique"), rows=("run_id", "count")
    ).reset_index()
    for _, row in summary.iterrows():
        rs = int(row.range_size) if pd.notna(row.range_size) else "none"
        print(f"  {row.backend:<12} r={rs:<5} runs={row.runs:>3}  rows={row.rows:>5}")
    print()


def section_delta_distribution(df, backends):
    print("=" * 70)
    print("SECTION 2: Storage Delta Distribution")
    print("=" * 70)
    print()

    for backend in backends:
        sub = df[df.backend == backend]
        total = len(sub)
        if total == 0:
            continue
        nz = int((sub.storage_delta != 0).sum())
        label = BACKEND_LABELS.get(backend, backend)
        print(f"--- {label} ---")
        print(f"  Total operation rows: {total}")
        print(f"  Non-zero deltas: {nz} ({nz/total*100:.1f}%)")
        print(f"  Zero deltas: {total - nz} ({(total-nz)/total*100:.1f}%)")
        print()

        nz_vals = sub[sub.storage_delta != 0]["storage_delta"].value_counts().sort_index()
        if len(nz_vals) > 0:
            print("  Non-zero delta value distribution:")
            for val, cnt in nz_vals.items():
                pages = val / 8192 if backend == "file_copy" else None
                page_str = f"  ({pages:.0f} pages)" if pages and pages == int(pages) else ""
                print(f"    {fmt_bytes(val):>12}: {cnt:>4} occurrences{page_str}")
        print()


def section_update_delta_vs_N(df, backends):
    print("=" * 70)
    print("SECTION 3: Point UPDATE — Mean Storage Delta vs Branch Count")
    print("=" * 70)
    print()

    updates = df[df.operation == "UPDATE"]

    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        print(f"--- {label} ---")
        sub = updates[updates.backend == backend]

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


def section_range_update_delta_vs_N(df, backends):
    print("=" * 70)
    print("SECTION 4: RANGE_UPDATE (r=20) — Mean Storage Delta vs Branch Count")
    print("=" * 70)
    print()

    ru = df[(df.operation == "RANGE_UPDATE") & (df.range_size == 20)]

    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        print(f"--- {label} ---")
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


def section_range_size_sweep(df, backends):
    print("=" * 70)
    print("SECTION 5: RANGE_UPDATE — Per-Key Delta vs Range Size (spine only)")
    print("=" * 70)
    print()

    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]

    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        print(f"--- {label} ---")
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


def section_latency(df, backends):
    print("=" * 70)
    print("SECTION 6: Operation Latency Summary")
    print("=" * 70)
    print()

    for op in ["UPDATE", "RANGE_UPDATE"]:
        print(f"--- {op} ---")
        sub = df[df.operation == op]
        if op == "RANGE_UPDATE":
            sub = sub[sub.range_size == 20]

        for backend in backends:
            label = BACKEND_LABELS.get(backend, backend)
            bsub = sub[sub.backend == backend]
            print(f"  {label}:")
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


def section_research_questions(df, backends):
    print("=" * 70)
    print("SECTION 7: Research Question Answers")
    print("=" * 70)
    print()

    print("RQ1: Does per-operation storage overhead grow with the number of branches?")
    print()
    for op in ["UPDATE", "RANGE_UPDATE"]:
        sub = df[df.operation == op]
        if op == "RANGE_UPDATE":
            sub = sub[sub.range_size == 20]
        for backend in backends:
            label = BACKEND_LABELS.get(backend, backend)
            bsub = sub[sub.backend == backend]
            if bsub.empty:
                continue
            agg = bsub.groupby("N")["storage_delta"].mean()
            n1 = agg.get(1, 0)
            n1024 = agg.get(1024, 0)
            print(f"  {label} {op}:")
            print(f"    N=1: mean delta = {fmt_bytes(n1)}")
            print(f"    N=1024: mean delta = {fmt_bytes(n1024)}")
            nz_by_n = bsub.groupby("N").apply(
                lambda g: (g.storage_delta != 0).mean() * 100, include_groups=False
            )
            print(f"    Non-zero fraction range: {nz_by_n.min():.1f}% — {nz_by_n.max():.1f}%")
        print()

    print("RQ2: Is the growth rate backend-dependent or topology-dependent?")
    print()
    updates = df[df.operation == "UPDATE"]
    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        bsub = updates[updates.backend == backend]
        if bsub.empty:
            continue
        agg = bsub.groupby(["topology", "N"])["storage_delta"].mean().reset_index()
        max_n = agg.N.max()
        at_max = agg[agg.N == max_n]
        print(f"  {label} at N={max_n}:")
        for topo in TOPO_ORDER:
            val = at_max[at_max.topology == topo]["storage_delta"]
            if not val.empty:
                print(f"    {topo:<10}: {fmt_bytes(val.iloc[0])}")
    print()

    print("RQ3: Is per-key storage overhead constant across range sizes?")
    print()
    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")]
    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        bsub = ru_spine[ru_spine.backend == backend]
        if bsub.empty:
            continue
        bsub = bsub.copy()
        bsub["per_key_delta"] = bsub["storage_delta"] / bsub["num_keys_touched"]
        agg = bsub.groupby("range_size")["per_key_delta"].mean()
        print(f"  {label} (spine, all N values averaged):")
        for rs, val in agg.items():
            print(f"    r={int(rs):>3}: per-key delta = {fmt_bytes(val)}")
    print()


def section_absolute_storage(df, backends):
    """Show absolute storage sizes at key branch counts."""
    print("=" * 70)
    print("SECTION 8: Absolute Storage Size (disk_size_before at run start)")
    print("=" * 70)
    print()

    updates = df[df.operation == "UPDATE"]
    for backend in backends:
        label = BACKEND_LABELS.get(backend, backend)
        sub = updates[(updates.backend == backend) & (updates.topology == "spine")]
        print(f"--- {label} (spine, UPDATE) ---")
        print(f"{'N':>6}  {'first disk_size_before':>25}")
        for n in N_VALUES:
            tsub = sub[sub.N == n]
            if tsub.empty:
                print(f"{n:>6}  {'—':>25}")
            else:
                first_before = tsub["disk_size_before"].iloc[0]
                print(f"{n:>6}  {fmt_bytes(first_before):>25}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Experiment 2 analysis (Linux)")
    parser.add_argument("--data-dir", default="/tmp/run_stats")
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    print(f"Loading data from: {data_dir}")
    print()

    df = load_all(data_dir)
    backends = get_backends(df)
    print(f"Detected backends: {backends}")
    print()

    section_overview(df, backends)
    section_delta_distribution(df, backends)
    section_update_delta_vs_N(df, backends)
    section_range_update_delta_vs_N(df, backends)
    section_range_size_sweep(df, backends)
    section_latency(df, backends)
    section_research_questions(df, backends)
    section_absolute_storage(df, backends)


if __name__ == "__main__":
    main()
