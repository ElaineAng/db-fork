#!/usr/bin/env python3
"""Compute evidence metrics for storage scaling behavior analysis.

Compares power-law vs linear-with-offset models and analyzes per-operation
branch deltas to explain *why* each backend scales the way it does.

Usage:
    python reports/scripts/scaling_evidence.py --base-dir /tmp/run_stats
"""

import argparse
import glob
import re
from pathlib import Path

import numpy as np
import pandas as pd

BACKENDS = [
    ("dolt", "Dolt"),
    ("postgres", "PostgreSQL (CoW)"),
    ("neon", "Neon"),
    ("xata", "Xata"),
]


def load_storage_series(base_dir: str, prefix: str) -> list[tuple[int, float]]:
    """Load (num_branches, max_storage_bytes) for each nth_op run."""
    files = glob.glob(f"{base_dir}/{prefix}_tpcc_nth_op_*_spine.parquet")
    rows = []
    for f in files:
        m = re.search(r"(\d+)_spine", Path(f).stem)
        n = int(m.group(1))
        df = pd.read_parquet(f)
        br = df[df["op_type"] == 1]
        if br.empty:
            continue
        mx = br["disk_size_after"].max()
        if mx > 0:
            rows.append((n, mx))
    return sorted(rows)


def load_branch_ops(base_dir: str, prefix: str) -> pd.DataFrame:
    """Load all BRANCH operations across all runs for a backend."""
    files = glob.glob(f"{base_dir}/{prefix}_tpcc_nth_op_*_spine.parquet")
    frames = []
    for f in files:
        m = re.search(r"(\d+)_spine", Path(f).stem)
        nth = int(m.group(1))
        df = pd.read_parquet(f)
        br = df[df["op_type"] == 1].copy()
        if br.empty:
            continue
        br["nth_op"] = nth
        br["delta"] = br["disk_size_after"] - br["disk_size_before"]
        frames.append(br)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["nth_op", "iteration_number"])


def fit_power_law(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    lx, ly = np.log(x), np.log(y)
    c = np.polyfit(lx, ly, 1)
    b, a = c[0], np.exp(c[1])
    pred = c[0] * lx + c[1]
    ss_res = np.sum((ly - pred) ** 2)
    ss_tot = np.sum((ly - np.mean(ly)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return a, b, r2


def fit_linear(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    """Fit y = C + m*x. Returns (C, m, R²)."""
    c = np.polyfit(x, y, 1)
    m, C = c[0], c[1]
    pred = m * x + C
    ss_res = np.sum((y - pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return C, m, r2


def section_model_comparison(base_dir: str):
    print("=" * 70)
    print("MODEL COMPARISON: Power-Law vs Linear-with-Offset")
    print("=" * 70)
    print()
    print(f"{'Backend':<22} {'PL a':>8} {'PL b':>8} {'PL R²':>8}   "
          f"{'Lin C(MB)':>10} {'Lin m(MB/br)':>12} {'Lin R²':>8}   {'Better':>10}")
    print("-" * 100)

    for prefix, label in BACKENDS:
        data = load_storage_series(base_dir, prefix)
        if not data:
            continue
        x = np.array([d[0] for d in data], dtype=float)
        y = np.array([d[1] for d in data], dtype=float) / (1 << 20)

        a, b, r2_pl = fit_power_law(x, y)
        C, m, r2_lin = fit_linear(x, y)
        better = "Linear" if r2_lin > r2_pl else "Power-Law"

        print(f"{label:<22} {a:>8.2f} {b:>8.3f} {r2_pl:>8.4f}   "
              f"{C:>10.2f} {m:>12.4f} {r2_lin:>8.4f}   {better:>10}")
    print()


def section_per_op_deltas(base_dir: str):
    print("=" * 70)
    print("PER-OPERATION BRANCH DELTAS")
    print("=" * 70)
    print()

    for prefix, label in BACKENDS:
        ops = load_branch_ops(base_dir, prefix)
        if ops.empty:
            print(f"{label}: no BRANCH ops found\n")
            continue

        # Filter to non-zero deltas for meaningful stats
        nonzero = ops[ops["delta"] > 0]
        all_deltas = ops["delta"].values

        print(f"--- {label} ---")
        print(f"  Total BRANCH ops:     {len(ops)}")
        print(f"  Non-zero delta ops:   {len(nonzero)}")

        if not nonzero.empty:
            init_db = nonzero["initial_db_size"].iloc[0]
            avg_delta = nonzero["delta"].mean()
            min_delta = nonzero["delta"].min()
            max_delta = nonzero["delta"].max()
            ratio = avg_delta / init_db if init_db > 0 else float("inf")

            if avg_delta < 1e6:
                unit, div = "KB", 1e3
            else:
                unit, div = "MB", 1e6

            print(f"  initial_db_size:      {init_db / 1e6:.3f} MB")
            print(f"  Avg delta:            {avg_delta / div:.3f} {unit}")
            print(f"  Min delta:            {min_delta / div:.3f} {unit}")
            print(f"  Max delta:            {max_delta / div:.3f} {unit}")
            print(f"  delta / initial_db:   {ratio:.4f}")

        # Show per-nth_op breakdown
        print(f"\n  Per-run breakdown:")
        print(f"  {'nth_op':>8} {'n_ops':>6} {'avg_delta':>12} {'delta/init':>12} {'max_storage':>14}")
        for nth, grp in ops.groupby("nth_op"):
            nz = grp[grp["delta"] > 0]
            avg_d = nz["delta"].mean() if not nz.empty else 0
            init = grp["initial_db_size"].iloc[0]
            r = avg_d / init if init > 0 else float("inf")
            mx = grp["disk_size_after"].max()

            if avg_d < 1e6:
                d_str = f"{avg_d / 1e3:.2f} KB"
            else:
                d_str = f"{avg_d / 1e6:.3f} MB"

            r_str = f"{r:.4f}" if r != float("inf") else "N/A"
            print(f"  {int(nth):>8} {len(grp):>6} {d_str:>12} {r_str:>12} {mx / 1e6:>12.2f} MB")
        print()


def section_marginal_cost(base_dir: str):
    print("=" * 70)
    print("MARGINAL COST BETWEEN RUNS (total_storage delta / branch_count delta)")
    print("=" * 70)
    print()

    for prefix, label in BACKENDS:
        data = load_storage_series(base_dir, prefix)
        if len(data) < 2:
            continue
        x = np.array([d[0] for d in data], dtype=float)
        y = np.array([d[1] for d in data], dtype=float) / (1 << 20)

        print(f"--- {label} ---")
        print(f"  {'from':>6} -> {'to':>6}   {'delta_storage':>14} {'delta_branches':>15} {'marginal MB/br':>15}")
        for i in range(1, len(data)):
            dn = x[i] - x[i - 1]
            ds = y[i] - y[i - 1]
            marginal = ds / dn if dn > 0 else 0
            print(f"  {int(x[i-1]):>6} -> {int(x[i]):>6}   {ds:>12.2f} MB {int(dn):>14} br   {marginal:>13.4f}")
        print()


def section_c_over_m(base_dir: str):
    print("=" * 70)
    print("BASE OVERHEAD / MARGINAL COST RATIO (explains power-law exponent)")
    print("=" * 70)
    print()
    print(f"{'Backend':<22} {'C (MB)':>10} {'m (MB/br)':>10} {'C/m':>8} {'PL b':>8} {'Interpretation'}")
    print("-" * 85)

    for prefix, label in BACKENDS:
        data = load_storage_series(base_dir, prefix)
        if not data:
            continue
        x = np.array([d[0] for d in data], dtype=float)
        y = np.array([d[1] for d in data], dtype=float) / (1 << 20)

        _, b, _ = fit_power_law(x, y)
        C, m, _ = fit_linear(x, y)
        ratio = abs(C) / m if m > 0 else 0

        if ratio < 0.5:
            interp = "Low C/m → PL exponent is accurate"
        elif ratio < 3:
            interp = "Moderate C/m → PL underestimates linearity"
        else:
            interp = "High C/m → PL exponent is artifact of offset"
        print(f"{label:<22} {C:>10.2f} {m:>10.2f} {ratio:>8.2f} {b:>8.3f} {interp}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Scaling evidence analysis.")
    parser.add_argument("--base-dir", default="/tmp/run_stats")
    args = parser.parse_args()

    section_model_comparison(args.base_dir)
    section_per_op_deltas(args.base_dir)
    section_marginal_cost(args.base_dir)
    section_c_over_m(args.base_dir)


if __name__ == "__main__":
    main()
