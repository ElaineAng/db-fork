#!/usr/bin/env python3
"""Compute all numerical metrics for the storage scaling report.

Reads parquet files from /tmp/run_stats/ and prints structured sections
that can be pasted into the report markdown.

Usage:
    python reports/scripts/storage_analysis.py [--base-dir /tmp/run_stats]
"""

import argparse
import glob
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BACKENDS = [
    ("dolt", "Dolt"),
    ("postgres", "PostgreSQL (CoW)"),
    ("neon", "Neon"),
    ("xata", "Xata"),
]


def extract_num_branches(filename: str) -> int:
    match = re.search(r"(\d+)_spine", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(f"Could not extract num_branches from: {filename}")


def load_all(base_dir: str) -> dict[str, pd.DataFrame]:
    """Load all parquet files per backend into a dict of DataFrames."""
    data = {}
    for prefix, _ in BACKENDS:
        files = glob.glob(os.path.join(base_dir, f"{prefix}_tpcc_nth_op_*_spine.parquet"))
        if not files:
            continue
        dfs = []
        for f in files:
            n = extract_num_branches(f)
            df = pd.read_parquet(f)
            df["num_branches"] = n
            dfs.append(df)
        data[prefix] = pd.concat(dfs, ignore_index=True)
    return data


def get_storage_series(df: pd.DataFrame) -> pd.DataFrame:
    """Return (num_branches, storage_bytes) from BRANCH ops, max per branch count."""
    branch_ops = df[df["op_type"] == 1].copy()
    if branch_ops.empty or "disk_size_after" not in branch_ops.columns:
        return pd.DataFrame()
    agg = branch_ops.groupby("num_branches").agg(
        storage_bytes=("disk_size_after", "max")
    ).reset_index()
    return agg[agg["storage_bytes"] > 0].sort_values("num_branches").reset_index(drop=True)


def fmt_bytes(b: float) -> str:
    """Human-readable byte string."""
    if b >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    elif b >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    elif b >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


def fit_power_law(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    """Fit y = a * x^b via log-log regression. Returns (a, b, r2)."""
    lx, ly = np.log(x), np.log(y)
    coeffs = np.polyfit(lx, ly, 1)
    b, log_a = coeffs
    a = np.exp(log_a)
    pred = b * lx + log_a
    ss_res = np.sum((ly - pred) ** 2)
    ss_tot = np.sum((ly - ly.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return a, b, r2


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------
def section_overview(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Data Overview")
    print("=" * 70)
    print()
    print(f"{'Backend':<22} {'Files':>6} {'Branch Range':>20} {'Total Rows':>12}")
    print("-" * 62)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            print(f"{label:<22} {'—':>6}")
            continue
        df = all_data[prefix]
        branches = sorted(df["num_branches"].unique())
        n_files = len(branches)
        br_range = f"{branches[0]}-{branches[-1]}"
        print(f"{label:<22} {n_files:>6} {br_range:>20} {len(df):>12}")
    print()


def section_power_law(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Power-Law Fit  (storage_MB = a * branches^b)")
    print("=" * 70)
    print()
    print(f"{'Backend':<22} {'a (MB)':>10} {'b (exponent)':>14} {'R²':>8} {'Interpretation':<30}")
    print("-" * 86)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        s = get_storage_series(all_data[prefix])
        if s.empty or len(s) < 2:
            continue
        x = s["num_branches"].values.astype(float)
        y = s["storage_bytes"].values / (1 << 20)  # MB
        a, b, r2 = fit_power_law(x, y)
        if b < 0.7:
            interp = "strongly sublinear"
        elif b < 0.9:
            interp = "sublinear"
        elif b < 1.1:
            interp = "~linear"
        else:
            interp = "superlinear"
        print(f"{label:<22} {a:>10.2f} {b:>14.3f} {r2:>8.4f} {interp:<30}")
    print()


def section_absolute_storage(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Absolute Storage at Key Branch Counts")
    print("=" * 70)
    print()
    # Collect common branch counts
    all_branches = set()
    storage_map = {}
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        s = get_storage_series(all_data[prefix])
        if s.empty:
            continue
        storage_map[label] = dict(zip(s["num_branches"], s["storage_bytes"]))
        all_branches.update(s["num_branches"].tolist())

    branch_list = sorted(all_branches)
    header = f"{'Branches':>10}"
    for _, label in BACKENDS:
        if label in storage_map:
            header += f"  {label:>22}"
    print(header)
    print("-" * len(header))

    for n in branch_list:
        row = f"{n:>10}"
        for _, label in BACKENDS:
            if label not in storage_map:
                continue
            val = storage_map[label].get(n)
            if val is not None:
                row += f"  {fmt_bytes(val):>22}"
            else:
                row += f"  {'—':>22}"
        print(row)
    print()


def section_per_branch_cost(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Per-Branch Storage Cost (total_storage / num_branches)")
    print("=" * 70)
    print()
    header = f"{'Branches':>10}"
    for _, label in BACKENDS:
        if _ in all_data:
            header += f"  {label:>22}"
    print(header)
    print("-" * len(header))

    all_branches = set()
    cost_map = {}
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        s = get_storage_series(all_data[prefix])
        if s.empty:
            continue
        costs = {}
        for _, r in s.iterrows():
            n = r["num_branches"]
            costs[n] = r["storage_bytes"] / n
        cost_map[label] = costs
        all_branches.update(costs.keys())

    for n in sorted(all_branches):
        row = f"{n:>10}"
        for prefix, label in BACKENDS:
            if label not in cost_map:
                continue
            val = cost_map[label].get(n)
            if val is not None:
                row += f"  {fmt_bytes(val):>22}"
            else:
                row += f"  {'—':>22}"
        print(row)
    print()


def section_branch_delta(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Marginal Branch Cost (Nth branch creation delta)")
    print("=" * 70)
    print()
    print("Average disk_size_after - disk_size_before for BRANCH operations:")
    print()
    print(f"{'Backend':<22} {'Branches':>10} {'Avg Delta':>15} {'Std Delta':>15} {'Count':>6}")
    print("-" * 70)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        df = all_data[prefix]
        branch_ops = df[(df["op_type"] == 1) & (df["disk_size_after"] > 0)].copy()
        if branch_ops.empty:
            continue
        branch_ops["delta"] = branch_ops["disk_size_after"] - branch_ops["disk_size_before"]
        for n in sorted(branch_ops["num_branches"].unique()):
            subset = branch_ops[branch_ops["num_branches"] == n]
            avg = subset["delta"].mean()
            std = subset["delta"].std() if len(subset) > 1 else 0
            print(f"{label:<22} {n:>10} {fmt_bytes(avg):>15} {fmt_bytes(std):>15} {len(subset):>6}")
    print()


def section_storage_amplification(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Storage Amplification Factor")
    print("=" * 70)
    print()
    print("Amplification = total_storage / (initial_db_size * num_branches)")
    print("  < 1.0: storage sharing (data deduplication)")
    print("  = 1.0: no sharing (each branch is a full copy)")
    print("  > 1.0: overhead beyond raw data")
    print()
    print(f"{'Backend':<22} {'Branches':>10} {'Total Storage':>15} {'Ideal (no share)':>18} {'Amplification':>15}")
    print("-" * 82)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        df = all_data[prefix]
        s = get_storage_series(df)
        if s.empty:
            continue
        # Get initial_db_size (from first branch op)
        branch_ops = df[df["op_type"] == 1]
        init_sizes = branch_ops["initial_db_size"].unique()
        init_db = max(init_sizes) if len(init_sizes) > 0 else 0
        if init_db == 0:
            # For dolt, use disk_size_after at n=1 as proxy
            s1 = s[s["num_branches"] == 1]
            if not s1.empty:
                init_db = s1["storage_bytes"].iloc[0]
            else:
                continue

        for _, r in s.iterrows():
            n = r["num_branches"]
            total = r["storage_bytes"]
            ideal = init_db * (n + 1)  # +1 for the main/base branch
            amp = total / ideal if ideal > 0 else float("inf")
            print(f"{label:<22} {n:>10} {fmt_bytes(total):>15} {fmt_bytes(ideal):>18} {amp:>15.3f}")
    print()


def section_branch_latency(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Branch Creation Latency")
    print("=" * 70)
    print()
    print(f"{'Backend':<22} {'Branches':>10} {'Mean (ms)':>12} {'Std (ms)':>12} {'Count':>6}")
    print("-" * 64)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        df = all_data[prefix]
        branch_ops = df[df["op_type"] == 1]
        for n in sorted(branch_ops["num_branches"].unique()):
            subset = branch_ops[branch_ops["num_branches"] == n]
            mean_ms = subset["latency"].mean() * 1000
            std_ms = subset["latency"].std() * 1000 if len(subset) > 1 else 0
            print(f"{label:<22} {n:>10} {mean_ms:>12.2f} {std_ms:>12.2f} {len(subset):>6}")
    print()


def section_branch_latency_vs_storage(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Branch Latency per MB of Data Created")
    print("=" * 70)
    print()
    print("Efficiency metric: how many ms does it cost to create 1 MB of branch data?")
    print()
    print(f"{'Backend':<22} {'Branches':>10} {'Latency (ms)':>14} {'Delta (MB)':>12} {'ms/MB':>10}")
    print("-" * 70)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        df = all_data[prefix]
        branch_ops = df[(df["op_type"] == 1) & (df["disk_size_after"] > 0)].copy()
        if branch_ops.empty:
            continue
        branch_ops["delta_mb"] = (branch_ops["disk_size_after"] - branch_ops["disk_size_before"]) / (1 << 20)

        for n in sorted(branch_ops["num_branches"].unique()):
            subset = branch_ops[branch_ops["num_branches"] == n]
            mean_ms = subset["latency"].mean() * 1000
            mean_delta_mb = subset["delta_mb"].mean()
            if mean_delta_mb > 0:
                ms_per_mb = mean_ms / mean_delta_mb
            else:
                ms_per_mb = float("inf")
            print(f"{label:<22} {n:>10} {mean_ms:>14.2f} {mean_delta_mb:>12.2f} {ms_per_mb:>10.2f}")
    print()


def section_doubling_ratio(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Storage Doubling Ratio (storage[2n] / storage[n])")
    print("=" * 70)
    print()
    print("Ratio = 2.0 means linear; < 2.0 = sublinear; > 2.0 = superlinear")
    print()
    print(f"{'Backend':<22} {'From':>6} {'To':>6} {'Ratio':>8}")
    print("-" * 44)
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        s = get_storage_series(all_data[prefix])
        if len(s) < 2:
            continue
        branches = s["num_branches"].values
        storage = s["storage_bytes"].values.astype(float)
        for i in range(len(branches) - 1):
            ratio = storage[i + 1] / storage[i]
            print(f"{label:<22} {branches[i]:>6} {branches[i+1]:>6} {ratio:>8.2f}")
    print()


def section_projected_storage(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Projected Storage at Higher Branch Counts")
    print("=" * 70)
    print()
    print("Extrapolation using power-law fit (use with caution for backends with few data points)")
    print()
    targets = [10, 100, 1000, 10000]
    print(f"{'Backend':<22}", end="")
    for t in targets:
        print(f"  {t:>8} branches", end="")
    print()
    print("-" * (22 + len(targets) * 18))

    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        s = get_storage_series(all_data[prefix])
        if s.empty or len(s) < 2:
            continue
        x = s["num_branches"].values.astype(float)
        y = s["storage_bytes"].values
        a, b, _ = fit_power_law(x, y)
        print(f"{label:<22}", end="")
        for t in targets:
            projected = a * (t ** b)
            print(f"  {fmt_bytes(projected):>16}", end="")
        print()
    print()


def section_summary_table(all_data: dict[str, pd.DataFrame]):
    print("=" * 70)
    print("SECTION: Summary Comparison Table")
    print("=" * 70)
    print()

    rows = []
    for prefix, label in BACKENDS:
        if prefix not in all_data:
            continue
        df = all_data[prefix]
        s = get_storage_series(df)
        if s.empty or len(s) < 2:
            continue

        x = s["num_branches"].values.astype(float)
        y_mb = s["storage_bytes"].values / (1 << 20)
        a, b, r2 = fit_power_law(x, y_mb)

        # Per-branch cost at max branches
        max_n = s["num_branches"].max()
        max_storage = s[s["num_branches"] == max_n]["storage_bytes"].iloc[0]
        per_branch_mb = max_storage / max_n / (1 << 20)

        # Branch latency
        branch_ops = df[df["op_type"] == 1]
        mean_latency_ms = branch_ops["latency"].mean() * 1000

        # Initial db size
        init_sizes = branch_ops["initial_db_size"].unique()
        init_db = max(init_sizes) if len(init_sizes) > 0 else 0

        rows.append({
            "Backend": label,
            "Exponent (b)": f"{b:.3f}",
            "Scaling": "sublinear" if b < 0.9 else ("~linear" if b < 1.1 else "superlinear"),
            "Cost @ max br": f"{per_branch_mb:.1f} MB/br (n={max_n})",
            "Avg Branch Latency": f"{mean_latency_ms:.1f} ms",
            "Max Tested": str(max_n),
            "R²": f"{r2:.4f}",
        })

    # Print as table
    if rows:
        keys = list(rows[0].keys())
        widths = {k: max(len(k), max(len(r[k]) for r in rows)) for k in keys}
        header = "  ".join(f"{k:<{widths[k]}}" for k in keys)
        print(header)
        print("-" * len(header))
        for r in rows:
            print("  ".join(f"{r[k]:<{widths[k]}}" for k in keys))
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", default="/tmp/run_stats")
    args = parser.parse_args()

    all_data = load_all(args.base_dir)
    if not all_data:
        print("ERROR: No data found.")
        return

    section_overview(all_data)
    section_power_law(all_data)
    section_absolute_storage(all_data)
    section_per_branch_cost(all_data)
    section_branch_delta(all_data)
    section_storage_amplification(all_data)
    section_branch_latency(all_data)
    section_branch_latency_vs_storage(all_data)
    section_doubling_ratio(all_data)
    section_projected_storage(all_data)
    section_summary_table(all_data)


if __name__ == "__main__":
    main()
