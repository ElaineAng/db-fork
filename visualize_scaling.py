#!/usr/bin/env python3
"""Visualize storage scaling analysis: power-law fit and per-branch marginal cost.

Usage:
    python visualize_scaling.py --base-dir /tmp/run_stats -o figures/
"""

import argparse
import glob
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from viz_common import save_or_show

# Backend display config: (glob_prefix, label, color)
BACKENDS = [
    ("dolt", "Dolt", "#1f77b4"),
    ("postgres", "PostgreSQL (CoW)", "#2ca02c"),
    ("neon", "Neon", "#ff7f0e"),
    ("xata", "Xata", "#d62728"),
]


def extract_num_branches(filename: str) -> int:
    match = re.search(r"(\d+)_spine", Path(filename).stem)
    if match:
        return int(match.group(1))
    match = re.search(r"(\d+)", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(f"Could not extract num_branches from filename: {filename}")


def load_backend_storage(base_dir: str, backend_prefix: str) -> pd.DataFrame:
    """Load all parquet files for a backend and return (num_branches, storage_bytes)."""
    pattern = os.path.join(base_dir, f"{backend_prefix}_tpcc_nth_op_*_spine.parquet")
    files = glob.glob(pattern)
    if not files:
        return pd.DataFrame()

    rows = []
    for filepath in files:
        num_branches = extract_num_branches(filepath)
        df = pd.read_parquet(filepath)
        if "disk_size_after" not in df.columns:
            continue
        # Use BRANCH ops (op_type=1) for storage measurement
        branch_rows = df[df["op_type"] == 1]
        if branch_rows.empty:
            continue
        max_storage = branch_rows["disk_size_after"].max()
        if max_storage > 0:
            rows.append({"num_branches": num_branches, "storage_bytes": max_storage})

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("num_branches").reset_index(drop=True)


def fit_power_law(branches: np.ndarray, storage: np.ndarray) -> tuple[float, float, float]:
    """Fit storage = a * branches^b via log-log linear regression.

    Returns (a, b, r_squared).
    """
    log_x = np.log(branches)
    log_y = np.log(storage)
    coeffs = np.polyfit(log_x, log_y, 1)
    b = coeffs[0]
    log_a = coeffs[1]
    a = np.exp(log_a)

    # R-squared
    y_pred = coeffs[0] * log_x + coeffs[1]
    ss_res = np.sum((log_y - y_pred) ** 2)
    ss_tot = np.sum((log_y - np.mean(log_y)) ** 2)
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return a, b, r_squared


def plot_power_law_fit(all_data: dict, output_path: str | None = None):
    """Plot storage vs branches with power-law fit lines and exponents."""
    fig, ax = plt.subplots(figsize=(12, 8))

    fit_results = {}

    for backend_prefix, label, color in BACKENDS:
        if backend_prefix not in all_data or all_data[backend_prefix].empty:
            continue
        df = all_data[backend_prefix]
        branches = df["num_branches"].values.astype(float)
        storage_mb = df["storage_bytes"].values / (1 << 20)

        a, b, r2 = fit_power_law(branches, storage_mb)
        fit_results[label] = (a, b, r2)

        # Plot actual data
        ax.scatter(branches, storage_mb, color=color, s=60, zorder=5)

        # Plot fit line
        x_fit = np.logspace(np.log2(branches.min()), np.log2(branches.max()), 100, base=2)
        y_fit = a * x_fit ** b
        ax.plot(
            x_fit, y_fit,
            color=color, linestyle="--", alpha=0.7,
            label=f"{label}: $s = {a:.1f} \\cdot n^{{{b:.2f}}}$  ($R^2$={r2:.3f})",
        )

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Number of Branches", fontsize=12)
    ax.set_ylabel("Total Storage (MB)", fontsize=12)
    ax.set_title("Storage Scaling: Power-Law Fit per Backend", fontsize=14)
    ax.legend(fontsize=11, loc="upper left")
    ax.grid(True, alpha=0.3)
    ax.grid(True, which="minor", alpha=0.1)
    fig.tight_layout()
    save_or_show(fig, output_path)

    return fit_results


def plot_per_branch_cost(all_data: dict, output_path: str | None = None):
    """Plot per-branch marginal storage cost vs number of branches."""
    fig, ax = plt.subplots(figsize=(12, 8))

    for backend_prefix, label, color in BACKENDS:
        if backend_prefix not in all_data or all_data[backend_prefix].empty:
            continue
        df = all_data[backend_prefix].sort_values("num_branches")
        branches = df["num_branches"].values.astype(float)
        storage_mb = df["storage_bytes"].values / (1 << 20)

        cost_per_branch = storage_mb / branches

        ax.plot(
            branches, cost_per_branch,
            marker="o", color=color, label=label, linewidth=2,
        )

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Number of Branches", fontsize=12)
    ax.set_ylabel("Storage per Branch (MB/branch)", fontsize=12)
    ax.set_title("Per-Branch Storage Cost vs Number of Branches", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.grid(True, which="minor", alpha=0.1)
    fig.tight_layout()
    save_or_show(fig, output_path)


def plot_doubling_ratio(all_data: dict, output_path: str | None = None):
    """Plot storage doubling ratio: storage(2n) / storage(n) at each step."""
    fig, ax = plt.subplots(figsize=(12, 8))

    ax.axhline(y=2.0, color="gray", linestyle=":", alpha=0.5, label="Linear scaling (ratio=2)")

    for backend_prefix, label, color in BACKENDS:
        if backend_prefix not in all_data or all_data[backend_prefix].empty:
            continue
        df = all_data[backend_prefix].sort_values("num_branches")
        branches = df["num_branches"].values
        storage = df["storage_bytes"].values.astype(float)

        if len(branches) < 2:
            continue

        ratios = storage[1:] / storage[:-1]
        midpoints = branches[1:]  # x-axis: the larger branch count

        ax.plot(
            midpoints, ratios,
            marker="o", color=color, label=label, linewidth=2,
        )

    ax.set_xscale("log", base=2)
    ax.set_xlabel("Number of Branches", fontsize=12)
    ax.set_ylabel("Storage Ratio (storage[2n] / storage[n])", fontsize=12)
    ax.set_title("Storage Doubling Ratio per Backend", fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.grid(True, which="minor", alpha=0.1)
    fig.tight_layout()
    save_or_show(fig, output_path)


def main():
    parser = argparse.ArgumentParser(description="Storage scaling analysis.")
    parser.add_argument(
        "--base-dir", type=str, default="/tmp/run_stats",
        help="Directory containing parquet files.",
    )
    parser.add_argument(
        "-o", "--output-dir", type=str, default=".",
        help="Output directory for figures.",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Load data for all backends
    all_data = {}
    for backend_prefix, label, _ in BACKENDS:
        df = load_backend_storage(args.base_dir, backend_prefix)
        if not df.empty:
            all_data[backend_prefix] = df
            print(f"{label}: {len(df)} data points, branches {df['num_branches'].min()}-{df['num_branches'].max()}")
        else:
            print(f"{label}: no storage data found")

    # Plot 1: Power-law fit
    fit_results = plot_power_law_fit(
        all_data,
        os.path.join(args.output_dir, "storage_power_law_fit.png"),
    )

    # Print fit summary
    print("\n=== Power-Law Fit: storage(MB) = a * branches^b ===")
    print(f"{'Backend':<25} {'a':>10} {'b (exponent)':>15} {'R^2':>10}")
    print("-" * 62)
    for label, (a, b, r2) in fit_results.items():
        print(f"{label:<25} {a:>10.2f} {b:>15.3f} {r2:>10.4f}")

    # Plot 2: Per-branch cost
    plot_per_branch_cost(
        all_data,
        os.path.join(args.output_dir, "storage_per_branch_cost.png"),
    )

    # Plot 3: Doubling ratio
    plot_doubling_ratio(
        all_data,
        os.path.join(args.output_dir, "storage_doubling_ratio.png"),
    )


if __name__ == "__main__":
    main()
