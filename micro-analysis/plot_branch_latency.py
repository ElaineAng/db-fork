#!/usr/bin/env python3
"""
Plot branch creation latency across different backends and branch counts.

Usage:
    python micro-analysis/plot_branch_latency.py --data-dir final/micro/branch --outdir figures/
"""

import argparse
import glob
import os
import re
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Operation type enum (from dblib/result.proto)
OP_TYPE_BRANCH_CREATE = 1
OP_TYPE_BRANCH_CONNECT = 2
OP_TYPE_READ = 3

# Backend colors - highly differentiable palette
BACKEND_COLORS = {
    "dolt": "#0072B2",      # Strong Blue
    "neon": "#D55E00",      # Vermillion/Orange
    "kpg": "#009E73",       # Teal/Green
    "xata": "#CC79A7",      # Pink/Magenta
    "file_copy": "#F0E442",  # Yellow
    "txn": "#E69F00",       # Gold/Orange
    "tiger": "#56B4E9",     # Sky Blue
}


def parse_filename(filename):
    """Extract backend and branch_count from filename.

    Example: dolt_tpcc_16_spine_branch.parquet -> ('dolt', 16)
    """
    basename = os.path.basename(filename)
    # Pattern: {backend}_{schema}_{branches}_spine_branch.parquet
    match = re.match(r'([a-z_]+)_\w+_(\d+)_spine_branch\.parquet$', basename)
    if match:
        backend = match.group(1)
        branches = int(match.group(2))
        return backend, branches
    return None, None


def load_branch_data(data_dir):
    """Load all branch creation parquet files.

    Returns:
        dict: {backend: {branch_count: DataFrame}}
    """
    data = defaultdict(dict)

    pattern = os.path.join(data_dir, "*_spine_branch.parquet")
    files = glob.glob(pattern)

    for filepath in files:
        backend, branches = parse_filename(filepath)
        if backend is None:
            print(f"  Warning: Could not parse filename: {os.path.basename(filepath)}")
            continue

        # Read parquet file
        df = pq.read_table(filepath).to_pandas()

        # Filter to only BRANCH_CREATE operations (op_type=1)
        # Note: The file also contains CONNECT operations
        df_branch = df[df['op_type'] == OP_TYPE_BRANCH_CREATE].copy()

        if len(df_branch) == 0:
            print(f"  Warning: No BRANCH operations in {os.path.basename(filepath)}")
            continue

        data[backend][branches] = df_branch
        print(f"  Loaded {backend:10s} branches={branches:4d}: {len(df_branch):3d} BRANCH ops")

    return data


def plot_latency_vs_branches(data, outdir):
    """Plot branch creation latency vs number of existing branches.

    Shows median latency with error bars (25th-75th percentile).
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    for backend in sorted(data.keys()):
        branch_counts = sorted(data[backend].keys())
        medians = []
        p25s = []
        p75s = []

        for bc in branch_counts:
            df = data[backend][bc]
            latencies = df['latency'].values * 1000  # Convert to milliseconds

            medians.append(np.median(latencies))
            p25s.append(np.percentile(latencies, 25))
            p75s.append(np.percentile(latencies, 75))

        # Convert to arrays
        branch_counts = np.array(branch_counts)
        medians = np.array(medians)
        p25s = np.array(p25s)
        p75s = np.array(p75s)

        # Plot line with error bars
        color = BACKEND_COLORS.get(backend, "#000000")
        ax.plot(
            branch_counts,
            medians,
            marker='o',
            markersize=5,
            linewidth=1.5,
            label=backend.upper(),
            color=color,
            alpha=0.9,
        )

    ax.set_xlabel("Number of Existing Branches", fontsize=12)
    ax.set_ylabel("Branch Creation Latency (ms)", fontsize=12)
    ax.set_title("Branch Creation Latency vs Existing Branches", fontsize=14, fontweight="bold")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)

    # Format tick labels to show actual values instead of powers
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x)}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: f'{int(y)}' if y >= 1 else f'{y:.2g}'))

    ax.grid(True, alpha=0.25, which="major", linewidth=0.8)
    ax.grid(True, alpha=0.15, which="minor", linewidth=0.4)
    ax.legend(fontsize=10, framealpha=0.95, edgecolor='gray', fancybox=True)

    fig.tight_layout()
    path = os.path.join(outdir, "branch_latency_vs_count.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def plot_latency_boxplots(data, outdir):
    """Create box plots showing latency distribution for each backend."""
    backends = sorted(data.keys())

    # Group branch counts (e.g., 1-10, 11-100, 101-1000)
    groups = [
        ("1-10", range(1, 11)),
        ("11-100", range(11, 101)),
        ("101-1000", range(101, 1001)),
    ]

    fig, axes = plt.subplots(1, len(groups), figsize=(5 * len(groups), 6), squeeze=False)
    axes_flat = axes.flatten()

    for group_idx, (group_label, group_range) in enumerate(groups):
        ax = axes_flat[group_idx]

        # Collect data for this group
        plot_data = []
        plot_labels = []
        plot_colors = []

        for backend in backends:
            # Find branch counts in this group
            branch_counts = sorted([bc for bc in data[backend].keys() if bc in group_range])

            if not branch_counts:
                continue

            # Combine all latencies from this group
            latencies = []
            for bc in branch_counts:
                latencies.extend(data[backend][bc]['latency'].values)

            if latencies:
                plot_data.append(latencies)
                plot_labels.append(backend.upper())
                plot_colors.append(BACKEND_COLORS.get(backend, "#000000"))

        if plot_data:
            bp = ax.boxplot(
                plot_data,
                labels=plot_labels,
                patch_artist=True,
                showfliers=False,  # Hide outliers for clarity
            )

            # Color the boxes
            for patch, color in zip(bp['boxes'], plot_colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.6)

        ax.set_ylabel("Latency (s)", fontsize=11)
        ax.set_title(f"{group_label} Branches", fontsize=12, fontweight="bold")
        ax.set_yscale("log")
        ax.grid(True, alpha=0.3, axis="y")
        ax.tick_params(axis='x', rotation=45)

    fig.suptitle("Branch Creation Latency Distribution by Backend", fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "branch_latency_boxplots.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def plot_latency_heatmap(data, outdir):
    """Create a heatmap of median latencies: backends x branch counts."""
    backends = sorted(data.keys())

    # Get all unique branch counts across backends
    all_branch_counts = set()
    for backend in backends:
        all_branch_counts.update(data[backend].keys())
    branch_counts = sorted(all_branch_counts)

    # Build matrix: rows=backends, cols=branch_counts
    matrix = np.full((len(backends), len(branch_counts)), np.nan)

    for i, backend in enumerate(backends):
        for j, bc in enumerate(branch_counts):
            if bc in data[backend]:
                matrix[i, j] = data[backend][bc]['latency'].median()

    fig, ax = plt.subplots(figsize=(max(10, len(branch_counts) * 0.6), len(backends) * 0.8))

    # Plot heatmap
    im = ax.imshow(matrix, aspect='auto', cmap='YlOrRd', interpolation='nearest')

    # Add colorbar
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Median Latency (s)", fontsize=11)

    # Set ticks and labels
    ax.set_xticks(range(len(branch_counts)))
    ax.set_xticklabels([str(bc) for bc in branch_counts], rotation=45, ha='right')
    ax.set_yticks(range(len(backends)))
    ax.set_yticklabels([b.upper() for b in backends])

    ax.set_xlabel("Number of Existing Branches", fontsize=12)
    ax.set_ylabel("Backend", fontsize=12)
    ax.set_title("Branch Creation Latency Heatmap (Median)", fontsize=14, fontweight="bold")

    # Add text annotations
    for i in range(len(backends)):
        for j in range(len(branch_counts)):
            if not np.isnan(matrix[i, j]):
                text = ax.text(
                    j, i, f'{matrix[i, j]:.3f}',
                    ha="center", va="center",
                    color="white" if matrix[i, j] > np.nanmedian(matrix) else "black",
                    fontsize=9,
                )

    fig.tight_layout()
    path = os.path.join(outdir, "branch_latency_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def print_summary(data):
    """Print summary statistics."""
    print("\n=== SUMMARY STATISTICS ===")

    for backend in sorted(data.keys()):
        print(f"\n{backend.upper()}:")
        branch_counts = sorted(data[backend].keys())

        for bc in branch_counts:
            df = data[backend][bc]
            latencies = df['latency'].values * 1000  # Convert to milliseconds

            print(f"  {bc:4d} branches: "
                  f"median={np.median(latencies):.4f}ms, "
                  f"p95={np.percentile(latencies, 95):.4f}ms, "
                  f"n={len(latencies)}")


def main():
    parser = argparse.ArgumentParser(
        description="Plot branch creation latency across backends."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="final/micro/branch",
        help="Directory containing parquet files (default: final/micro/branch)",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="figures/micro",
        help="Output directory for figures (default: figures/micro)",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print("Loading branch creation data...")
    data = load_branch_data(args.data_dir)

    if not data:
        print("No data found!")
        return

    print("\nPlotting: Latency vs branch count")
    plot_latency_vs_branches(data, args.outdir)

    print_summary(data)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
