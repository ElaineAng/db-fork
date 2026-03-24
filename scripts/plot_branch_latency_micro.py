#!/usr/bin/env python3
"""
Plot branch creation and connection latency across different backends and branch counts.

Usage:
    # Plot branch creation latency only
    python micro-analysis/plot_branch_latency.py --data-dir final/micro/branch --outdir figures/

    # Plot branch connection latency only
    python micro-analysis/plot_branch_latency.py --data-dir final/micro/connect --outdir figures/ --operation connect

    # Plot both (separate plots from same directory)
    python micro-analysis/plot_branch_latency.py --data-dir final/micro/branch --outdir figures/ --operation both

    # Plot combined (branch and connect on single plot from two directories)
    # Note: Automatically extracts connect ops from branch files for backends missing connect data
    python micro-analysis/plot_branch_latency.py --branch-dir final/micro/branch --connect-dir final/micro/connect --outdir figures/ --operation combined

    # Plot combined (extract connect ops from branch files if no separate connect dir)
    python micro-analysis/plot_branch_latency.py --branch-dir final/micro/branch --outdir figures/ --operation combined
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
OP_TYPE_CONNECT_FIRST = 10
OP_TYPE_CONNECT_MID = 11
OP_TYPE_CONNECT_LAST = 12

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

# Backend markers - different shapes for differentiation
BACKEND_MARKERS = {
    "dolt": "o",      # Circle
    "neon": "s",      # Square
    "kpg": "^",       # Triangle up
    "xata": "D",      # Diamond
    "file_copy": "v", # Triangle down
    "txn": "p",       # Pentagon
    "tiger": "*",     # Star
}


def parse_filename(filename, operation='branch'):
    """Extract backend and branch_count from filename.

    Example: dolt_tpcc_16_spine_branch.parquet -> ('dolt', 16)
             TIGER_tpcc_16_spine_branch.parquet -> ('tiger', 16)
             dolt_tpcc_16_spine_connect.parquet -> ('dolt', 16)
             dolt_tpcc_multitrd_16_spine.parquet -> ('dolt', 16)
             neon_tpcc_multitrd_16_fan_out.parquet -> ('neon', 16)
    """
    basename = os.path.basename(filename)

    # Try multithread pattern with any shape: {backend}_{schema}_multitrd_{threads}_{shape}.parquet
    # For multithread files, threads = branches
    multithread_pattern = rf'([a-zA-Z_]+)_\w+_multitrd_(\d+)_(?:spine|fan_out|bushy)\.parquet$'
    match = re.match(multithread_pattern, basename, re.IGNORECASE)
    if match:
        backend = match.group(1).lower()  # Normalize to lowercase
        threads = int(match.group(2))
        return backend, threads

    # Original pattern: {backend}_{schema}_{branches}_{shape}_{operation}.parquet
    pattern = rf'([a-zA-Z_]+)_\w+_(\d+)_(?:spine|fan_out|bushy)_{operation}\.parquet$'
    match = re.match(pattern, basename, re.IGNORECASE)
    if match:
        backend = match.group(1).lower()  # Normalize to lowercase
        branches = int(match.group(2))
        return backend, branches

    return None, None


def load_branch_data(data_dir):
    """Load all branch creation parquet files.

    Supports both single-thread (*_{shape}_branch.parquet) and
    multithread (*_multitrd_*_{shape}.parquet) formats.

    Returns:
        dict: {backend: {branch_count: DataFrame}}
    """
    data = defaultdict(dict)

    # Look for both original and multithread patterns with different shapes
    patterns = [
        os.path.join(data_dir, "*_spine_branch.parquet"),
        os.path.join(data_dir, "*_fan_out_branch.parquet"),
        os.path.join(data_dir, "*_bushy_branch.parquet"),
        os.path.join(data_dir, "*_multitrd_*_spine.parquet"),
        os.path.join(data_dir, "*_multitrd_*_fan_out.parquet"),
        os.path.join(data_dir, "*_multitrd_*_bushy.parquet"),
    ]

    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))

    for filepath in files:
        backend, branches = parse_filename(filepath, operation='branch')
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


def load_connect_data(data_dir):
    """Load all branch connect parquet files.

    Supports both single-thread (*_{shape}_connect*.parquet) and
    multithread (*_multitrd_*_{shape}.parquet) formats.

    If no dedicated connect files are found, will also extract CONNECT operations
    from branch files (e.g., for TIGER backend).

    Returns:
        dict: {backend: {branch_count: DataFrame}}
    """
    data = defaultdict(dict)

    # Match connect, connect_first, connect_mid, connect_last, and multithread with different shapes
    connect_patterns = [
        os.path.join(data_dir, "*_spine_connect.parquet"),
        os.path.join(data_dir, "*_spine_connect_first.parquet"),
        os.path.join(data_dir, "*_spine_connect_mid.parquet"),
        os.path.join(data_dir, "*_spine_connect_last.parquet"),
        os.path.join(data_dir, "*_fan_out_connect.parquet"),
        os.path.join(data_dir, "*_fan_out_connect_first.parquet"),
        os.path.join(data_dir, "*_fan_out_connect_mid.parquet"),
        os.path.join(data_dir, "*_fan_out_connect_last.parquet"),
        os.path.join(data_dir, "*_bushy_connect.parquet"),
        os.path.join(data_dir, "*_bushy_connect_first.parquet"),
        os.path.join(data_dir, "*_bushy_connect_mid.parquet"),
        os.path.join(data_dir, "*_bushy_connect_last.parquet"),
        os.path.join(data_dir, "*_multitrd_*_spine.parquet"),
        os.path.join(data_dir, "*_multitrd_*_fan_out.parquet"),
        os.path.join(data_dir, "*_multitrd_*_bushy.parquet"),
    ]

    files = []
    for pattern in connect_patterns:
        files.extend(glob.glob(pattern))

    # All CONNECT operation types (CONNECT=2, CONNECT_FIRST=10, CONNECT_MID=11, CONNECT_LAST=12)
    connect_op_types = [OP_TYPE_BRANCH_CONNECT, OP_TYPE_CONNECT_FIRST, OP_TYPE_CONNECT_MID, OP_TYPE_CONNECT_LAST]

    for filepath in files:
        # Try parsing with different operation names
        backend, branches = None, None
        for op_name in ['connect', 'connect_first', 'connect_mid', 'connect_last']:
            backend, branches = parse_filename(filepath, operation=op_name)
            if backend is not None:
                break

        if backend is None:
            print(f"  Warning: Could not parse filename: {os.path.basename(filepath)}")
            continue

        # Read parquet file
        df = pq.read_table(filepath).to_pandas()

        # Filter to all CONNECT operations (op_type in [2, 10, 11, 12])
        df_connect = df[df['op_type'].isin(connect_op_types)].copy()

        if len(df_connect) == 0:
            print(f"  Warning: No CONNECT operations in {os.path.basename(filepath)}")
            continue

        # Aggregate with existing data for same backend/branch count if present
        if branches in data[backend]:
            data[backend][branches] = pd.concat([data[backend][branches], df_connect], ignore_index=True)
        else:
            data[backend][branches] = df_connect

        print(f"  Loaded {backend:10s} branches={branches:4d}: {len(df_connect):3d} CONNECT ops from {os.path.basename(filepath)}")

    # Also look for CONNECT operations in branch files (for backends without dedicated connect files)
    branch_patterns = [
        os.path.join(data_dir, "*_spine_branch.parquet"),
        os.path.join(data_dir, "*_fan_out_branch.parquet"),
        os.path.join(data_dir, "*_bushy_branch.parquet"),
    ]

    branch_files = []
    for pattern in branch_patterns:
        branch_files.extend(glob.glob(pattern))

    for filepath in branch_files:
        backend, branches = parse_filename(filepath, operation='branch')
        if backend is None:
            continue

        # Skip if we already have connect data for this backend/branch_count
        if branches in data[backend]:
            continue

        # Read parquet file
        df = pq.read_table(filepath).to_pandas()

        # Filter to all CONNECT operations
        df_connect = df[df['op_type'].isin(connect_op_types)].copy()

        if len(df_connect) == 0:
            continue

        data[backend][branches] = df_connect
        print(f"  Loaded {backend:10s} branches={branches:4d}: {len(df_connect):3d} CONNECT ops from branch file {os.path.basename(filepath)}")

    return data


def plot_latency_vs_branches(data, outdir, is_multithread=False, legend_bbox=None, legend_loc=None):
    """Plot branch creation latency vs number of existing branches.

    Shows one line per backend on a single plot.
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
        marker = BACKEND_MARKERS.get(backend, "o")
        ax.plot(
            branch_counts,
            medians,
            marker=marker,
            markersize=10,
            linewidth=2.5,
            label=backend.upper(),
            color=color,
            alpha=0.9,
        )

    if is_multithread:
        ax.set_xlabel("Number of Concurrent Threads (= Branches) (log scale)", fontsize=20)
    else:
        ax.set_xlabel("Number of Existing Branches (log scale)", fontsize=20)

    ax.set_ylabel("Branch Creation Latency (ms) (log scale)", fontsize=20)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)

    # Format tick labels to show actual values instead of powers
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x)}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: f'{int(y)}' if y >= 1 else f'{y:.2g}'))
    ax.tick_params(axis='both', which='major', labelsize=14)

    ax.grid(True, alpha=0.25, which="major", linewidth=0.8)
    ax.grid(True, alpha=0.15, which="minor", linewidth=0.4)

    # Place legend on top of plot outside plot area in 2 rows by default
    if legend_loc is None:
        legend_loc = 'lower center'
    if legend_bbox is None:
        legend_bbox = (0.5, 1.02)

    legend_kwargs = {
        'fontsize': 16,
        'framealpha': 0.95,
        'edgecolor': 'gray',
        'fancybox': True,
        'loc': legend_loc,
        'bbox_to_anchor': legend_bbox,
        'ncol': 4  # Spread items horizontally to fit in 2 rows max
    }

    ax.legend(**legend_kwargs)

    fig.tight_layout()
    path = os.path.join(outdir, "branch_latency_vs_count.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def plot_connect_latency_vs_branches(data, outdir, is_multithread=False, legend_bbox=None, legend_loc=None):
    """Plot branch connection latency vs number of existing branches.

    Shows one line per backend on a single plot.
    Shows average (mean) connection latency.
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    for backend in sorted(data.keys()):
        branch_counts = sorted(data[backend].keys())
        means = []

        for bc in branch_counts:
            df = data[backend][bc]
            latencies = df['latency'].values * 1000  # Convert to milliseconds
            means.append(np.mean(latencies))

        # Convert to arrays
        branch_counts = np.array(branch_counts)
        means = np.array(means)

        # Plot line
        color = BACKEND_COLORS.get(backend, "#000000")
        marker = BACKEND_MARKERS.get(backend, "o")
        ax.plot(
            branch_counts,
            means,
            marker=marker,
            markersize=10,
            linewidth=2.5,
            label=backend.upper(),
            color=color,
            alpha=0.9,
        )

    if is_multithread:
        ax.set_xlabel("Number of Concurrent Threads (= Branches) (log scale)", fontsize=20)
    else:
        ax.set_xlabel("Number of Existing Branches (log scale)", fontsize=20)

    ax.set_ylabel("Branch Connection Latency (ms) (log scale)", fontsize=20)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)

    # Format tick labels to show actual values instead of powers
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x)}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: f'{int(y)}' if y >= 1 else f'{y:.2g}'))
    ax.tick_params(axis='both', which='major', labelsize=14)

    ax.grid(True, alpha=0.25, which="major", linewidth=0.8)
    ax.grid(True, alpha=0.15, which="minor", linewidth=0.4)

    # Place legend on top of plot outside plot area in 2 rows by default
    if legend_loc is None:
        legend_loc = 'lower center'
    if legend_bbox is None:
        legend_bbox = (0.5, 1.02)

    legend_kwargs = {
        'fontsize': 16,
        'framealpha': 0.95,
        'edgecolor': 'gray',
        'fancybox': True,
        'loc': legend_loc,
        'bbox_to_anchor': legend_bbox,
        'ncol': 4  # Spread items horizontally to fit in 2 rows max
    }

    ax.legend(**legend_kwargs)

    fig.tight_layout()
    path = os.path.join(outdir, "connect_latency_vs_count.png")
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


def plot_combined_latency(branch_data, connect_data, outdir, is_multithread=False, legend_bbox=None, legend_loc=None):
    """Plot both branch and connect latency on a single figure.

    Each backend+operation combination gets its own line:
    - "Dolt Branch", "Dolt Connect"
    - "Neon Branch", "Neon Connect"
    etc.
    """
    fig, ax = plt.subplots(figsize=(12, 8))

    # Get all backends across both datasets
    all_backends = set(branch_data.keys()) | set(connect_data.keys())

    for backend in sorted(all_backends):
        color = BACKEND_COLORS.get(backend, "#000000")
        marker = BACKEND_MARKERS.get(backend, "o")

        # Plot branch creation data (solid line, backend-specific marker)
        if backend in branch_data:
            branch_counts = sorted(branch_data[backend].keys())
            medians = []

            for bc in branch_counts:
                df = branch_data[backend][bc]
                latencies = df['latency'].values * 1000  # Convert to milliseconds
                medians.append(np.median(latencies))

            branch_counts = np.array(branch_counts)
            medians = np.array(medians)

            ax.plot(
                branch_counts,
                medians,
                marker=marker,
                markersize=10,
                linewidth=2.5,
                linestyle='-',
                label=f"{backend.upper()} Branch",
                color=color,
                alpha=0.9,
            )

        # Plot connect data (dashed line, filled marker for differentiation)
        if backend in connect_data:
            branch_counts = sorted(connect_data[backend].keys())
            means = []

            for bc in branch_counts:
                df = connect_data[backend][bc]
                latencies = df['latency'].values * 1000  # Convert to milliseconds
                means.append(np.mean(latencies))

            branch_counts = np.array(branch_counts)
            means = np.array(means)

            ax.plot(
                branch_counts,
                means,
                marker=marker,
                markersize=10,
                linewidth=2.5,
                linestyle='--',
                label=f"{backend.upper()} Connect",
                color=color,
                alpha=0.7,  # Lighter shade for connect lines
                markerfacecolor='white',  # Hollow marker for connect
                markeredgewidth=2,
            )

    if is_multithread:
        ax.set_xlabel("Number of Concurrent Threads (= Branches) (log scale)", fontsize=20)
    else:
        ax.set_xlabel("Number of Existing Branches (log scale)", fontsize=20)

    ax.set_ylabel("Latency (ms) (log scale)", fontsize=20)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)

    # Format tick labels to show actual values instead of powers
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x)}'))
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, pos: f'{int(y)}' if y >= 1 else f'{y:.2g}'))
    ax.tick_params(axis='both', which='major', labelsize=14)

    ax.grid(True, alpha=0.25, which="major", linewidth=0.8)
    ax.grid(True, alpha=0.15, which="minor", linewidth=0.4)

    # Place legend on top of plot outside plot area in 2 rows by default
    if legend_loc is None:
        legend_loc = 'lower center'
    if legend_bbox is None:
        legend_bbox = (0.5, 1.02)

    legend_kwargs = {
        'fontsize': 16,
        'framealpha': 0.95,
        'edgecolor': 'gray',
        'fancybox': True,
        'loc': legend_loc,
        'bbox_to_anchor': legend_bbox,
        'ncol': 4  # Spread items horizontally to fit in 2 rows max
    }

    ax.legend(**legend_kwargs)

    fig.tight_layout()
    path = os.path.join(outdir, "combined_branch_connect_latency.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Plot branch creation and connection latency across backends."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory containing parquet files (for single operation mode)",
    )
    parser.add_argument(
        "--branch-dir",
        type=str,
        help="Directory containing branch operation parquet files",
    )
    parser.add_argument(
        "--connect-dir",
        type=str,
        help="Directory containing connect operation parquet files",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="figures/micro",
        help="Output directory for figures (default: figures/micro)",
    )
    parser.add_argument(
        "--operation",
        type=str,
        choices=["branch", "connect", "both", "combined"],
        default="branch",
        help="Which operation to plot: branch, connect, both (separate plots), or combined (single plot) (default: branch)",
    )
    parser.add_argument(
        "--multithread",
        action="store_true",
        help="Indicate that data is from multithread experiments (auto-detected if not specified)",
    )
    parser.add_argument(
        "--legend-bbox",
        type=str,
        help="Legend bbox_to_anchor as 'x,y' (e.g., '0.5,1.02' for top center). Default: '0.5,1.02' (on top, outside plot area).",
    )
    parser.add_argument(
        "--legend-loc",
        type=str,
        help="Legend loc parameter (e.g., 'upper left', 'lower center', 'upper right'). Default: 'lower center'.",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Parse legend bbox if provided
    legend_bbox = None
    if args.legend_bbox:
        try:
            x, y = map(float, args.legend_bbox.split(','))
            legend_bbox = (x, y)
        except ValueError:
            print(f"Warning: Invalid --legend-bbox format '{args.legend_bbox}'. Expected 'x,y'. Ignoring.")

    legend_loc = args.legend_loc

    # Handle combined mode with two directories
    if args.operation == "combined":
        if not args.branch_dir:
            print("Error: --branch-dir is required for combined mode")
            return

        # Use branch_dir for connect_dir if not specified (extracts connect ops from branch files)
        connect_dir = args.connect_dir if args.connect_dir else args.branch_dir

        print("Loading branch creation data...")
        branch_data = load_branch_data(args.branch_dir)

        print(f"\nLoading branch connection data from {connect_dir}...")
        connect_data = load_connect_data(connect_dir)

        # For backends with branch data but no connect data, extract connect from branch files
        if args.connect_dir:  # Only do this if separate directories were specified
            backends_missing_connect = set(branch_data.keys()) - set(connect_data.keys())
            if backends_missing_connect:
                print(f"\nBackends missing connect data: {sorted(backends_missing_connect)}")
                print(f"Extracting connect operations from branch files in {args.branch_dir}...")

                # Extract connect data from branch files for missing backends
                fallback_connect_data = load_connect_data(args.branch_dir)

                for backend in backends_missing_connect:
                    if backend in fallback_connect_data:
                        connect_data[backend] = fallback_connect_data[backend]
                        print(f"  Added connect data for {backend} from branch files")

        if not branch_data and not connect_data:
            print("No data found!")
            return

        is_multithread = args.multithread or "multithread" in (args.branch_dir + connect_dir).lower()

        print("\nPlotting: Combined branch and connect latency")
        plot_combined_latency(branch_data, connect_data, args.outdir, is_multithread=is_multithread,
                             legend_bbox=legend_bbox, legend_loc=legend_loc)

        print(f"\nFigure saved to {args.outdir}/")
        return

    # Original single-directory modes
    data_dir = args.data_dir
    if not data_dir:
        print("Error: --data-dir is required for single operation modes")
        return

    # Auto-detect multithread data by checking for "multitrd" in directory path
    is_multithread = args.multithread or "multithread" in data_dir.lower()

    if args.operation in ["branch", "both"]:
        print("Loading branch creation data...")
        data = load_branch_data(data_dir)

        if not data:
            print("No branch data found!")
        else:
            print("\nPlotting: Branch creation latency vs branch count")
            plot_latency_vs_branches(data, args.outdir, is_multithread=is_multithread,
                                   legend_bbox=legend_bbox, legend_loc=legend_loc)
            print_summary(data)

    if args.operation in ["connect", "both"]:
        print("\nLoading branch connection data...")
        connect_data = load_connect_data(data_dir)

        if not connect_data:
            print("No connect data found!")
        else:
            print("\nPlotting: Branch connection latency vs branch count")
            plot_connect_latency_vs_branches(connect_data, args.outdir, is_multithread=is_multithread,
                                           legend_bbox=legend_bbox, legend_loc=legend_loc)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
