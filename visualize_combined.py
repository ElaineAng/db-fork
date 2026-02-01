#!/usr/bin/env python3
"""Visualize benchmark results from run_stats directory.

Usage:
    # Generate all figures for a backend (combines prefilled + empty)
    python visualize_combined.py --backend dolt

    # Generate only multiop or nth_op figures
    python visualize_combined.py --backend dolt --type multiop
    python visualize_combined.py --backend neon --type nth_op

    # Include storage plots
    python visualize_combined.py --backend dolt --storage
"""

import argparse
import glob
import os
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from viz_common import (
    OP_COLORS,
    OP_TYPE_NAMES,
    auto_scale_storage,
    process_range_updates,
    save_or_show,
)


def extract_num_branches(filename: str) -> int:
    """Extract the number of branches from the filename."""
    match = re.search(r"(\d+)_spine", Path(filename).stem)
    if match:
        return int(match.group(1))
    # Fallback: find any number
    match = re.search(r"(\d+)", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(
        f"Could not extract num_branches from filename: {filename}"
    )


def find_subdirs(
    base_dir: str, backend: str, benchmark_type: str
) -> tuple[str, str]:
    """Find prefilled and empty subdirectories for a backend and benchmark type.

    Returns:
        Tuple of (prefilled_dir, empty_dir) or (None, None) if not found.
    """
    pattern = f"{backend}_{benchmark_type}_*"
    subdirs = glob.glob(os.path.join(base_dir, pattern))

    prefilled_dir = None
    empty_dir = None

    for subdir in subdirs:
        dirname = os.path.basename(subdir)
        if "prefilled" in dirname:
            prefilled_dir = subdir
        elif "empty" in dirname:
            empty_dir = subdir

    return prefilled_dir, empty_dir


def load_parquet_files(directory: str, label: str) -> pd.DataFrame:
    """Load all parquet files from a directory and add metadata."""
    if not directory or not os.path.exists(directory):
        return pd.DataFrame()

    parquet_files = glob.glob(os.path.join(directory, "*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    all_data = []
    for filepath in parquet_files:
        num_branches = extract_num_branches(filepath)
        df = pd.read_parquet(filepath)
        df["num_branches"] = num_branches
        df["data_source"] = label  # "prefilled" or "empty"
        all_data.append(df)

    return (
        pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    )


def aggregate_for_nth_op(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate by (data_source, num_branches, op_type) for mean latency."""
    agg_dict = {"latency": ["mean", "std", "count"]}
    if "disk_size_after" in df.columns:
        agg_dict["disk_size_after"] = "max"
    return (
        df.groupby(["data_source", "num_branches", "op_type"])
        .agg(agg_dict)
        .reset_index()
    )


def aggregate_for_multiop(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate by (data_source, num_branches, op_type) for p50/p99."""
    agg_dict = {
        "latency": [
            lambda x: np.percentile(x, 50),
            lambda x: np.percentile(x, 99),
            "count",
        ]
    }
    if "disk_size_after" in df.columns:
        agg_dict["disk_size_after"] = "max"
    return (
        df.groupby(["data_source", "num_branches", "op_type"])
        .agg(agg_dict)
        .reset_index()
    )


def plot_nth_op_combined(
    prefilled_df: pd.DataFrame,
    empty_df: pd.DataFrame,
    backend: str,
    output_path: str = None,
    log_scale: bool = False,
):
    """Plot nth-op benchmark comparing prefilled vs empty."""
    fig = plt.figure(figsize=(14, 9))

    for df, linestyle, label_suffix in [
        (prefilled_df, "-", " (prefilled)"),
        (empty_df, "--", " (empty)"),
    ]:
        if df.empty:
            continue

        df = process_range_updates(df)
        agg = aggregate_for_nth_op(df)
        cols = [
            "data_source",
            "num_branches",
            "op_type",
            "latency_mean",
            "latency_std",
            "count",
        ]
        if "disk_size_after" in df.columns:
            cols.append("storage_max")
        agg.columns = cols

        op_types = sorted(agg["op_type"].unique())
        for op_type in op_types:
            op_data = agg[agg["op_type"] == op_type].sort_values("num_branches")
            op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")
            color = OP_COLORS.get(op_type, "#000000")

            plt.errorbar(
                op_data["num_branches"],
                op_data["latency_mean"] * 1000,
                yerr=op_data["latency_std"] * 1000,
                marker="o" if "prefilled" in label_suffix else "s",
                linestyle=linestyle,
                color=color,
                label=f"{op_name}{label_suffix}",
                capsize=3,
                alpha=0.8 if "prefilled" in label_suffix else 0.6,
            )

    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel("Average Latency (ms)", fontsize=12)
    plt.title(
        f"{backend.upper()} Nth-Op Benchmark: Prefilled vs Empty", fontsize=14
    )
    plt.legend(
        title="Operation Type", bbox_to_anchor=(1.05, 1), loc="upper left"
    )
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()
    save_or_show(fig, output_path)


def plot_multiop_combined(
    prefilled_df: pd.DataFrame,
    empty_df: pd.DataFrame,
    backend: str,
    output_path: str = None,
    log_scale: bool = False,
):
    """Plot multiop benchmark comparing prefilled vs empty (p50 only for clarity)."""
    fig = plt.figure(figsize=(14, 9))

    for df, linestyle, label_suffix in [
        (prefilled_df, "-", " (prefilled)"),
        (empty_df, "--", " (empty)"),
    ]:
        if df.empty:
            continue

        df = process_range_updates(df)
        agg = aggregate_for_multiop(df)
        cols = [
            "data_source",
            "num_branches",
            "op_type",
            "latency_p50",
            "latency_p99",
            "count",
        ]
        if "disk_size_after" in df.columns:
            cols.append("storage_max")
        agg.columns = cols

        op_types = sorted(agg["op_type"].unique())
        for op_type in op_types:
            op_data = agg[agg["op_type"] == op_type].sort_values("num_branches")
            op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")
            color = OP_COLORS.get(op_type, "#000000")

            # Plot p50
            plt.plot(
                op_data["num_branches"],
                op_data["latency_p50"] * 1000,
                marker="o" if "prefilled" in label_suffix else "s",
                linestyle=linestyle,
                color=color,
                label=f"{op_name} p50{label_suffix}",
                alpha=0.8 if "prefilled" in label_suffix else 0.6,
            )

    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel("p50 Latency (ms)", fontsize=12)
    plt.title(
        f"{backend.upper()} Multi-Op Benchmark: Prefilled vs Empty", fontsize=14
    )
    plt.legend(
        title="Operation Type", bbox_to_anchor=(1.05, 1), loc="upper left"
    )
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()
    save_or_show(fig, output_path)


def plot_storage_combined(
    prefilled_df: pd.DataFrame,
    empty_df: pd.DataFrame,
    backend: str,
    bench_type: str,
    output_path: str = None,
    log_scale: bool = False,
):
    """Plot storage vs branches for prefilled and empty data sources."""
    has_data = False
    fig = plt.figure(figsize=(12, 8))

    all_values = []
    plot_items = []

    for df, linestyle, label_suffix in [
        (prefilled_df, "-", "prefilled"),
        (empty_df, "--", "empty"),
    ]:
        if df.empty:
            continue
        df = process_range_updates(df)
        if "disk_size_after" not in df.columns:
            continue

        # Filter to BRANCH rows (op_type=1) and aggregate
        branch_df = df[df["op_type"] == 1].copy()
        if branch_df.empty:
            continue

        storage_agg = (
            branch_df.groupby(["num_branches"])
            .agg({"disk_size_after": "max"})
            .reset_index()
        )
        storage_agg.columns = ["num_branches", "storage_max"]
        storage_agg = storage_agg[storage_agg["storage_max"] > 0]
        if storage_agg.empty:
            continue

        all_values.append(storage_agg["storage_max"])
        plot_items.append((storage_agg, linestyle, label_suffix))

    if not plot_items:
        print("Warning: No storage data found. Skipping storage plot.")
        plt.close(fig)
        return

    # Pick a single unit based on all values
    combined_values = pd.concat(all_values)
    _, unit = auto_scale_storage(combined_values)

    for storage_agg, linestyle, label_suffix in plot_items:
        storage_agg = storage_agg.sort_values("num_branches")
        scaled, _ = auto_scale_storage(storage_agg["storage_max"])
        # Re-scale with the global unit for consistency
        if unit == "GB":
            scaled = storage_agg["storage_max"] / (1 << 30)
        elif unit == "MB":
            scaled = storage_agg["storage_max"] / (1 << 20)
        elif unit == "KB":
            scaled = storage_agg["storage_max"] / (1 << 10)
        else:
            scaled = storage_agg["storage_max"]

        plt.plot(
            storage_agg["num_branches"],
            scaled,
            marker="o" if label_suffix == "prefilled" else "s",
            linestyle=linestyle,
            color=OP_COLORS[1],
            label=f"Storage ({label_suffix})",
            alpha=0.8 if label_suffix == "prefilled" else 0.6,
        )
        has_data = True

    if not has_data:
        print("Warning: No storage data found. Skipping storage plot.")
        plt.close(fig)
        return

    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel(f"Storage ({unit})", fontsize=12)
    plt.title(
        f"{backend.upper()} {bench_type.replace('_', '-').title()} Benchmark: Storage",
        fontsize=14,
    )
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()
    save_or_show(fig, output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Visualize benchmark results combining prefilled and empty data."
    )
    parser.add_argument(
        "--backend",
        type=str,
        required=True,
        help="Backend to visualize (e.g., 'dolt', 'neon').",
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["multiop", "nth_op", "all"],
        default="all",
        help="Benchmark type to visualize (default: all).",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default="./run_stats",
        help="Base directory containing benchmark subdirectories.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=".",
        help="Output directory for figures.",
    )
    parser.add_argument(
        "--log-scale",
        action="store_true",
        default=False,
        help="Use log scale for y-axis.",
    )
    parser.add_argument(
        "--storage",
        action="store_true",
        default=False,
        help="Also generate storage vs branches plots.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.base_dir):
        print(
            f"Error: Base directory not found: {args.base_dir}", file=sys.stderr
        )
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    benchmark_types = (
        ["multiop", "nth_op"] if args.type == "all" else [args.type]
    )

    for bench_type in benchmark_types:
        print(f"\n{'=' * 60}")
        print(f"Processing {args.backend} {bench_type}")
        print(f"{'=' * 60}")

        prefilled_dir, empty_dir = find_subdirs(
            args.base_dir, args.backend, bench_type
        )

        print(f"Prefilled dir: {prefilled_dir}")
        print(f"Empty dir: {empty_dir}")

        if not prefilled_dir and not empty_dir:
            print(f"Warning: No data found for {args.backend} {bench_type}")
            continue

        prefilled_df = load_parquet_files(prefilled_dir, "prefilled")
        empty_df = load_parquet_files(empty_dir, "empty")

        print(
            f"Loaded {len(prefilled_df)} prefilled rows, {len(empty_df)} empty rows"
        )

        output_path = os.path.join(
            args.output_dir, f"{args.backend}_{bench_type}_combined.png"
        )

        if bench_type == "nth_op":
            plot_nth_op_combined(
                prefilled_df,
                empty_df,
                args.backend,
                output_path,
                args.log_scale,
            )
        else:
            plot_multiop_combined(
                prefilled_df,
                empty_df,
                args.backend,
                output_path,
                args.log_scale,
            )

        if args.storage:
            storage_output = os.path.join(
                args.output_dir,
                f"{args.backend}_{bench_type}_storage_combined.png",
            )
            plot_storage_combined(
                prefilled_df,
                empty_df,
                args.backend,
                bench_type,
                storage_output,
                args.log_scale,
            )


if __name__ == "__main__":
    main()
