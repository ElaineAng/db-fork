#!/usr/bin/env python3
"""Visualize multi-threaded benchmark results.

Produces p50 and p99 latency lines for each operation type, aggregated across threads.

Usage:
    python visualize_multithread.py 'benchmark_*.parquet' -o output.png
    python visualize_multithread.py '/path/to/run_stats/*multithread*/*.parquet' --log-scale
"""

import argparse
import glob
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


def extract_num_threads(filename: str) -> int:
    """Extract the number of threads from the filename."""
    match = re.search(
        r"(?:multithread|multi_trd_op)_(\d+)_", Path(filename).stem
    )
    if match:
        return int(match.group(1))
    match = re.search(r"(\d+)_fanout", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(f"Could not extract num_threads from filename: {filename}")


def load_and_compute_percentiles(parquet_files: list) -> pd.DataFrame:
    """Load parquet files and compute p50/p99 latency per (num_threads, op_type)."""
    all_data = []

    for filepath in parquet_files:
        num_threads = extract_num_threads(filepath)
        df = pd.read_parquet(filepath)
        df["num_threads"] = num_threads
        df = process_range_updates(df)
        all_data.append(df)

    if not all_data:
        raise ValueError("No data loaded from parquet files")

    combined_df = pd.concat(all_data, ignore_index=True)

    # Build aggregation dict
    agg_dict = {
        "latency": [
            lambda x: np.percentile(x, 50),
            lambda x: np.percentile(x, 99),
            "std",
            "count",
        ]
    }
    if "disk_size_after" in combined_df.columns:
        agg_dict["disk_size_after"] = "max"

    # Aggregate by (num_threads, op_type)
    aggregated = (
        combined_df.groupby(["num_threads", "op_type"])
        .agg(agg_dict)
        .reset_index()
    )

    cols = [
        "num_threads",
        "op_type",
        "latency_p50",
        "latency_p99",
        "latency_std",
        "count",
    ]
    if "disk_size_after" in combined_df.columns:
        cols.append("storage_max")
    aggregated.columns = cols

    return aggregated


def plot_latencies(
    df: pd.DataFrame, output_path: str = None, log_scale: bool = False
):
    """Create line plot with p50/p99 latency vs num_threads for each operation."""
    fig = plt.figure(figsize=(14, 9))

    for op_type in sorted(df["op_type"].unique()):
        op_data = df[df["op_type"] == op_type].sort_values("num_threads")
        op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")
        color = OP_COLORS.get(op_type, "#000000")

        plt.plot(
            op_data["num_threads"],
            op_data["latency_p50"] * 1000,
            marker="o",
            linestyle="-",
            color=color,
            label=f"{op_name} p50",
            alpha=0.8,
        )
        plt.plot(
            op_data["num_threads"],
            op_data["latency_p99"] * 1000,
            marker="s",
            linestyle="--",
            color=color,
            label=f"{op_name} p99",
            alpha=0.6,
        )

    plt.xlabel("Number of Threads", fontsize=12)
    plt.ylabel("Latency (ms)", fontsize=12)
    plt.title("Multi-Thread Benchmark: p50 and p99 Latency", fontsize=14)
    plt.legend(
        title="Operation Type", bbox_to_anchor=(1.05, 1), loc="upper left"
    )
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")
    plt.tight_layout()
    save_or_show(fig, output_path)


def plot_storage_by_threads(
    df: pd.DataFrame, output_path: str = None, log_scale: bool = False
):
    """Plot total DB storage vs number of threads (BRANCH_CREATE rows only)."""
    if "storage_max" not in df.columns:
        print("Warning: No storage data found. Skipping storage plot.")
        return

    storage_df = df[(df["op_type"] == 1) & (df["storage_max"] > 0)]
    if storage_df.empty:
        print("Warning: No non-zero storage data found. Skipping storage plot.")
        return

    storage_df = storage_df.sort_values("num_threads")
    scaled, unit = auto_scale_storage(storage_df["storage_max"])

    fig = plt.figure(figsize=(12, 8))
    plt.plot(
        storage_df["num_threads"],
        scaled,
        marker="o",
        color=OP_COLORS[1],
        label="Total Storage",
    )
    plt.xlabel("Number of Threads", fontsize=12)
    plt.ylabel(f"Storage ({unit})", fontsize=12)
    plt.title("Multi-Thread Benchmark: Storage vs Number of Threads", fontsize=14)
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
        description="Visualize multi-threaded benchmark results with p50/p99 latencies."
    )
    parser.add_argument(
        "pattern", type=str, help="Glob pattern for parquet files."
    )
    parser.add_argument(
        "-o", "--output", type=str, default=None, help="Output file path."
    )
    parser.add_argument(
        "-s",
        "--storage-output",
        type=str,
        default=None,
        help="Output file path for the storage figure.",
    )
    parser.add_argument(
        "--log-scale", action="store_true", help="Use log scale for y-axis."
    )

    args = parser.parse_args()

    pattern = (
        args.pattern
        if args.pattern.endswith(".parquet")
        else args.pattern + ".parquet"
    )
    parquet_files = sorted(glob.glob(pattern))

    if not parquet_files:
        print(
            f"Error: No files found matching pattern: {args.pattern}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Found {len(parquet_files)} files")

    try:
        df = load_and_compute_percentiles(parquet_files)
        print(f"Aggregated data:\n{df}")
        plot_latencies(df, args.output, args.log_scale)
        if args.storage_output:
            plot_storage_by_threads(
                df, args.storage_output, log_scale=args.log_scale
            )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
