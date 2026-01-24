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


def process_range_updates(df: pd.DataFrame) -> pd.DataFrame:
    """Mark RANGE_UPDATE ops and compute per-key latency."""
    if df.empty or "num_keys_touched" not in df.columns:
        return df

    range_update_mask = (df["op_type"] == 5) & (df["num_keys_touched"] > 1)
    if range_update_mask.any():
        df = df.copy()
        df.loc[range_update_mask, "op_type"] = 6
        df.loc[range_update_mask, "latency"] = (
            df.loc[range_update_mask, "latency"]
            / df.loc[range_update_mask, "num_keys_touched"]
        )
    return df


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

    # Aggregate by (num_threads, op_type)
    aggregated = (
        combined_df.groupby(["num_threads", "op_type"])
        .agg(
            {
                "latency": [
                    lambda x: np.percentile(x, 50),
                    lambda x: np.percentile(x, 99),
                    "std",
                    "count",
                ]
            }
        )
        .reset_index()
    )
    aggregated.columns = [
        "num_threads",
        "op_type",
        "latency_p50",
        "latency_p99",
        "latency_std",
        "count",
    ]

    return aggregated


OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH",
    2: "CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "RANGE_UPDATE (per-key)",
}

OP_COLORS = {
    0: "#888888",
    1: "#1f77b4",
    2: "#ff7f0e",
    3: "#2ca02c",
    4: "#d62728",
    5: "#9467bd",
    6: "#8c564b",
}


def plot_latencies(
    df: pd.DataFrame, output_path: str = None, log_scale: bool = False
):
    """Create line plot with p50/p99 latency vs num_threads for each operation."""
    plt.figure(figsize=(14, 9))

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

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved figure to {output_path}")
    else:
        plt.show()
    plt.close()


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
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
