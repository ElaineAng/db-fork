#!/usr/bin/env python3
"""Visualize multi-op benchmark results with p50 and p99 latencies.

Usage:
    python visualize_multiop.py benchmark_8.parquet benchmark_16.parquet ...

The script expects parquet filenames to contain the number of branches.
For each operation type, it draws two lines: p50 and p99 latency.
"""

import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def extract_num_branches(filename: str) -> int:
    """Extract the number of branches from the filename."""
    match = re.search(r"(\d+)", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(
        f"Could not extract num_branches from filename: {filename}"
    )


def load_and_compute_percentiles(parquet_files: list[str]) -> pd.DataFrame:
    """Load parquet files and compute p50/p99 by (num_branches, op_type)."""
    all_data = []

    for filepath in parquet_files:
        num_branches = extract_num_branches(filepath)
        df = pd.read_parquet(filepath)
        df["num_branches"] = num_branches

        # Distinguish UPDATE (op_type=5, num_keys_touched=1) from
        # RANGE_UPDATE (op_type=5, num_keys_touched > 1)
        if "num_keys_touched" in df.columns:
            range_update_mask = (df["op_type"] == 5) & (
                df["num_keys_touched"] > 1
            )
            if range_update_mask.any():
                df.loc[range_update_mask, "op_type"] = 6
                df.loc[range_update_mask, "latency"] = (
                    df.loc[range_update_mask, "latency"]
                    / df.loc[range_update_mask, "num_keys_touched"]
                )

        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    # Group by num_branches and op_type, compute percentiles
    aggregated = (
        combined.groupby(["num_branches", "op_type"])
        .agg(
            {
                "latency": [
                    lambda x: np.percentile(x, 50),  # p50
                    lambda x: np.percentile(x, 99),  # p99
                    "count",
                ]
            }
        )
        .reset_index()
    )

    # Flatten column names
    aggregated.columns = [
        "num_branches",
        "op_type",
        "latency_p50",
        "latency_p99",
        "count",
    ]

    return aggregated


# Operation type enum values to names (from task.proto)
OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH",
    2: "CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "RANGE_UPDATE (per-key)",
}

# Colors for each operation type
OP_COLORS = {
    0: "#888888",
    1: "#1f77b4",
    2: "#ff7f0e",
    3: "#2ca02c",
    4: "#d62728",
    5: "#9467bd",
    6: "#8c564b",
}


def plot_latency_percentiles(df: pd.DataFrame, output_path: str = None):
    """Create a line plot of p50/p99 latency vs num_branches for each operation type."""
    plt.figure(figsize=(14, 9))

    op_types = sorted(df["op_type"].unique())

    for op_type in op_types:
        op_data = df[df["op_type"] == op_type].sort_values("num_branches")
        op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")
        color = OP_COLORS.get(op_type, "#000000")

        # p50 line (solid)
        plt.plot(
            op_data["num_branches"],
            op_data["latency_p50"] * 1000,
            marker="o",
            linestyle="-",
            color=color,
            label=f"{op_name} (p50)",
        )

        # p99 line (dashed)
        plt.plot(
            op_data["num_branches"],
            op_data["latency_p99"] * 1000,
            marker="s",
            linestyle="--",
            color=color,
            label=f"{op_name} (p99)",
            alpha=0.7,
        )

    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel("Latency (ms)", fontsize=12)
    plt.title(
        "Multi-Op Benchmark: p50 and p99 Latency vs Number of Branches",
        fontsize=14,
    )
    plt.legend(
        title="Operation Type", bbox_to_anchor=(1.05, 1), loc="upper left"
    )
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved figure to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize multi-op benchmark results with p50 and p99 latencies."
    )
    parser.add_argument(
        "pattern",
        type=str,
        help="Glob pattern for parquet files (e.g., 'benchmark_*.parquet' or '/path/to/benchmark_*').",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output file path for the figure.",
    )

    args = parser.parse_args()

    # Expand glob pattern to list of files
    import glob

    # Add .parquet extension if not present in pattern
    pattern = args.pattern
    if not pattern.endswith(".parquet"):
        pattern = pattern + ".parquet"

    parquet_files = sorted(glob.glob(pattern))

    if not parquet_files:
        print(
            f"Error: No files found matching pattern: {args.pattern}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"Found {len(parquet_files)} files matching pattern '{args.pattern}':"
    )
    for f in parquet_files:
        print(f"  - {f}")

    try:
        df = load_and_compute_percentiles(parquet_files)
        print(f"Aggregated data:\n{df}")
        plot_latency_percentiles(df, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
