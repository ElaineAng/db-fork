#!/usr/bin/env python3
"""Visualize Nth-op benchmark results across different branch counts.

Usage:
    python visualize_nth_op.py benchmark_8_branches.parquet benchmark_16_branches.parquet ...

The script expects parquet filenames to contain the number of branches (e.g., "benchmark_64_branches.parquet").
It will extract the number from the filename using regex.
"""

import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def extract_num_branches(filename: str) -> int:
    """Extract the number of branches from the filename.

    Expects patterns like:
    - benchmark_64_branches.parquet
    - nth_op_64.parquet
    - results_64.parquet
    """
    # Try to find a number in the filename
    match = re.search(r"(\d+)", Path(filename).stem)
    if match:
        return int(match.group(1))
    raise ValueError(
        f"Could not extract num_branches from filename: {filename}"
    )


def load_and_aggregate(parquet_files: list[str]) -> pd.DataFrame:
    """Load parquet files and aggregate by (num_branches, op_type).

    For RANGE_UPDATE operations, computes per-key latency by dividing
    latency by num_keys_touched.
    """
    all_data = []

    for filepath in parquet_files:
        num_branches = extract_num_branches(filepath)
        df = pd.read_parquet(filepath)
        df["num_branches"] = num_branches

        # Distinguish UPDATE (op_type=5, num_keys_touched=1) from
        # RANGE_UPDATE (op_type=5, num_keys_touched > 1)
        # Create synthetic op_type 6 for RANGE_UPDATE
        if "num_keys_touched" in df.columns:
            range_update_mask = (df["op_type"] == 5) & (
                df["num_keys_touched"] > 1
            )
            if range_update_mask.any():
                # Mark RANGE_UPDATE as op_type 6 (synthetic)
                df.loc[range_update_mask, "op_type"] = 6
                # Compute per-key latency for range updates
                df.loc[range_update_mask, "latency"] = (
                    df.loc[range_update_mask, "latency"]
                    / df.loc[range_update_mask, "num_keys_touched"]
                )

        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    # Group by num_branches and op_type, compute mean latency
    aggregated = (
        combined.groupby(["num_branches", "op_type"])
        .agg({"latency": ["mean", "std", "count"]})
        .reset_index()
    )

    # Flatten column names
    aggregated.columns = [
        "num_branches",
        "op_type",
        "latency_mean",
        "latency_std",
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


def plot_latency_by_branches(df: pd.DataFrame, output_path: str = None):
    """Create a line plot of latency vs num_branches for each operation type."""
    plt.figure(figsize=(12, 8))

    # Get unique operation types
    op_types = sorted(df["op_type"].unique())

    for op_type in op_types:
        op_data = df[df["op_type"] == op_type].sort_values("num_branches")
        op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")

        plt.errorbar(
            op_data["num_branches"],
            op_data["latency_mean"] * 1000,  # Convert to ms
            yerr=op_data["latency_std"] * 1000,
            marker="o",
            label=op_name,
            capsize=3,
        )

    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel("Average Latency (ms)", fontsize=12)
    plt.title("Nth-Op Benchmark: Latency vs Number of Branches", fontsize=14)
    plt.legend(title="Operation Type")
    plt.grid(True, alpha=0.3)
    plt.xscale(
        "log", base=2
    )  # Log scale for branch counts (typically powers of 2)

    # Add minor gridlines
    plt.grid(True, which="minor", alpha=0.1)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150)
        print(f"Saved figure to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize Nth-op benchmark results across different branch counts."
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
        help="Output file path for the figure (e.g., output.png). If not specified, displays interactively.",
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
        df = load_and_aggregate(parquet_files)
        print(f"Aggregated data:\n{df}")
        plot_latency_by_branches(df, args.output)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
