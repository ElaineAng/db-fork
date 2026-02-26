#!/usr/bin/env python3
"""Visualize single-thread benchmark results with p50 and p99 latencies.

Usage:
    python visualize_single_thread.py benchmark_8.parquet benchmark_16.parquet ...

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


# Operation type enum values to names (from task.proto)
OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH",
    2: "CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "RANGE_UPDATE (per-key)",
    7: "RANGE_READ (per-key)",
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
    7: "#17becf",
}


def auto_scale_storage(values_bytes: pd.Series) -> tuple[pd.Series, str]:
    """Pick a human-readable unit for a series of byte values.

    Returns (scaled_series, unit_label) where unit_label is one of
    'B', 'KB', 'MB', 'GB'.
    """
    max_val = values_bytes.max()
    if max_val >= 1 << 30:
        return values_bytes / (1 << 30), "GB"
    elif max_val >= 1 << 20:
        return values_bytes / (1 << 20), "MB"
    elif max_val >= 1 << 10:
        return values_bytes / (1 << 10), "KB"
    else:
        return values_bytes, "B"


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

            # Distinguish READ (op_type=3, num_keys_touched=1) from
            # RANGE_READ (op_type=3, num_keys_touched > 1)
            range_read_mask = (df["op_type"] == 3) & (
                df["num_keys_touched"] > 1
            )
            if range_read_mask.any():
                df.loc[range_read_mask, "op_type"] = 7
                df.loc[range_read_mask, "latency"] = (
                    df.loc[range_read_mask, "latency"]
                    / df.loc[range_read_mask, "num_keys_touched"]
                )

        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    # Build aggregation dict
    agg_dict = {
        "latency": [
            lambda x: np.percentile(x, 50),  # p50
            lambda x: np.percentile(x, 99),  # p99
            "count",
        ]
    }
    if "disk_size_after" in combined.columns:
        agg_dict["disk_size_after"] = "max"

    # Group by num_branches and op_type, compute percentiles
    aggregated = (
        combined.groupby(["num_branches", "op_type"])
        .agg(agg_dict)
        .reset_index()
    )

    # Flatten column names
    cols = ["num_branches", "op_type", "latency_p50", "latency_p99", "count"]
    if "disk_size_after" in combined.columns:
        cols.append("storage_max")
    aggregated.columns = cols

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
    7: "RANGE_READ (per-key)",
    8: "CONNECT_FIRST",
    9: "CONNECT_MID",
    10: "CONNECT_LAST",
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
    7: "#17becf",
    8: "#9fa123",
    9: "#bcccbf",
}


def plot_latency_percentiles(
    df: pd.DataFrame, output_path: str = None, log_scale: bool = True
):
    """Create a line plot of p50/p99 latency vs num_branches for each operation type."""
    fig = plt.figure(figsize=(14, 9))

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
        "p50 and p99 Latency vs Number of Branches",
        fontsize=14,
    )
    plt.legend(
        title="Operation Type", bbox_to_anchor=(1.05, 1), loc="upper left"
    )
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")  # Log scale for latency
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved figure to {output_path}")
    else:
        plt.show()
    plt.close(fig)


def plot_storage_by_branches(
    df: pd.DataFrame, output_path: str = None, log_scale: bool = False
):
    """Plot total DB storage vs number of branches (BRANCH_CREATE rows only)."""
    if "storage_max" not in df.columns:
        print("Warning: No storage data found. Skipping storage plot.")
        return

    storage_df = df[(df["op_type"] == 1) & (df["storage_max"] > 0)]
    if storage_df.empty:
        print("Warning: No non-zero storage data found. Skipping storage plot.")
        return

    storage_df = storage_df.sort_values("num_branches")
    scaled, unit = auto_scale_storage(storage_df["storage_max"])

    fig = plt.figure(figsize=(12, 8))
    plt.plot(
        storage_df["num_branches"],
        scaled,
        marker="o",
        color=OP_COLORS[1],
        label="Total Storage",
    )
    plt.xlabel("Number of Branches", fontsize=12)
    plt.ylabel(f"Storage ({unit})", fontsize=12)
    plt.title("Multi-Op Benchmark: Storage vs Number of Branches", fontsize=14)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xscale("log", base=2)
    if log_scale:
        plt.yscale("log")
    plt.grid(True, which="minor", alpha=0.1)
    plt.tight_layout()

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Saved figure to {output_path}")
    else:
        plt.show()
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Visualize single-thread benchmark results with p50 and p99 latencies."
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
    parser.add_argument(
        "-s",
        "--storage-output",
        type=str,
        default=None,
        help="Output file path for the storage figure.",
    )
    parser.add_argument(
        "--log-scale",
        action="store_true",
        default=False,
        help="Use log scale for y-axis (latency).",
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
        plot_latency_percentiles(df, args.output, log_scale=args.log_scale)
        if args.storage_output:
            plot_storage_by_branches(
                df, args.storage_output, log_scale=args.log_scale
            )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
