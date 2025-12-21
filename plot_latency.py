#!/usr/bin/env python3
"""
Plot mean latency with 95% confidence intervals per key for each operation type.

For range updates and other multi-key operations, latency is divided by num_keys_touched
to get the per-key latency.
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats


# OpType enum mapping from result.proto
OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH_CREATE",
    2: "BRANCH_CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "COMMIT",
}


def load_benchmark_data(parquet_path: str) -> pd.DataFrame:
    """Load benchmark results from parquet file."""
    df = pd.read_parquet(parquet_path)
    # Map op_type enum to string names
    df["op_type"] = (
        df["op_type"].map(OP_TYPE_NAMES).fillna(df["op_type"].astype(str))
    )
    # Separate UPDATE into UPDATE (single key) and RANGE_UPDATE (multiple keys)
    df.loc[
        (df["op_type"] == "UPDATE") & (df["num_keys_touched"] > 1),
        "op_type",
    ] = "RANGE_UPDATE"
    return df


def calculate_per_key_latency(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate per-key latency by dividing latency by num_keys_touched."""
    df = df.copy()
    # Convert latency from seconds to milliseconds
    df["latency_ms"] = df["latency"] * 1000
    # Avoid division by zero - treat 0 keys as 1 for per-key calculation
    df["num_keys"] = df["num_keys_touched"].replace(0, 1)
    df["per_key_latency"] = df["latency_ms"] / df["num_keys"]
    return df


def calculate_ci95_by_operation(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate mean and 95% confidence interval for each operation type."""

    def ci95(x):
        n = len(x)
        if n < 2:
            return 0
        se = stats.sem(x)  # Standard error of the mean
        ci = se * stats.t.ppf(0.975, n - 1)  # 95% CI using t-distribution
        return ci

    ci_stats = (
        df.groupby("op_type")["per_key_latency"]
        .agg(["mean", "std", "count", ci95])
        .reset_index()
        .rename(columns={"mean": "mean_latency", "ci95": "ci_95"})
    )
    return ci_stats


def plot_latency_with_ci(ci_stats: pd.DataFrame, output_path: str = None):
    """Create a bar chart of mean latency with 95% CI error bars."""
    # Sort by latency for better visualization
    ci_stats = ci_stats.sort_values("mean_latency", ascending=False)

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(ci_stats)))

    bars = ax.bar(
        ci_stats["op_type"],
        ci_stats["mean_latency"],
        yerr=ci_stats["ci_95"],
        color=colors,
        capsize=5,
        error_kw={"elinewidth": 2, "capthick": 2},
    )

    # Add value labels on bars
    for bar, mean, ci in zip(bars, ci_stats["mean_latency"], ci_stats["ci_95"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + ci + 0.02 * max(ci_stats["mean_latency"]),
            f"{mean:.3f}±{ci:.3f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_ylabel("Mean Latency per Key (ms) with 95% CI", fontsize=12)
    ax.set_xlabel("Operation Type", fontsize=12)
    ax.set_title(
        "Mean Latency per Key with 95% Confidence Interval",
        fontsize=14,
        fontweight="bold",
    )

    # Rotate x-axis labels for readability
    plt.xticks(rotation=45, ha="right")

    # Add grid for readability
    ax.yaxis.grid(True, linestyle="--", alpha=0.7)
    ax.set_axisbelow(True)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Plot saved to {output_path}")
    else:
        plt.show()

    return fig


def main():
    parser = argparse.ArgumentParser(
        description="Plot mean latency with 95% CI from benchmark parquet files"
    )
    parser.add_argument(
        "parquet_file",
        type=str,
        help="Path to the benchmark parquet file",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output path for the plot (PNG). If not specified, displays plot.",
    )
    parser.add_argument(
        "--show-stats",
        action="store_true",
        help="Print detailed statistics to console",
    )

    args = parser.parse_args()

    # Load data
    print(f"Loading data from {args.parquet_file}...")
    df = load_benchmark_data(args.parquet_file)
    print(f"Loaded {len(df)} records")

    # Calculate per-key latency
    df = calculate_per_key_latency(df)

    # Calculate 95% CI stats
    ci_stats = calculate_ci95_by_operation(df)

    if args.show_stats:
        print("\n=== Mean Latency with 95% CI by Operation ===")
        print(ci_stats.to_string(index=False))

        print("\n=== Summary Statistics ===")
        summary = (
            df.groupby("op_type")
            .agg(
                count=("per_key_latency", "count"),
                mean=("per_key_latency", "mean"),
                std=("per_key_latency", "std"),
                median=("per_key_latency", "median"),
                min=("per_key_latency", "min"),
                max=("per_key_latency", "max"),
            )
            .reset_index()
        )
        print(summary.to_string(index=False))

    # Plot
    plot_latency_with_ci(ci_stats, args.output)


if __name__ == "__main__":
    main()
