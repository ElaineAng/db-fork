#!/usr/bin/env python3
"""Visualize latency over iterations for different operation types from a parquet file."""

import argparse
import pandas as pd
import matplotlib.pyplot as plt


# OpType values from result.proto
OP_TYPE_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH_CREATE",
    2: "BRANCH_CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "COMMIT",
    # Derived types (not in proto, computed from num_keys_touched)
    "RANGE_READ": "RANGE_READ",
    "RANGE_UPDATE": "RANGE_UPDATE",
}


def classify_op_type(row):
    """Classify operation type, detecting RANGE_READ/RANGE_UPDATE from num_keys_touched.

    Returns:
        - "RANGE_READ" for READ (3) with num_keys_touched > 1
        - "RANGE_UPDATE" for UPDATE (5) with num_keys_touched > 1
        - Original op_type (int) for all other cases, including single-key operations
    """
    op_type = row["op_type"]
    num_keys = row.get("num_keys_touched", 1)

    # RANGE_READ: READ (3) with num_keys_touched > 1
    if op_type == 3 and num_keys > 1:
        return "RANGE_READ"
    # RANGE_UPDATE: UPDATE (5) with num_keys_touched > 1
    if op_type == 5 and num_keys > 1:
        return "RANGE_UPDATE"

    return op_type


def main():
    parser = argparse.ArgumentParser(
        description="Plot latency vs iteration number for different operation types",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operation types (from result.proto):
  0: UNSPECIFIED
  1: BRANCH_CREATE
  2: BRANCH_CONNECT
  3: READ
  4: INSERT
  5: UPDATE
  6: COMMIT
  
  RANGE_READ: READ with num_keys_touched > 1
  RANGE_UPDATE: UPDATE with num_keys_touched > 1

Examples:
  python visualize_latency.py results.parquet --ops 1 3 5
  python visualize_latency.py results.parquet --ops RANGE_READ RANGE_UPDATE
  python visualize_latency.py results.parquet --ops 3 RANGE_READ --output plot.png
""",
    )
    parser.add_argument("parquet_file", help="Path to the parquet file")
    parser.add_argument(
        "--ops",
        nargs="+",
        required=True,
        help="List of operation types to plot (numeric codes or RANGE_READ/RANGE_UPDATE)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file path (if not specified, displays plot)",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="Latency vs Iteration Number",
        help="Plot title",
    )

    args = parser.parse_args()

    # Parse operation types (can be int or string like RANGE_READ)
    ops_to_plot = []
    for op in args.ops:
        try:
            ops_to_plot.append(int(op))
        except ValueError:
            ops_to_plot.append(op.upper())

    # Read the parquet file
    df = pd.read_parquet(args.parquet_file)

    # Add classified op_type column
    df["op_type_classified"] = df.apply(classify_op_type, axis=1)

    plt.figure(figsize=(12, 6))

    for op_type in ops_to_plot:
        # Filter for this operation type
        op_df = df[df["op_type_classified"] == op_type]

        if op_df.empty:
            print(f"Warning: No data found for op_type {op_type}")
            continue

        # Group by iteration_number and average latency
        grouped = (
            op_df.groupby("iteration_number")["latency"].mean().reset_index()
        )

        # Get operation name for legend
        if isinstance(op_type, int):
            op_name = OP_TYPE_NAMES.get(op_type, f"OP_{op_type}")
        else:
            op_name = op_type

        # Plot (convert latency from seconds to milliseconds)
        latency_ms = grouped["latency"] * 1000

        # Plot raw data with thin line
        plt.plot(
            grouped["iteration_number"],
            latency_ms,
            label=op_name,
            linewidth=0.3,
        )

        # Add rolling average trend line (semi-transparent)
        window_size = max(10, len(grouped) // 20)  # Adaptive window
        trend = latency_ms.rolling(window=window_size, center=True).mean()
        plt.plot(
            grouped["iteration_number"],
            trend,
            label=f"{op_name} (trend)",
            linewidth=2,
            alpha=0.5,
        )

    plt.xlabel("Iteration Number")
    plt.ylabel("Latency (ms)")
    plt.title(args.title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if args.output:
        plt.savefig(args.output, dpi=150)
        print(f"Saved plot to {args.output}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
