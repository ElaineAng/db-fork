#!/usr/bin/env python3
"""Generate figures for Experiment 3 throughput and failure rates."""

from __future__ import annotations

import argparse
import glob
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


NEW_NAME_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_[a-zA-Z0-9]+$"
)
LEGACY_NAME_RE = re.compile(
    r"^(dolt|file_copy|neon)_\w+_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_throughput$"
)

BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy (PostgreSQL CoW)",
    "neon": "Neon",
}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_COLORS = {"spine": "#d62728", "bushy": "#2ca02c", "fan_out": "#1f77b4"}
TOPO_LABELS = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}

FAILURE_CATEGORY_COLORS = {
    "FAILURE_LOCK_CONTENTION": "#8c564b",
    "FAILURE_TIMEOUT": "#ff7f0e",
    "FAILURE_RESOURCE_LIMIT": "#e377c2",
    "FAILURE_CONNECTION": "#17becf",
    "FAILURE_BACKEND_STATE_CONFLICT": "#9467bd",
    "FAILURE_CONSTRAINT_OR_DATA": "#bcbd22",
    "FAILURE_INTERNAL_BUG": "#7f7f7f",
    "FAILURE_UNKNOWN": "#d62728",
}

CATEGORY_NAMES = {
    0: "FAILURE_NONE",
    1: "FAILURE_LOCK_CONTENTION",
    2: "FAILURE_TIMEOUT",
    3: "FAILURE_RESOURCE_LIMIT",
    4: "FAILURE_CONNECTION",
    5: "FAILURE_BACKEND_STATE_CONFLICT",
    6: "FAILURE_CONSTRAINT_OR_DATA",
    7: "FAILURE_INTERNAL_BUG",
    8: "FAILURE_UNKNOWN",
}


def parse_run_stem(stem: str) -> dict | None:
    if stem.endswith("_setup"):
        return None

    match = NEW_NAME_RE.match(stem)
    if not match:
        match = LEGACY_NAME_RE.match(stem)
    if not match:
        return None

    return {
        "backend": match.group(1),
        "topology": match.group(2),
        "T": int(match.group(3)),
        "mode": match.group(4),
    }


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []

    for path in files:
        meta = parse_run_stem(Path(path).stem)
        if meta is None:
            continue

        df = pd.read_parquet(path)
        df["backend"] = meta["backend"]
        df["topology"] = meta["topology"]
        df["T"] = meta["T"]
        df["mode"] = meta["mode"]

        if "outcome_success" not in df.columns:
            df["outcome_success"] = True
        df["outcome_success"] = df["outcome_success"].fillna(False).astype(bool)

        if "failure_category" not in df.columns:
            df["failure_category"] = 0
        df["failure_category"] = df["failure_category"].fillna(0).astype(int)

        dfs.append(df)

    if not dfs:
        raise RuntimeError(f"No throughput parquet files found in {data_dir}")

    return pd.concat(dfs, ignore_index=True)


def compute_goodput(df: pd.DataFrame, duration: float) -> pd.DataFrame:
    success_df = df[df["outcome_success"]]
    grouped = (
        success_df.groupby(["backend", "topology", "T", "mode", "thread_id"])
        .size()
        .reset_index(name="ops_count")
    )
    grouped["goodput_ops_sec"] = grouped["ops_count"] / duration
    return grouped


def compute_aggregate(per_thread: pd.DataFrame) -> pd.DataFrame:
    return (
        per_thread.groupby(["backend", "topology", "T", "mode"])
        .agg(
            aggregate_goodput=("goodput_ops_sec", "sum"),
            mean_per_thread=("goodput_ops_sec", "mean"),
            std_per_thread=("goodput_ops_sec", "std"),
        )
        .reset_index()
    )


def plot_fig3a(agg: pd.DataFrame, output_dir: str):
    branch = agg[agg["mode"] == "branch"]
    if branch.empty:
        print("Skipping fig3a: no branch data")
        return

    backends_present = [b for b in BACKENDS if b in branch["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]

    for ax, backend in zip(axes, backends_present):
        bsub = branch[branch.backend == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo].sort_values("T")
            if tsub.empty:
                continue
            x = tsub["T"].values
            y = tsub["aggregate_goodput"].values
            std = tsub["std_per_thread"].fillna(0).values * np.sqrt(x)
            ax.plot(
                x,
                y,
                "o-",
                color=TOPO_COLORS[topo],
                label=TOPO_LABELS[topo],
                linewidth=1.5,
                markersize=5,
            )
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.12)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of threads (T)")
        ax.set_ylabel("Branch creation throughput (ops/s)")
        ax.set_xscale("log", base=2)
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Exp 3a: Branch Creation Throughput vs Threads", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3a_branch_throughput.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_fig3b(agg: pd.DataFrame, output_dir: str):
    crud = agg[agg["mode"] == "crud"]
    if crud.empty:
        print("Skipping fig3b: no CRUD data")
        return

    backends_present = [b for b in BACKENDS if b in crud["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]

    for ax, backend in zip(axes, backends_present):
        bsub = crud[crud.backend == backend]

        t1_mean = bsub[bsub.T == 1]["aggregate_goodput"].mean()
        if t1_mean > 0:
            ref_x = sorted(bsub["T"].unique())
            ref_y = [t1_mean * t for t in ref_x]
            ax.plot(ref_x, ref_y, "k--", alpha=0.3, label="Ideal linear", linewidth=1)

        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo].sort_values("T")
            if tsub.empty:
                continue
            x = tsub["T"].values
            y = tsub["aggregate_goodput"].values
            ax.plot(
                x,
                y,
                "o-",
                color=TOPO_COLORS[topo],
                label=TOPO_LABELS[topo],
                linewidth=1.5,
                markersize=5,
            )

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of threads / branches (N)")
        ax.set_ylabel("Aggregate CRUD goodput (ops/s)")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Exp 3b: Aggregate CRUD Goodput vs Branch Count", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3b_crud_aggregate_goodput.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def plot_failure_rates(df: pd.DataFrame, output_dir: str):
    if "outcome_success" not in df.columns or "failure_category" not in df.columns:
        print("Skipping fig3e: outcome columns are missing")
        return

    attempted = (
        df.groupby(["backend", "T"]).size().rename("attempted").reset_index()
    )

    failures = df[~df["outcome_success"]].copy()
    if failures.empty:
        print("Skipping fig3e: no failures found")
        return

    failures["failure_name"] = failures["failure_category"].map(CATEGORY_NAMES)
    failures = failures[failures["failure_name"] != "FAILURE_NONE"]
    if failures.empty:
        print("Skipping fig3e: no non-NONE failure categories")
        return

    fail_counts = (
        failures.groupby(["backend", "T", "failure_name"])\
        .size()\
        .rename("count")\
        .reset_index()
    )
    fail_counts = fail_counts.merge(attempted, on=["backend", "T"], how="left")
    fail_counts["rate"] = fail_counts["count"] / fail_counts["attempted"]

    categories = [
        c
        for c in FAILURE_CATEGORY_COLORS
        if c in set(fail_counts["failure_name"].unique())
    ]
    backends_present = [b for b in BACKENDS if b in fail_counts["backend"].unique()]

    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=True)
    if ncols == 1:
        axes = [axes]

    for ax, backend in zip(axes, backends_present):
        bsub = fail_counts[fail_counts["backend"] == backend]
        x_values = sorted(bsub["T"].unique())
        bottom = np.zeros(len(x_values), dtype=float)

        for category in categories:
            csub = bsub[bsub["failure_name"] == category]
            rates_by_t = {
                int(row["T"]): float(row.rate)
                for _, row in csub.iterrows()
            }
            heights = np.array([rates_by_t.get(t, 0.0) for t in x_values], dtype=float)
            ax.bar(
                x_values,
                heights,
                bottom=bottom,
                color=FAILURE_CATEGORY_COLORS[category],
                label=category.replace("FAILURE_", ""),
            )
            bottom += heights

        ax.set_title(f"{BACKEND_LABELS[backend]}")
        ax.set_xlabel("Threads (T)")
        ax.set_ylabel("Failure rate")
        ax.set_xscale("log", base=2)
        ax.set_ylim(0, 1)
        ax.grid(True, axis="y", alpha=0.3)
        ax.legend(fontsize=8)

    fig.suptitle("Failure Rate by Thread Count (Stacked Categories)", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3e_failure_rate_stacked.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Experiment 3 plots")
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "figures"),
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Duration of each throughput run in seconds (default: 30)",
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading data from: {data_dir}")
    df = load_all(data_dir)
    print(f"Loaded {len(df)} rows")
    print()

    per_thread = compute_goodput(df, args.duration)
    agg = compute_aggregate(per_thread)

    plot_fig3a(agg, output_dir)
    plot_fig3b(agg, output_dir)
    plot_failure_rates(df, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
