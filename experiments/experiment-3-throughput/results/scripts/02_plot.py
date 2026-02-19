#!/usr/bin/env python3
"""Generate figures for Experiment 3: Operation Throughput Under Branching.

Produces:
  fig3a: Branch creation throughput vs thread count (by backend, series per topology)
  fig3b: Aggregate CRUD goodput vs thread count (by backend, with ideal-linear ref)
  fig3c: Per-thread throughput distribution at max T (box plot per topology)
  fig3d: Per-thread goodput vs thread_id at max T (scatter, spine only)

Usage:
    python 02_plot.py [--data-dir ../data] [--output-dir ../figures] [--duration 30]
"""

import argparse
import glob
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Data loading (shared with 01_analyze.py)
# ---------------------------------------------------------------------------
def parse_throughput_filename(filepath: str) -> dict | None:
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None
    m = re.match(
        r"^(dolt|file_copy|neon)_\w+_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_throughput$",
        stem,
    )
    if not m:
        return None
    return {
        "backend": m.group(1),
        "topology": m.group(2),
        "T": int(m.group(3)),
        "mode": m.group(4),
    }


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []
    for f in files:
        meta = parse_throughput_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["topology"] = meta["topology"]
        df["T"] = meta["T"]
        df["mode"] = meta["mode"]
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def compute_goodput(df: pd.DataFrame, duration: float) -> pd.DataFrame:
    grouped = (
        df.groupby(["backend", "topology", "T", "mode", "thread_id"])
        .size()
        .reset_index(name="ops_count")
    )
    grouped["goodput_ops_sec"] = grouped["ops_count"] / duration
    return grouped


def compute_aggregate(per_thread: pd.DataFrame) -> pd.DataFrame:
    return (
        per_thread
        .groupby(["backend", "topology", "T", "mode"])
        .agg(
            aggregate_goodput=("goodput_ops_sec", "sum"),
            mean_per_thread=("goodput_ops_sec", "mean"),
            std_per_thread=("goodput_ops_sec", "std"),
        )
        .reset_index()
    )


# ---------------------------------------------------------------------------
# Plot config
# ---------------------------------------------------------------------------
BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL CoW)", "neon": "Neon"}
BACKENDS = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_COLORS = {"spine": "#d62728", "bushy": "#2ca02c", "fan_out": "#1f77b4"}
TOPO_LABELS = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}


# ---------------------------------------------------------------------------
# Fig 3a: Branch creation throughput vs thread count
# ---------------------------------------------------------------------------
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
            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=5)
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


# ---------------------------------------------------------------------------
# Fig 3b: Aggregate CRUD goodput vs thread count (with ideal-linear ref)
# ---------------------------------------------------------------------------
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

        # Ideal-linear reference from T=1 mean across topologies
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
            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=5)

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


# ---------------------------------------------------------------------------
# Fig 3c: Per-thread throughput distribution at max T (box plot)
# ---------------------------------------------------------------------------
def plot_fig3c(per_thread: pd.DataFrame, output_dir: str):
    crud = per_thread[per_thread["mode"] == "crud"]
    if crud.empty:
        print("Skipping fig3c: no CRUD data")
        return

    backends_present = [b for b in BACKENDS if b in crud["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]

    for ax, backend in zip(axes, backends_present):
        bsub = crud[crud.backend == backend]
        max_t = bsub.T.max()
        at_max = bsub[bsub.T == max_t]

        box_data = []
        labels = []
        colors = []
        for topo in TOPO_ORDER:
            tsub = at_max[at_max.topology == topo]
            if tsub.empty:
                continue
            box_data.append(tsub["goodput_ops_sec"].values)
            labels.append(TOPO_LABELS[topo])
            colors.append(TOPO_COLORS[topo])

        if box_data:
            bp = ax.boxplot(box_data, labels=labels, patch_artist=True, widths=0.5)
            for patch, color in zip(bp["boxes"], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.4)

        ax.set_title(f"{BACKEND_LABELS[backend]} (T={max_t})", fontsize=12)
        ax.set_ylabel("Per-thread goodput (ops/s)")
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Exp 3b: Per-Thread CRUD Goodput Distribution at Max T",
                 fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3c_per_thread_distribution.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 3d: Per-thread goodput vs thread_id (spine only, scatter)
# ---------------------------------------------------------------------------
def plot_fig3d(per_thread: pd.DataFrame, output_dir: str):
    crud_spine = per_thread[
        (per_thread["mode"] == "crud") & (per_thread["topology"] == "spine")
    ]
    if crud_spine.empty:
        print("Skipping fig3d: no spine CRUD data")
        return

    backends_present = [b for b in BACKENDS if b in crud_spine["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]

    for ax, backend in zip(axes, backends_present):
        bsub = crud_spine[crud_spine.backend == backend]
        max_t = bsub.T.max()
        at_max = bsub[bsub.T == max_t]

        ax.scatter(at_max.thread_id, at_max.goodput_ops_sec,
                   alpha=0.6, s=20, color=TOPO_COLORS["spine"])
        ax.axhline(at_max.goodput_ops_sec.mean(), color="gray",
                   linestyle="--", alpha=0.5, label="Mean")

        ax.set_title(f"{BACKEND_LABELS[backend]} — Spine (T={max_t})", fontsize=12)
        ax.set_xlabel("Thread ID (branch index)")
        ax.set_ylabel("Per-thread goodput (ops/s)")
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Exp 3b: Per-Thread Goodput vs Branch Index (Spine)",
                 fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3d_per_thread_vs_index_spine.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 3 plots")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "figures"))
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Duration of each throughput run in seconds (default: 30)")
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
    plot_fig3c(per_thread, output_dir)
    plot_fig3d(per_thread, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
