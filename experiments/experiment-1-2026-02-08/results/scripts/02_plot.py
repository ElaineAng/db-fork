#!/usr/bin/env python3
"""Generate figures for Experiment 1: Branch Creation Storage Overhead.

Produces three figures:
  fig1: Marginal storage delta per branch (N=1024 trajectory)
  fig2: Cumulative storage vs number of branches
  fig3: Mean marginal delta vs number of branches

Usage:
    python 02_plot.py [--data-dir ../data] [--output-dir ../figures]
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
def parse_filename(filepath: str) -> dict:
    stem = Path(filepath).stem
    m = re.match(r"^(dolt|file_copy)_tpcc_(\d+)_(spine|bushy|fan_out)_branch_setup$", stem)
    if not m:
        return None
    return {"backend": m.group(1), "N": int(m.group(2)), "topology": m.group(3)}


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*_branch_setup.parquet")))
    dfs = []
    for f in files:
        meta = parse_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        df["rep_id"] = (df["iteration_number"] == 0).cumsum() - 1
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Plot config
# ---------------------------------------------------------------------------
BACKEND_LABELS = {"dolt": "Dolt", "file_copy": "file_copy (PostgreSQL CoW)"}
BACKENDS = ["dolt", "file_copy"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_COLORS = {"spine": "#d62728", "bushy": "#2ca02c", "fan_out": "#1f77b4"}
TOPO_LABELS = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}


def fmt_bytes_axis(val, _pos):
    """Formatter for byte-valued axes."""
    if abs(val) >= 1 << 30:
        return f"{val / (1 << 30):.1f} GB"
    elif abs(val) >= 1 << 20:
        return f"{val / (1 << 20):.1f} MB"
    elif abs(val) >= 1 << 10:
        return f"{val / (1 << 10):.0f} KB"
    return f"{val:.0f} B"


# ---------------------------------------------------------------------------
# Figure 1: Marginal storage delta per branch at N=1024
# ---------------------------------------------------------------------------
def plot_fig1(df: pd.DataFrame, output_dir: str):
    target_n = df["N"].max()
    sub = df[df["N"] == target_n].copy()
    sub["storage_delta"] = sub["disk_size_after"] - sub["disk_size_before"]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)

    for ax, backend in zip(axes, BACKENDS):
        bsub = sub[sub["backend"] == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub["topology"] == topo]
            # Average across reps per iteration_number
            agg = tsub.groupby("iteration_number")["storage_delta"].agg(["mean", "std"]).reset_index()
            x = agg["iteration_number"].values + 1  # 1-indexed branch number
            y = agg["mean"].values
            std = agg["std"].values

            ax.plot(x, y, color=TOPO_COLORS[topo], label=TOPO_LABELS[topo], linewidth=0.8, alpha=0.9)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Branch index (nth branch created)")
        ax.set_ylabel("Marginal storage delta (bytes)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"Marginal Storage Delta per Branch (N={target_n})", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig1_marginal_storage_by_topology.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Figure 2: Cumulative storage vs number of branches
# ---------------------------------------------------------------------------
def plot_fig2(df: pd.DataFrame, output_dir: str):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)

    for ax, backend in zip(axes, BACKENDS):
        bsub = df[df["backend"] == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub["topology"] == topo]
            # For each N, get the final disk_size_after (last iteration per rep), then average
            final = tsub.groupby(["N", "rep_id"]).apply(
                lambda g: g.iloc[-1]["disk_size_after"], include_groups=False
            ).reset_index(name="final_storage")
            agg = final.groupby("N")["final_storage"].agg(["mean", "std"]).reset_index()
            agg = agg.sort_values("N")

            x = agg["N"].values
            y = agg["mean"].values
            std = agg["std"].values

            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Total storage after N branches")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Cumulative Storage vs Number of Branches", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2_cumulative_storage_vs_branches.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Figure 3: Mean marginal delta vs N
# ---------------------------------------------------------------------------
def plot_fig3(df: pd.DataFrame, output_dir: str):
    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)

    for ax, backend in zip(axes, BACKENDS):
        bsub = df[df["backend"] == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub["topology"] == topo]
            agg = tsub.groupby("N")["storage_delta"].agg(["mean", "std"]).reset_index()
            agg = agg.sort_values("N")

            x = agg["N"].values
            y = agg["mean"].values
            std = agg["std"].values

            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Mean marginal storage delta per branch")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Mean Marginal Storage Delta vs Branch Count", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig3_mean_delta_vs_branches.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 1 plots")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "figures"))
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading data from: {data_dir}")
    df = load_all(data_dir)
    print(f"Loaded {len(df)} rows")
    print()

    plot_fig1(df, output_dir)
    plot_fig2(df, output_dir)
    plot_fig3(df, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
