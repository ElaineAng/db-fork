#!/usr/bin/env python3
"""Generate figures for Experiment 1: Branch Creation Storage Overhead.

Produces two figures:
  fig1: Marginal storage delta per branch (per-backend max N trajectory)
  fig5: file_copy delta volatility by topology (scatter + rolling std)

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
    m = re.match(r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_branch_setup$", stem)
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
BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy (PostgreSQL CoW)",
    "neon": "Neon",
    "xata": "Xata",
}
BACKENDS = ["dolt", "file_copy", "neon", "xata"]
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
    df = df.copy()
    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
    # Exclude Xata rows where the metrics API returned no data (disk_size == 0).
    # Other backends can legitimately have disk_size_before=0 (e.g. Dolt N=1).
    df = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]

    def max_common_n(bdf: pd.DataFrame, topologies: list[str]) -> int | None:
        common = None
        for topo in topologies:
            ns = set(bdf[bdf["topology"] == topo]["N"].unique())
            common = ns if common is None else (common & ns)
        if not common:
            return None
        return int(max(common))

    backends_present = [b for b in BACKENDS if b in df["backend"].unique()]

    # Default: one panel at backend max N.
    # Xata: use comparable panel at max common N across all topologies (prefer N=8 here)
    # instead of partial-coverage max N.
    panel_specs = []
    for backend in backends_present:
        bdf = df[df["backend"] == backend]
        if backend == "xata":
            common_n = max_common_n(bdf, TOPO_ORDER)
            target_n = int(common_n) if common_n is not None else int(bdf["N"].max())
            if common_n is not None:
                panel_specs.append((backend, target_n, f"N={target_n}, all topologies"))
            else:
                panel_specs.append((backend, target_n, f"N={target_n}"))
        else:
            backend_max_n = int(bdf["N"].max())
            panel_specs.append((backend, backend_max_n, f"N={backend_max_n}"))

    ncols = len(panel_specs)
    fig, axes = plt.subplots(1, ncols, figsize=(6.2 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]

    for ax, (backend, target_n, title_suffix) in zip(axes, panel_specs):
        bsub = df[(df["backend"] == backend) & (df["N"] == target_n)]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub["topology"] == topo]
            if tsub.empty:
                continue
            # Average across reps per iteration_number
            agg = tsub.groupby("iteration_number")["storage_delta"].agg(["mean", "std"]).reset_index()
            x = agg["iteration_number"].values + 1  # 1-indexed branch number
            y = agg["mean"].values
            std = agg["std"].values

            ax.plot(x, y, color=TOPO_COLORS[topo], label=TOPO_LABELS[topo], linewidth=0.8, alpha=0.9)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]} ({title_suffix})", fontsize=12)
        ax.set_xlabel("Branch index (nth branch created)")
        ax.set_ylabel("Marginal storage delta (bytes)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Marginal Storage Delta per Branch", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig1_marginal_storage_by_topology.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Figure 5: file_copy delta volatility by topology (scatter + rolling std)
# ---------------------------------------------------------------------------
def plot_fig5(df: pd.DataFrame, output_dir: str):
    target_n = df["N"].max()
    sub = df[(df["backend"] == "file_copy") & (df["N"] == target_n)].copy()
    if sub.empty:
        print("Skipping fig5: no file_copy data at max N")
        return
    sub["storage_delta"] = sub["disk_size_after"] - sub["disk_size_before"]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), sharey=True)
    window = 50  # rolling window for std

    for ax, topo in zip(axes, TOPO_ORDER):
        tsub = sub[sub["topology"] == topo]
        if tsub.empty:
            ax.set_visible(False)
            continue
        color = TOPO_COLORS[topo]

        # Scatter all individual data points (each rep)
        for rep in sorted(tsub["rep_id"].unique()):
            rsub = tsub[tsub["rep_id"] == rep]
            x = rsub["iteration_number"].values + 1
            y = rsub["storage_delta"].values
            ax.scatter(x, y, s=1.5, alpha=0.35, color=color, rasterized=True)

        # Rolling mean + std across all reps (sorted by iteration_number)
        agg = tsub.groupby("iteration_number")["storage_delta"].agg(
            ["mean", "std"]
        ).reset_index()
        agg = agg.sort_values("iteration_number")
        x = agg["iteration_number"].values + 1
        rolling_mean = pd.Series(agg["mean"].values).rolling(window, min_periods=1, center=True).mean().values
        rolling_std = pd.Series(agg["std"].values).rolling(window, min_periods=1, center=True).mean().values

        ax.plot(x, rolling_mean, color=color, linewidth=1.5, label=f"Rolling mean (w={window})")
        ax.fill_between(x, rolling_mean - rolling_std, rolling_mean + rolling_std,
                        color=color, alpha=0.2, label=f"±1 rolling std")

        # Zero line
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")

        ax.set_title(f"{TOPO_LABELS[topo]}", fontsize=12)
        ax.set_xlabel("Branch index (nth branch created)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=8, loc="upper left")
        ax.grid(True, alpha=0.3)

    axes[0].set_ylabel("Storage delta per branch creation")
    fig.suptitle(f"file_copy Storage Delta Volatility by Topology (N={target_n})",
                 fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig5_file_copy_delta_volatility.png")
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
    plot_fig5(df, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
