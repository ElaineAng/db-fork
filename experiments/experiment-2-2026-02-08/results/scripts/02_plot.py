#!/usr/bin/env python3
"""Generate figures for Experiment 2: Per-Operation Storage Overhead.

Produces:
  fig2a: UPDATE storage delta vs branch count (by backend × topology)
  fig2b: RANGE_UPDATE(r=20) storage delta vs branch count
  fig2c: Per-key delta normalization vs range size
  fig2d: Operation latency vs branch count

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
# Data loading
# ---------------------------------------------------------------------------
def parse_measurement_filename(filepath: str) -> dict | None:
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None
    m = re.match(
        r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_(update|range_update)(?:_r(\d+))?$",
        stem,
    )
    if not m:
        return None
    return {
        "backend": m.group(1),
        "N": int(m.group(2)),
        "topology": m.group(3),
        "operation": m.group(4).upper(),
        "range_size": int(m.group(5)) if m.group(5) else None,
    }


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs = []
    for f in files:
        meta = parse_measurement_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        df["operation"] = meta["operation"]
        df["range_size"] = meta["range_size"]
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        dfs.append(df)
    out = pd.concat(dfs, ignore_index=True)
    out["is_xata_invalid"] = (
        (out["backend"] == "xata")
        & ((out["disk_size_before"] == 0) | (out["disk_size_after"] == 0))
    )
    # Keep non-Xata rows unchanged; drop Xata rows with missing disk metrics.
    return out[~out["is_xata_invalid"]].copy()


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
N_VALUES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]


def fmt_bytes_axis(val, _pos):
    if abs(val) >= 1 << 30:
        return f"{val / (1 << 30):.1f} GB"
    elif abs(val) >= 1 << 20:
        return f"{val / (1 << 20):.1f} MB"
    elif abs(val) >= 1 << 10:
        return f"{val / (1 << 10):.0f} KB"
    return f"{val:.0f} B"


# ---------------------------------------------------------------------------
# Fig 2a: UPDATE storage delta vs branch count
# ---------------------------------------------------------------------------
def plot_fig2a(df: pd.DataFrame, output_dir: str):
    updates = df[df.operation == "UPDATE"]

    backends_present = [b for b in BACKENDS if b in updates["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]
    for ax, backend in zip(axes, backends_present):
        bsub = updates[updates.backend == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                continue
            agg = tsub.groupby("N")["storage_delta"].agg(["mean", "std"]).reset_index()
            agg = agg.sort_values("N")

            x = agg["N"].values
            y = agg["mean"].values
            std = agg["std"].fillna(0).values

            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Mean per-UPDATE storage delta (bytes)")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Point UPDATE: Storage Delta vs Branch Count", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2a_update_storage_delta.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2b: RANGE_UPDATE(r=20) storage delta vs branch count
# ---------------------------------------------------------------------------
def plot_fig2b(df: pd.DataFrame, output_dir: str):
    ru = df[(df.operation == "RANGE_UPDATE") & (df.range_size == 20)]

    backends_present = [b for b in BACKENDS if b in ru["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]
    for ax, backend in zip(axes, backends_present):
        bsub = ru[ru.backend == backend]
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                continue
            agg = tsub.groupby("N")["storage_delta"].agg(["mean", "std"]).reset_index()
            agg = agg.sort_values("N")

            x = agg["N"].values
            y = agg["mean"].values
            std = agg["std"].fillna(0).values

            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                    linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Mean per-RANGE_UPDATE storage delta (bytes)")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("RANGE_UPDATE (r=20): Storage Delta vs Branch Count", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2b_range_update_storage_delta.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2c: Per-key overhead vs range size (spine, selected N values)
# ---------------------------------------------------------------------------
def plot_fig2c(df: pd.DataFrame, output_dir: str):
    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")].copy()
    ru_spine["per_key_delta"] = ru_spine["storage_delta"] / ru_spine["num_keys_touched"]

    backends_present = [b for b in BACKENDS if b in ru_spine["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(7 * ncols, 5), sharey=False)
    if ncols == 1:
        axes = [axes]
    key_ns = [1, 8, 64, 256, 1024]
    cmap = plt.cm.viridis(np.linspace(0.1, 0.9, len(key_ns)))

    for ax, backend in zip(axes, backends_present):
        bsub = ru_spine[ru_spine.backend == backend]
        for i, n in enumerate(key_ns):
            nsub = bsub[bsub.N == n]
            if nsub.empty:
                continue
            agg = nsub.groupby("range_size")["per_key_delta"].mean().reset_index()
            agg = agg.sort_values("range_size")

            ax.plot(agg.range_size, agg.per_key_delta, "o-",
                    color=cmap[i], label=f"N={n}", linewidth=1.5, markersize=5)

        ax.set_title(f"{BACKEND_LABELS[backend]}", fontsize=12)
        ax.set_xlabel("Range size (keys per RANGE_UPDATE)")
        ax.set_ylabel("Per-key storage delta (bytes)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle("Per-Key Storage Delta vs Range Size (Spine topology)", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2c_per_key_delta_vs_range_size.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2d: Operation latency vs branch count
# ---------------------------------------------------------------------------
def plot_fig2d(df: pd.DataFrame, output_dir: str):
    backends_present = [b for b in BACKENDS if b in df["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(2, ncols, figsize=(7 * ncols, 10), sharey=False)
    if ncols == 1:
        axes = axes.reshape(2, 1)

    for col, backend in enumerate(backends_present):
        for row, op in enumerate(["UPDATE", "RANGE_UPDATE"]):
            ax = axes[row, col]
            sub = df[(df.backend == backend) & (df.operation == op)]
            if op == "RANGE_UPDATE":
                sub = sub[sub.range_size == 20]

            for topo in TOPO_ORDER:
                tsub = sub[sub.topology == topo]
                agg = tsub.groupby("N")["latency"].agg(["mean", "std"]).reset_index()
                agg = agg.sort_values("N")

                x = agg["N"].values
                y = agg["mean"].values * 1000  # to ms
                std = agg["std"].fillna(0).values * 1000

                ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo],
                        linewidth=1.5, markersize=4)
                ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

            ax.set_title(f"{BACKEND_LABELS[backend]} — {op}", fontsize=11)
            ax.set_xlabel("Number of branches (N)")
            ax.set_ylabel("Mean latency (ms)")
            ax.set_xscale("log", base=2)
            ax.legend(frameon=True, fontsize=9)
            ax.grid(True, alpha=0.3)

    fig.suptitle("Operation Latency vs Branch Count", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2d_latency_vs_branches.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2e: Non-zero delta fraction vs branch count
# ---------------------------------------------------------------------------
def plot_fig2e(df: pd.DataFrame, output_dir: str):
    backends_present = [b for b in BACKENDS if b in df["backend"].unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(2, ncols, figsize=(7 * ncols, 10), sharey=False)
    if ncols == 1:
        axes = axes.reshape(2, 1)

    for col, backend in enumerate(backends_present):
        for row, op in enumerate(["UPDATE", "RANGE_UPDATE"]):
            ax = axes[row, col]
            sub = df[(df.backend == backend) & (df.operation == op)]
            if op == "RANGE_UPDATE":
                sub = sub[sub.range_size == 20]

            for topo in TOPO_ORDER:
                tsub = sub[sub.topology == topo]
                agg = tsub.groupby("N").apply(
                    lambda g: (g.storage_delta != 0).mean() * 100,
                    include_groups=False,
                ).reset_index(name="nz_pct")
                agg = agg.sort_values("N")

                ax.plot(agg.N, agg.nz_pct, "o-", color=TOPO_COLORS[topo],
                        label=TOPO_LABELS[topo], linewidth=1.5, markersize=4)

            ax.set_title(f"{BACKEND_LABELS[backend]} — {op}", fontsize=11)
            ax.set_xlabel("Number of branches (N)")
            ax.set_ylabel("Non-zero delta fraction (%)")
            ax.set_xscale("log", base=2)
            ax.set_ylim(-1, max(10, ax.get_ylim()[1]))
            ax.legend(frameon=True, fontsize=9)
            ax.grid(True, alpha=0.3)

    fig.suptitle("Fraction of Operations with Non-Zero Storage Delta", fontsize=13, y=1.02)
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2e_nonzero_fraction.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Experiment 2 plots")
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

    plot_fig2a(df, output_dir)
    plot_fig2b(df, output_dir)
    plot_fig2c(df, output_dir)
    plot_fig2d(df, output_dir)
    plot_fig2e(df, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
