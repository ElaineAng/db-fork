#!/usr/bin/env python3
"""Generate per-backend individual figures for Exp2 REPORT.md 2x2 grids.

Produces one PNG per (backend, metric):
  fig2a_{backend}.png  - UPDATE storage delta vs N  (mean +/- 1 std shadow)
  fig2b_{backend}.png  - RANGE_UPDATE(r=20) storage delta vs N  (mean +/- 1 std shadow)
  fig2c_{backend}.png  - Per-key delta vs range size (spine, selected N)
  fig2f_{backend}.png  - Zero-delta fraction by topology (bar chart)
  fig2g_{backend}.png  - Non-zero delta quantization (horizontal bar)

Usage:
    python 06_generate_per_backend_figures.py [--data-dir ../data] [--output-dir ../figures]
"""

import argparse
import glob
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Data loading (same as 02_plot.py)
# ---------------------------------------------------------------------------
BACKENDS = ["dolt", "file_copy", "neon", "xata"]
BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy",
    "neon": "Neon",
    "xata": "Xata",
}
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_COLORS = {"spine": "#d62728", "bushy": "#2ca02c", "fan_out": "#1f77b4"}
TOPO_LABELS = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}


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
    return out[~out["is_xata_invalid"]].copy()


def fmt_bytes_axis(val, _pos):
    if abs(val) >= 1 << 30:
        return f"{val / (1 << 30):.1f} GB"
    elif abs(val) >= 1 << 20:
        return f"{val / (1 << 20):.1f} MB"
    elif abs(val) >= 1 << 10:
        return f"{val / (1 << 10):.0f} KB"
    return f"{val:.0f} B"


def fmt_bytes(b: float) -> str:
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


# ---------------------------------------------------------------------------
# Fig 2a: UPDATE storage delta vs N (per backend)
# ---------------------------------------------------------------------------
def plot_fig2a_per_backend(df: pd.DataFrame, output_dir: str):
    updates = df[df.operation == "UPDATE"]
    for backend in BACKENDS:
        bsub = updates[updates.backend == backend]
        if bsub.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.5, 4.0))
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                continue
            agg = tsub.groupby("N")["storage_delta"].agg(["mean", "std"]).reset_index().sort_values("N")
            x, y, std = agg["N"].values, agg["mean"].values, agg["std"].fillna(0).values
            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo], linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)
        ax.set_title(f"{BACKEND_LABELS[backend]} — UPDATE Delta vs N")
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Mean storage delta (bytes)")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig2a_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2b: RANGE_UPDATE(r=20) storage delta vs N (per backend)
# ---------------------------------------------------------------------------
def plot_fig2b_per_backend(df: pd.DataFrame, output_dir: str):
    ru = df[(df.operation == "RANGE_UPDATE") & (df.range_size == 20)]
    for backend in BACKENDS:
        bsub = ru[ru.backend == backend]
        if bsub.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.5, 4.0))
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                continue
            agg = tsub.groupby("N")["storage_delta"].agg(["mean", "std"]).reset_index().sort_values("N")
            x, y, std = agg["N"].values, agg["mean"].values, agg["std"].fillna(0).values
            ax.plot(x, y, "o-", color=TOPO_COLORS[topo], label=TOPO_LABELS[topo], linewidth=1.5, markersize=4)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)
        ax.set_title(f"{BACKEND_LABELS[backend]} — RANGE_UPDATE (r=20) Delta vs N")
        ax.set_xlabel("Number of branches (N)")
        ax.set_ylabel("Mean storage delta (bytes)")
        ax.set_xscale("log", base=2)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig2b_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2c: Per-key delta vs range size (spine, per backend)
# ---------------------------------------------------------------------------
def plot_fig2c_per_backend(df: pd.DataFrame, output_dir: str):
    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")].copy()
    ru_spine["per_key_delta"] = ru_spine["storage_delta"] / ru_spine["num_keys_touched"]
    key_ns = [1, 8, 64, 256, 1024]
    cmap = plt.cm.viridis(np.linspace(0.1, 0.9, len(key_ns)))

    for backend in BACKENDS:
        bsub = ru_spine[ru_spine.backend == backend]
        if bsub.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.5, 4.0))
        for i, n in enumerate(key_ns):
            nsub = bsub[bsub.N == n]
            if nsub.empty:
                continue
            agg = nsub.groupby("range_size")["per_key_delta"].mean().reset_index().sort_values("range_size")
            ax.plot(agg.range_size, agg.per_key_delta, "o-", color=cmap[i], label=f"N={n}", linewidth=1.5, markersize=5)
        ax.set_title(f"{BACKEND_LABELS[backend]} — Per-Key Delta vs Range Size")
        ax.set_xlabel("Range size (keys per RANGE_UPDATE)")
        ax.set_ylabel("Per-key storage delta (bytes)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig2c_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2f: Zero-delta fraction by topology (per backend, bar chart)
# ---------------------------------------------------------------------------
def plot_fig2f_per_backend(df: pd.DataFrame, output_dir: str):
    exp2a = df[
        (df.operation == "UPDATE")
        | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))
    ]
    for backend in BACKENDS:
        bsub = exp2a[exp2a.backend == backend]
        if bsub.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.5, 4.0))
        x = np.arange(len(TOPO_ORDER))
        fracs = []
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                fracs.append(0.0)
            else:
                fracs.append((tsub.storage_delta == 0).mean() * 100)
        bars = ax.bar(x, fracs, width=0.5, color=[TOPO_COLORS[t] for t in TOPO_ORDER], alpha=0.85)
        ax.set_xticks(x)
        ax.set_xticklabels([TOPO_LABELS[t] for t in TOPO_ORDER])
        ax.set_ylabel("Zero-delta fraction (%)")
        ax.set_title(f"{BACKEND_LABELS[backend]} — Zero-Delta Fraction")
        ax.set_ylim(0, 105)
        ax.grid(True, axis="y", alpha=0.3)
        ax.bar_label(bars, fmt="%.1f%%", fontsize=9, padding=2)
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig2f_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2g: Non-zero delta quantization (per backend, horizontal bar)
# ---------------------------------------------------------------------------
def plot_fig2g_per_backend(df: pd.DataFrame, output_dir: str):
    from matplotlib.patches import Patch

    for backend in BACKENDS:
        nz = df[(df.backend == backend) & (df.storage_delta != 0)]["storage_delta"]
        if nz.empty:
            continue
        counts = nz.value_counts().sort_index()
        colors = []
        for val in counts.index:
            if val < 0:
                colors.append("#e74c3c")
            elif val % 8192 == 0:
                colors.append("#2ecc71")
            else:
                colors.append("#f39c12")

        height = max(4.0, min(8.0, len(counts) * 0.35 + 1.5))
        fig, ax = plt.subplots(figsize=(5.5, height))
        bars = ax.barh(range(len(counts)), counts.values, color=colors, height=0.8)
        ylabels = [fmt_bytes(v) for v in counts.index]
        ax.set_yticks(range(len(counts)))
        ax.set_yticklabels(ylabels, fontsize=7)
        ax.set_xlabel("Occurrences")
        ax.set_title(f"{BACKEND_LABELS[backend]} ({len(nz)} non-zero deltas)")
        ax.grid(True, axis="x", alpha=0.3)
        for bar, cnt in zip(bars, counts.values):
            ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2, str(cnt), va="center", fontsize=7)

        legend_elements = [
            Patch(facecolor="#2ecc71", label="8KB-page aligned"),
            Patch(facecolor="#f39c12", label="Not page-aligned"),
            Patch(facecolor="#e74c3c", label="Negative"),
        ]
        ax.legend(handles=legend_elements, fontsize=7, loc="lower right")
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig2g_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate per-backend Exp2 figures for 2x2 grids")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "figures"))
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading data from: {data_dir}")
    df = load_all(data_dir)
    print(f"Loaded {len(df)} rows\n")

    plot_fig2a_per_backend(df, output_dir)
    plot_fig2b_per_backend(df, output_dir)
    plot_fig2c_per_backend(df, output_dir)
    plot_fig2f_per_backend(df, output_dir)
    plot_fig2g_per_backend(df, output_dir)

    print(f"\nDone. Generated per-backend figures in {output_dir}")


if __name__ == "__main__":
    main()
