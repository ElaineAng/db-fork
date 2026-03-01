#!/usr/bin/env python3
"""Generate per-backend individual figures for Exp1 REPORT.md 2x2 grids.

Produces one PNG per backend for Figure 1 (marginal storage delta trajectory):
  fig1_{backend}.png

Each figure shows mean ± 1 std across repetitions at each branch index,
using the max common N for that backend (1024 for Dolt/file_copy, 8 for
Neon, max common N across topologies for Xata).

Usage:
    python 03_generate_per_backend_figures.py [--data-dir ../data] [--output-dir ../figures]
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


def parse_filename(filepath: str) -> dict | None:
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


def fmt_bytes_axis(val, _pos):
    if abs(val) >= 1 << 30:
        return f"{val / (1 << 30):.1f} GB"
    elif abs(val) >= 1 << 20:
        return f"{val / (1 << 20):.1f} MB"
    elif abs(val) >= 1 << 10:
        return f"{val / (1 << 10):.0f} KB"
    return f"{val:.0f} B"


def max_common_n(bdf: pd.DataFrame, topologies: list[str]) -> int | None:
    common = None
    for topo in topologies:
        ns = set(bdf[bdf["topology"] == topo]["N"].unique())
        common = ns if common is None else (common & ns)
    if not common:
        return None
    return int(max(common))


# ---------------------------------------------------------------------------
# Per-backend figure generation
# ---------------------------------------------------------------------------
def plot_fig1_per_backend(df: pd.DataFrame, output_dir: str):
    df = df.copy()
    df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
    # Exclude Xata rows where the metrics API returned no data
    df = df[~((df["backend"] == "xata") & ((df["disk_size_before"] == 0) | (df["disk_size_after"] == 0)))]

    for backend in BACKENDS:
        bdf = df[df["backend"] == backend]
        if bdf.empty:
            continue

        # Determine target N
        if backend == "xata":
            common_n = max_common_n(bdf, TOPO_ORDER)
            target_n = int(common_n) if common_n is not None else int(bdf["N"].max())
            title_suffix = f"N={target_n}, all topologies" if common_n is not None else f"N={target_n}"
        else:
            target_n = int(bdf["N"].max())
            title_suffix = f"N={target_n}"

        bsub = bdf[bdf["N"] == target_n]
        if bsub.empty:
            continue

        fig, ax = plt.subplots(figsize=(5.5, 4.0))
        for topo in TOPO_ORDER:
            tsub = bsub[bsub["topology"] == topo]
            if tsub.empty:
                continue
            agg = tsub.groupby("iteration_number")["storage_delta"].agg(["mean", "std"]).reset_index()
            x = agg["iteration_number"].values + 1
            y = agg["mean"].values
            std = agg["std"].values

            ax.plot(x, y, color=TOPO_COLORS[topo], label=TOPO_LABELS[topo], linewidth=0.8, alpha=0.9)
            ax.fill_between(x, y - std, y + std, color=TOPO_COLORS[topo], alpha=0.15)

        ax.set_title(f"{BACKEND_LABELS[backend]} ({title_suffix})")
        ax.set_xlabel("Branch index (nth branch created)")
        ax.set_ylabel("Marginal storage delta (bytes)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(fmt_bytes_axis))
        ax.legend(frameon=True, fontsize=9)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        path = os.path.join(output_dir, f"fig1_{backend}.png")
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate per-backend Exp1 figures for 2x2 grids")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "figures"))
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading data from: {data_dir}")
    df = load_all(data_dir)
    print(f"Loaded {len(df)} rows\n")

    plot_fig1_per_backend(df, output_dir)

    print(f"\nDone. Generated per-backend figures in {output_dir}")


if __name__ == "__main__":
    main()
