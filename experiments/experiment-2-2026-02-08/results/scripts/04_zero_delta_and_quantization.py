#!/usr/bin/env python3
"""Analyze zero-delta fraction and non-zero delta quantization for Experiment 2.

Produces:
  fig2f: Zero-delta fraction by backend x topology (grouped bar chart)
  fig2g: Non-zero delta value distribution per backend (strip/histogram
         showing quantization patterns)

Also prints structured text output for pasting into REPORT.md.

Usage:
    python 04_zero_delta_and_quantization.py [--data-dir ../data] [--output-dir ../figures]
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
# Data loading (shared with other scripts)
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
    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BACKENDS = ["dolt", "file_copy", "neon", "xata"]
BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy",
    "neon": "Neon",
    "xata": "Xata",
}
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABELS = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
TOPO_COLORS = {"spine": "#d62728", "bushy": "#2ca02c", "fan_out": "#1f77b4"}


def fmt_bytes(b: float) -> str:
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


# ---------------------------------------------------------------------------
# Section 1: Zero-delta fraction overall
# ---------------------------------------------------------------------------
def print_zero_delta_overall(df: pd.DataFrame) -> None:
    print("=" * 70)
    print("ZERO-DELTA FRACTION: Overall by backend")
    print("=" * 70)
    print()
    print("| Backend | Total ops | Zero deltas | Zero fraction | Non-zero deltas | Non-zero fraction |")
    print("|---------|-----------|-------------|---------------|-----------------|-------------------|")
    for b in BACKENDS:
        sub = df[df.backend == b]
        if sub.empty:
            continue
        total = len(sub)
        zero = int((sub.storage_delta == 0).sum())
        nz = total - zero
        print(
            f"| **{BACKEND_LABELS[b]}** "
            f"| {total:,} "
            f"| {zero:,} "
            f"| {zero / total * 100:.1f}% "
            f"| {nz:,} "
            f"| {nz / total * 100:.1f}% |"
        )
    print()


# ---------------------------------------------------------------------------
# Section 2: Zero-delta fraction by topology
# ---------------------------------------------------------------------------
def print_zero_delta_by_topology(df: pd.DataFrame) -> None:
    print("=" * 70)
    print("ZERO-DELTA FRACTION: By backend x topology")
    print("=" * 70)
    print()

    # Use Exp 2a data: UPDATE + RANGE_UPDATE(r=20), all topologies
    exp2a = df[
        (df.operation == "UPDATE")
        | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))
    ]

    print("| Backend | Spine | Bushy | Fan-out | Topology-invariant? |")
    print("|---------|-------|-------|---------|---------------------|")
    for b in BACKENDS:
        bsub = exp2a[exp2a.backend == b]
        if bsub.empty:
            continue
        fracs = {}
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if tsub.empty:
                fracs[topo] = "—"
            else:
                zf = (tsub.storage_delta == 0).mean() * 100
                fracs[topo] = f"{zf:.1f}%"

        # Check invariance: max spread < 5pp
        numeric = []
        for topo in TOPO_ORDER:
            tsub = bsub[bsub.topology == topo]
            if not tsub.empty:
                numeric.append((tsub.storage_delta == 0).mean() * 100)
        if len(numeric) >= 2:
            spread = max(numeric) - min(numeric)
            invariant = "Yes" if spread < 5.0 else f"No (spread={spread:.1f}pp)"
        else:
            invariant = "—"

        print(
            f"| **{BACKEND_LABELS[b]}** "
            f"| {fracs['spine']} "
            f"| {fracs['bushy']} "
            f"| {fracs['fan_out']} "
            f"| {invariant} |"
        )
    print()


# ---------------------------------------------------------------------------
# Section 3: Non-zero delta quantization
# ---------------------------------------------------------------------------
def print_quantization_analysis(df: pd.DataFrame) -> None:
    print("=" * 70)
    print("NON-ZERO DELTA QUANTIZATION: By backend")
    print("=" * 70)
    print()

    for b in BACKENDS:
        sub = df[df.backend == b]
        nz = sub[sub.storage_delta != 0]["storage_delta"]
        if nz.empty:
            continue

        print(f"--- {BACKEND_LABELS[b]} ({len(nz)} non-zero deltas) ---")
        unique_vals = sorted(nz.unique())
        counts = nz.value_counts()

        # Classify each occurrence (not unique value)
        occ_page_8k = 0
        occ_unaligned = 0
        occ_negative = 0
        for val, cnt in counts.items():
            if val < 0:
                occ_negative += cnt
            elif val % 8192 == 0:
                occ_page_8k += cnt
            else:
                occ_unaligned += cnt
        total_nz = len(nz)

        if b == "dolt":
            dolt_chunks = {16384, 65536, 1048576}
            occ_chunk = sum(cnt for val, cnt in counts.items() if val in dolt_chunks)
            print(f"  Unique values: {len(unique_vals)}")
            print(f"  Dolt chunk-aligned occurrences: {occ_chunk}/{total_nz} ({occ_chunk / total_nz * 100:.0f}%)")
            print(f"  All non-zero deltas match chunk sizes: {'Yes' if occ_chunk == total_nz else 'No'}")
        else:
            print(f"  Unique values: {len(unique_vals)}")
            print(f"  8KB-page aligned: {occ_page_8k}/{total_nz} occurrences ({occ_page_8k / total_nz * 100:.0f}%)")
            print(f"  NOT page-aligned: {occ_unaligned}/{total_nz} occurrences ({occ_unaligned / total_nz * 100:.0f}%)")
            print(f"  Negative: {occ_negative}/{total_nz} occurrences ({occ_negative / total_nz * 100:.0f}%)")

        print()
        print(f"  Value distribution:")
        for val, cnt in nz.value_counts().sort_index().items():
            aligned = ""
            if val > 0 and val % 8192 == 0:
                aligned = f"  [{val // 8192} x 8KB]"
            elif val > 0 and val % 4096 == 0:
                aligned = f"  [{val // 4096} x 4KB]"
            if b == "dolt" and val in {16384, 65536, 1048576}:
                label = {16384: "16KB chunk", 65536: "64KB chunk", 1048576: "1MB chunk"}
                aligned = f"  [{label[val]}]"
            print(f"    {fmt_bytes(val):>12}  ({val:>15,} B)  x{cnt}{aligned}")
        print()


# ---------------------------------------------------------------------------
# Fig 2f: Zero-delta fraction by backend x topology (grouped bar)
# ---------------------------------------------------------------------------
def plot_fig2f(df: pd.DataFrame, output_dir: str) -> None:
    # Use Exp 2a data
    exp2a = df[
        (df.operation == "UPDATE")
        | ((df.operation == "RANGE_UPDATE") & (df.range_size == 20))
    ]

    backends_present = [b for b in BACKENDS if b in exp2a.backend.unique()]
    x = np.arange(len(backends_present))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))

    for i, topo in enumerate(TOPO_ORDER):
        fracs = []
        for b in backends_present:
            tsub = exp2a[(exp2a.backend == b) & (exp2a.topology == topo)]
            if tsub.empty:
                fracs.append(float("nan"))
            else:
                fracs.append((tsub.storage_delta == 0).mean() * 100)
        ax.bar(
            x + i * width,
            fracs,
            width,
            label=TOPO_LABELS[topo],
            color=TOPO_COLORS[topo],
            alpha=0.85,
        )

    ax.set_xlabel("Backend")
    ax.set_ylabel("Zero-delta fraction (%)")
    ax.set_title("Fraction of Operations with Zero Storage Delta by Backend & Topology")
    ax.set_xticks(x + width)
    ax.set_xticklabels([BACKEND_LABELS[b] for b in backends_present])
    ax.set_ylim(0, 105)
    ax.legend(frameon=True)
    ax.grid(True, axis="y", alpha=0.3)

    # Add value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt="%.1f%%", fontsize=8, padding=2)

    fig.tight_layout()
    path = os.path.join(output_dir, "fig2f_zero_delta_by_topology.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Fig 2g: Non-zero delta quantization (strip plot per backend)
# ---------------------------------------------------------------------------
def plot_fig2g(df: pd.DataFrame, output_dir: str) -> None:
    backends_present = [b for b in BACKENDS if b in df.backend.unique()]
    ncols = len(backends_present)
    fig, axes = plt.subplots(1, ncols, figsize=(6 * ncols, 6))
    if ncols == 1:
        axes = [axes]

    for ax, b in zip(axes, backends_present):
        nz = df[(df.backend == b) & (df.storage_delta != 0)]["storage_delta"]
        if nz.empty:
            ax.set_title(f"{BACKEND_LABELS[b]}\n(no non-zero deltas)")
            continue

        unique_vals = sorted(nz.unique())
        counts = nz.value_counts().sort_index()

        # Color by alignment
        colors = []
        for val in counts.index:
            if val < 0:
                colors.append("#e74c3c")  # red for negative
            elif val % 8192 == 0:
                colors.append("#2ecc71")  # green for 8KB-aligned
            else:
                colors.append("#f39c12")  # orange for unaligned

        bars = ax.barh(
            range(len(counts)),
            counts.values,
            color=colors,
            height=0.8,
        )

        # Y-tick labels = byte values
        ylabels = [fmt_bytes(v) for v in counts.index]
        ax.set_yticks(range(len(counts)))
        ax.set_yticklabels(ylabels, fontsize=7)
        ax.set_xlabel("Occurrences")
        ax.set_title(f"{BACKEND_LABELS[b]} ({len(nz)} non-zero deltas)")
        ax.grid(True, axis="x", alpha=0.3)

        # Add count labels
        for bar, cnt in zip(bars, counts.values):
            ax.text(
                bar.get_width() + 0.3,
                bar.get_y() + bar.get_height() / 2,
                str(cnt),
                va="center",
                fontsize=7,
            )

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="#2ecc71", label="8KB-page aligned"),
        Patch(facecolor="#f39c12", label="Not page-aligned"),
        Patch(facecolor="#e74c3c", label="Negative"),
    ]
    fig.legend(
        handles=legend_elements,
        loc="lower center",
        ncol=3,
        fontsize=9,
        bbox_to_anchor=(0.5, -0.02),
    )

    fig.suptitle(
        "Non-Zero Storage Delta Value Distribution (quantization check)",
        fontsize=13,
        y=1.02,
    )
    fig.tight_layout()
    path = os.path.join(output_dir, "fig2g_nonzero_delta_quantization.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Zero-delta and quantization analysis for Experiment 2"
    )
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
    )
    parser.add_argument(
        "--output-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "figures"),
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    output_dir = os.path.abspath(args.output_dir)
    os.makedirs(output_dir, exist_ok=True)

    print(f"Loading data from: {data_dir}")
    df = load_all(data_dir)
    print(f"Loaded {len(df)} rows")
    print()

    print_zero_delta_overall(df)
    print_zero_delta_by_topology(df)
    print_quantization_analysis(df)
    plot_fig2f(df, output_dir)
    plot_fig2g(df, output_dir)

    print("\nDone.")


if __name__ == "__main__":
    main()
