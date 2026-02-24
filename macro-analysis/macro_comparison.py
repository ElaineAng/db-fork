"""Compare macrobenchmark latency across backends (Dolt vs Neon), broken down by
operation type and workflow.

Reads parquet files from two backend directories and produces:
  1. Side-by-side box plots: latency by op type, one subplot per workflow,
     Dolt vs Neon overlaid.
  2. Grouped bar chart: median latency by op type per workflow, backends
     side-by-side.
  3. Stacked bar chart: total time breakdown by op type per workflow,
     one group per backend.
  4. Text summary table with per-op median latencies for both backends.

Usage:
    python macro-analysis/macro_comparison.py \
        --dolt-dir run_stats/macro_dolt_mini_s1 \
        --neon-dir run_stats/macro_neon_mini_s1 \
        --outdir macro-analysis/figures_comparison
"""

import argparse
import os

import numpy as np
import pyarrow.parquet as pq
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


# ── Constants (shared with macro_analysis.py) ────────────────────────

OP_NAMES = {
    0: "UNSPECIFIED",
    1: "BRANCH_CREATE",
    2: "BRANCH_CONNECT",
    3: "READ",
    4: "INSERT",
    5: "UPDATE",
    6: "COMMIT",
    7: "DDL",
    8: "BRANCH_DELETE",
    9: "API_RETRY_WAIT",
}

OP_SHORT = {
    1: "Create\nBranch",
    2: "Connect\nBranch",
    3: "Read",
    4: "Insert",
    5: "Update",
    6: "Commit",
    7: "DDL",
    8: "Delete\nBranch",
    9: "API\nRetry",
}

WORKFLOW_LABELS = {
    "software_dev": "Software Dev",
    "failure_repro": "Failure Repro",
    "data_cleaning": "Data Cleaning",
    "mcts": "MCTS",
    "simulation": "Simulation",
}

WORKFLOW_ORDER = [
    "software_dev",
    "failure_repro",
    "data_cleaning",
    "mcts",
    "simulation",
]

BRANCH_OPS = {1, 2, 8}
DATA_OPS = {3, 4, 5, 7}
OVERHEAD_OPS = {9}

BACKEND_COLORS = {
    "Dolt": "#2176AE",
    "Neon": "#E8554E",
}


# ── Data loading ─────────────────────────────────────────────────────


def load_all_workflows(indir):
    """Load all macro_*.parquet files, return dict of {workflow_name: DataFrame}."""
    import glob

    workflows = {}
    for wf in WORKFLOW_ORDER:
        pattern = os.path.join(indir, f"macro_{wf}*.parquet")
        matches = sorted(glob.glob(pattern))
        if matches:
            df = pq.read_table(matches[0]).to_pandas()
            workflows[wf] = df
            if len(matches) > 1:
                print(
                    f"  Note: multiple files for {wf}, using {os.path.basename(matches[0])}"
                )
        else:
            print(f"  Warning: no macro_{wf}*.parquet in {indir}, skipping.")
    return workflows


# ── Plot 1: Side-by-side box plots per workflow ──────────────────────


def plot_latency_boxplots(dolt_wfs, neon_wfs, outdir):
    """Box plots of latency by operation type, Dolt vs Neon side-by-side,
    two subplots per row, backends distinguished by color + border style."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_wfs and wf in neon_wfs
    ]
    n = len(common_wfs)
    if n == 0:
        print("  No common workflows found, skipping box plots.")
        return

    ncols = 2
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(9 * ncols, 5 * nrows), sharey=False,
    )
    axes = np.atleast_2d(axes)
    axes_flat = axes.flatten()

    # Canonical display order: branch ops first, then data ops, then overhead
    OP_ORDER = [1, 2, 8, 3, 4, 5, 7, 6, 9]

    # Category grouping for bracket labels
    cat_groups = [
        ("Branch Ops", BRANCH_OPS),
        ("Data Ops", DATA_OPS | {6}),  # include commit with data
        ("Overhead", OVERHEAD_OPS),
    ]

    for idx, wf in enumerate(common_wfs):
        ax = axes_flat[idx]
        df_d = dolt_wfs[wf]
        df_n = neon_wfs[wf]

        # Ops present in either backend, sorted by canonical order
        present = set(df_d.op_type.unique()) | set(df_n.op_type.unique())
        present.discard(0)
        all_ops = [op for op in OP_ORDER if op in present]

        positions_d = []
        positions_n = []
        data_d = []
        data_n = []
        labels = []

        for i, op in enumerate(all_ops):
            center = i * 3
            positions_d.append(center - 0.4)
            positions_n.append(center + 0.4)

            lats_d = df_d[df_d.op_type == op]["latency"].values * 1000
            lats_n = df_n[df_n.op_type == op]["latency"].values * 1000
            data_d.append(lats_d if len(lats_d) > 0 else [np.nan])
            data_n.append(lats_n if len(lats_n) > 0 else [np.nan])
            labels.append(OP_SHORT.get(int(op), str(op)))

        # Alternating light gray / transparent stripes per op
        for i in range(len(all_ops)):
            if i % 2 == 0:
                x_min = i * 3 - 1.4
                x_max = i * 3 + 1.4
                ax.axvspan(x_min, x_max, color="#000000", alpha=0.04, zorder=0)

        # Dolt: solid border
        bp_d = ax.boxplot(
            data_d,
            positions=positions_d,
            widths=0.6,
            patch_artist=True,
            showfliers=False,
            medianprops=dict(color="black", linewidth=1.5),
        )
        for patch in bp_d["boxes"]:
            patch.set_facecolor(BACKEND_COLORS["Dolt"])
            patch.set_alpha(0.7)
            patch.set_edgecolor("black")
            patch.set_linewidth(1.5)
        for element in ["whiskers", "caps"]:
            for line in bp_d[element]:
                line.set_color("black")
                line.set_linewidth(1.2)

        # Neon: dashed border
        bp_n = ax.boxplot(
            data_n,
            positions=positions_n,
            widths=0.6,
            patch_artist=True,
            showfliers=False,
            medianprops=dict(color="black", linewidth=1.5),
        )
        for patch in bp_n["boxes"]:
            patch.set_facecolor(BACKEND_COLORS["Neon"])
            patch.set_alpha(0.7)
            patch.set_edgecolor("black")
            patch.set_linewidth(1.5)
            patch.set_linestyle("--")
        for element in ["whiskers", "caps"]:
            for line in bp_n[element]:
                line.set_color("black")
                line.set_linewidth(1.2)
                line.set_linestyle("--")

        centers = [i * 3 for i in range(len(all_ops))]
        ax.set_xticks(centers)
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_ylabel("Latency (ms)", fontsize=10)
        ax.set_title(
            WORKFLOW_LABELS.get(wf, wf), fontsize=12, fontweight="bold"
        )
        ax.set_yscale("log")
        ax.grid(True, alpha=0.3, axis="y")

        # Category bracket labels below x-axis
        for cat_label, op_set in cat_groups:
            cat_indices = [i for i, op in enumerate(all_ops) if op in op_set]
            if not cat_indices:
                continue
            x_min = min(cat_indices) * 3 - 1.2
            x_max = max(cat_indices) * 3 + 1.2
            mid = (x_min + x_max) / 2
            # Draw bracket: horizontal line + short verticals at ends
            bracket_y = -0.13
            tick_y = -0.11
            ax.plot(
                [x_min, x_max], [bracket_y, bracket_y],
                color="#555555", linewidth=1.2, clip_on=False,
                transform=ax.get_xaxis_transform(),
            )
            for bx in [x_min, x_max]:
                ax.plot(
                    [bx, bx], [tick_y, bracket_y],
                    color="#555555", linewidth=1.2, clip_on=False,
                    transform=ax.get_xaxis_transform(),
                )
            ax.text(
                mid, -0.17, cat_label,
                ha="center", va="top", fontsize=8, fontweight="bold",
                color="#555555", transform=ax.get_xaxis_transform(),
            )

    # Hide unused subplots
    for idx in range(n, len(axes_flat)):
        axes_flat[idx].axis("off")

    # Shared legend
    handles = [
        Patch(
            facecolor=BACKEND_COLORS["Dolt"], alpha=0.7,
            edgecolor="black", linewidth=1.5, linestyle="-",
            label="Dolt (solid)",
        ),
        Patch(
            facecolor=BACKEND_COLORS["Neon"], alpha=0.7,
            edgecolor="black", linewidth=1.5, linestyle="--",
            label="Neon (dashed)",
        ),
    ]
    fig.legend(
        handles=handles, loc="upper right", fontsize=11, framealpha=0.9,
    )

    fig.suptitle(
        "Operation Latency: Dolt vs Neon",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )
    fig.tight_layout()
    path = os.path.join(outdir, "latency_boxplot_comparison.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 2: Total time breakdown stacked bars ────────────────────────


def plot_time_breakdown(dolt_wfs, neon_wfs, outdir):
    """Horizontal 100% stacked bar chart: percentage of time on each op-type
    category, one pair of bars (Dolt, Neon) per workflow."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_wfs and wf in neon_wfs
    ]
    if not common_wfs:
        print("  No common workflows found, skipping time breakdown.")
        return

    categories = [
        ("Branch Ops", BRANCH_OPS, "#F5A623"),
        ("Read", {3}, "#2D8B57"),
        ("Insert", {4}, "#52B788"),
        ("Update", {5}, "#95D5B2"),
        ("DDL", {7}, "#D8F3DC"),
        ("API Retry", OVERHEAD_OPS, "#B0B0B0"),
    ]

    n = len(common_wfs)
    bar_height = 0.25
    bar_gap = 0.05  # gap between Dolt and Neon bars
    group_gap = 0.6  # extra gap between workflow groups
    pair_height = 2 * bar_height + bar_gap

    fig, ax = plt.subplots(figsize=(14, 8))

    y_positions = []
    y_labels = []

    for wi, wf in enumerate(common_wfs):
        group_top = wi * (pair_height + group_gap)
        for bi, (backend_label, wfs) in enumerate(
            [("Dolt", dolt_wfs), ("Neon", neon_wfs)]
        ):
            df = wfs[wf]
            y = group_top + bi * (bar_height + bar_gap)
            y_positions.append(y)
            y_labels.append(backend_label)

            # Compute per-category times and normalize to percentages
            cat_times = []
            for _, op_set, _ in categories:
                cat_times.append(df[df.op_type.isin(op_set)]["latency"].sum())
            total = sum(cat_times)
            if total == 0:
                continue
            pcts = [t / total * 100 for t in cat_times]

            left = 0
            for ci, (cat_name, _, color) in enumerate(categories):
                if pcts[ci] == 0:
                    continue
                ax.barh(
                    y,
                    pcts[ci],
                    bar_height,
                    left=left,
                    color=color,
                    alpha=0.85,
                    edgecolor="white",
                    linewidth=0.5,
                )
                # Label segments >= 8%
                if pcts[ci] >= 8:
                    ax.text(
                        left + pcts[ci] / 2,
                        y,
                        f"{pcts[ci]:.0f}%",
                        ha="center",
                        va="center",
                        fontsize=9,
                        fontweight="bold",
                        color="white",
                    )
                left += pcts[ci]

        # Draw a light separator line between groups (except after the last)
        if wi < n - 1:
            sep_y = group_top + pair_height + group_gap / 2
            ax.axhline(sep_y, color="#cccccc", linewidth=0.8, linestyle="--")

    # Y-axis: backend labels as tick labels, workflow names placed close to axis
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels, fontsize=9)
    ax.tick_params(axis="y", pad=2)

    for wi, wf in enumerate(common_wfs):
        group_top = wi * (pair_height + group_gap)
        group_center = group_top + (pair_height - bar_gap) / 2
        ax.text(
            -0.1,
            group_center,
            WORKFLOW_LABELS.get(wf, wf),
            ha="right",
            va="center",
            fontsize=11,
            fontweight="bold",
            transform=ax.get_yaxis_transform(),
        )

    # Tighten y-axis to the data
    y_min = -0.4
    y_max = (n - 1) * (pair_height + group_gap) + pair_height + 0.1
    ax.set_ylim(y_max, y_min)  # inverted

    ax.set_xlim(0, 100)
    ax.set_xlabel("Time (%)", fontsize=11)
    ax.set_title(
        "Time Breakdown by Operation Type: Dolt vs Neon",
        fontsize=13,
        fontweight="bold",
    )
    ax.grid(True, alpha=0.3, axis="x")

    # Legend
    legend_handles = [
        Patch(facecolor=color, alpha=0.85, label=cat_name)
        for cat_name, _, color in categories
    ]
    ax.legend(
        handles=legend_handles,
        fontsize=9,
        framealpha=0.9,
        loc="lower right",
        ncol=len(categories),
    )

    fig.tight_layout()
    path = os.path.join(outdir, "time_breakdown_comparison.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 4: Heatmap comparison ───────────────────────────────────────


def plot_heatmap_comparison(dolt_wfs, neon_wfs, outdir):
    """Side-by-side heatmaps: median latency by (workflow x op_type)
    for Dolt and Neon, with ratio annotations."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_wfs and wf in neon_wfs
    ]
    if not common_wfs:
        print("  No common workflows found, skipping heatmap.")
        return

    all_ops = set()
    for wf in common_wfs:
        all_ops |= set(dolt_wfs[wf].op_type.unique())
        all_ops |= set(neon_wfs[wf].op_type.unique())
    all_ops = sorted(op for op in all_ops if op != 0)

    n_wf = len(common_wfs)
    n_op = len(all_ops)

    data_d = np.full((n_wf, n_op), np.nan)
    data_n = np.full((n_wf, n_op), np.nan)

    for i, wf in enumerate(common_wfs):
        for j, op in enumerate(all_ops):
            sub_d = dolt_wfs[wf][dolt_wfs[wf].op_type == op]
            sub_n = neon_wfs[wf][neon_wfs[wf].op_type == op]
            if len(sub_d) > 0:
                data_d[i, j] = np.median(sub_d["latency"].values) * 1000
            if len(sub_n) > 0:
                data_n[i, j] = np.median(sub_n["latency"].values) * 1000

    # Compute ratio: Neon / Dolt (>1 means Neon is slower)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = data_n / data_d

    fig, axes = plt.subplots(
        1, 3, figsize=(18, 4.5), gridspec_kw={"width_ratios": [1, 1, 1]}
    )

    op_labels = [
        OP_SHORT.get(int(op), str(op)).replace("\n", " ") for op in all_ops
    ]
    wf_labels = [WORKFLOW_LABELS.get(wf, wf) for wf in common_wfs]

    for ax_idx, (ax, data, title, cmap) in enumerate(
        zip(
            axes[:2],
            [data_d, data_n],
            ["Dolt", "Neon"],
            ["Blues", "Reds"],
        )
    ):
        masked = np.ma.masked_invalid(data)
        log_data = np.ma.log10(masked)

        im = ax.imshow(log_data, cmap=cmap, aspect="auto")
        ax.set_xticks(range(n_op))
        ax.set_xticklabels(op_labels, fontsize=9, rotation=45, ha="right")
        ax.set_yticks(range(n_wf))
        ax.set_yticklabels(wf_labels if ax_idx == 0 else [], fontsize=10)
        ax.set_title(title, fontsize=12, fontweight="bold")

        # Annotate cells
        for i in range(n_wf):
            for j in range(n_op):
                if not np.isnan(data[i, j]):
                    val = data[i, j]
                    text = f"{val:.1f}" if val < 100 else f"{val:.0f}"
                    log_val = np.log10(val) if val > 0 else 0
                    log_max = np.nanmax(log_data)
                    text_color = "white" if log_val > log_max * 0.6 else "black"
                    ax.text(
                        j,
                        i,
                        text,
                        ha="center",
                        va="center",
                        fontsize=8,
                        color=text_color,
                        fontweight="bold",
                    )
                else:
                    ax.text(
                        j,
                        i,
                        "--",
                        ha="center",
                        va="center",
                        fontsize=8,
                        color="#cccccc",
                    )

        fig.colorbar(im, ax=ax, shrink=0.8, label="log10(ms)")

    # Ratio heatmap — use log10 of the raw ratio for symmetric color mapping
    from matplotlib.colors import TwoSlopeNorm
    ax_r = axes[2]

    # Work in log10 space: 0 = parity (1x), positive = Neon slower, negative = Dolt slower
    with np.errstate(divide="ignore", invalid="ignore"):
        log10_ratio = np.log10(ratio)

    # Symmetric bounds so 1x sits at the colormap center
    finite = log10_ratio[np.isfinite(log10_ratio)]
    if len(finite) > 0:
        bound = max(abs(finite.min()), abs(finite.max()), 0.5)
    else:
        bound = 1.0
    norm = TwoSlopeNorm(vcenter=0, vmin=-bound, vmax=bound)

    im_r = ax_r.imshow(
        log10_ratio, cmap="RdBu_r", aspect="auto", norm=norm,
    )
    ax_r.set_xticks(range(n_op))
    ax_r.set_xticklabels(op_labels, fontsize=9, rotation=45, ha="right")
    ax_r.set_yticks(range(n_wf))
    ax_r.set_yticklabels([], fontsize=10)
    ax_r.set_title("Neon / Dolt Ratio", fontsize=12, fontweight="bold")

    for i in range(n_wf):
        for j in range(n_op):
            if not np.isnan(ratio[i, j]):
                r = ratio[i, j]
                # Adaptive formatting: show enough precision for small ratios
                if r >= 100:
                    text = f"{r:.0f}x"
                elif r >= 10:
                    text = f"{r:.1f}x"
                elif r >= 1:
                    text = f"{r:.1f}x"
                elif r >= 0.1:
                    text = f"{r:.2f}x"
                else:
                    text = f"{r:.2g}x"
                lr = log10_ratio[i, j]
                text_color = (
                    "white" if np.isfinite(lr) and abs(lr) > bound * 0.45
                    else "black"
                )
                ax_r.text(
                    j, i, text,
                    ha="center", va="center", fontsize=8,
                    color=text_color, fontweight="bold",
                )
            else:
                ax_r.text(
                    j, i, "--",
                    ha="center", va="center", fontsize=8,
                    color="#cccccc",
                )

    # Colorbar with ratio labels instead of raw log10 values
    cbar = fig.colorbar(im_r, ax=ax_r, shrink=0.8)
    cbar.set_label("Neon / Dolt", fontsize=10)
    tick_logs = np.array([-3, -2, -1, 0, 1, 2, 3])
    tick_logs = tick_logs[(tick_logs >= -bound) & (tick_logs <= bound)]
    cbar.set_ticks(tick_logs)
    cbar.set_ticklabels([f"{10.0**t:.0g}x" if t >= 0 else f"{10.0**t:.2g}x"
                         for t in tick_logs])

    fig.suptitle(
        "Median Latency Comparison (ms)", fontsize=14, fontweight="bold", y=1.02
    )
    fig.tight_layout()
    path = os.path.join(outdir, "heatmap_comparison.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Summary table ────────────────────────────────────────────────────


def print_summary(dolt_wfs, neon_wfs):
    """Print a text summary comparing both backends."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_wfs and wf in neon_wfs
    ]
    if not common_wfs:
        print("  No common workflows.")
        return

    all_ops = set()
    for wf in common_wfs:
        all_ops |= set(dolt_wfs[wf].op_type.unique())
        all_ops |= set(neon_wfs[wf].op_type.unique())
    all_ops = sorted(op for op in all_ops if op != 0)

    print("\n" + "=" * 120)
    print("Macrobenchmark Backend Comparison: Dolt vs Neon")
    print("=" * 120)

    # Per-workflow summary
    header = (
        f"{'Workflow':>18} | {'Backend':>7} | {'Ops':>5} | "
        f"{'Total (s)':>10} | {'Median (ms)':>11} | {'P95 (ms)':>10} | "
        f"{'Branch %':>8} | {'Ops/sec':>8}"
    )
    print(header)
    print("-" * len(header))

    for wf in common_wfs:
        for backend_label, wfs in [("Dolt", dolt_wfs), ("Neon", neon_wfs)]:
            df = wfs[wf]
            df_no_overhead = df[~df.op_type.isin(OVERHEAD_OPS)]
            n_ops = len(df_no_overhead)
            total_s = df.latency.sum()
            median_ms = df_no_overhead.latency.median() * 1000
            p95_ms = df_no_overhead.latency.quantile(0.95) * 1000
            branch_ms = df[df.op_type.isin(BRANCH_OPS)].latency.sum() * 1000
            total_ms = total_s * 1000
            branch_pct = branch_ms / total_ms * 100 if total_ms > 0 else 0
            ops_sec = n_ops / total_s if total_s > 0 else 0

            label = (
                WORKFLOW_LABELS.get(wf, wf) if backend_label == "Dolt" else ""
            )
            print(
                f"{label:>18} | {backend_label:>7} | {n_ops:>5} | "
                f"{total_s:>10.2f} | {median_ms:>11.2f} | {p95_ms:>10.2f} | "
                f"{branch_pct:>7.1f}% | {ops_sec:>8.1f}"
            )
        print("-" * len(header))

    # Per-op-type median comparison
    print(f"\n{'Per-Op Median Latency (ms) — Dolt / Neon':^120}")
    header2 = f"{'Workflow':>18}"
    for op in all_ops:
        op_name = OP_SHORT.get(int(op), str(op)).replace("\n", " ")
        header2 += f" | {op_name:>18}"
    print(header2)
    print("-" * len(header2))

    for wf in common_wfs:
        df_d = dolt_wfs[wf]
        df_n = neon_wfs[wf]
        label = WORKFLOW_LABELS.get(wf, wf)

        row_d = f"{label:>18}"
        row_n = f"{'':>18}"
        row_r = f"{'':>18}"

        for op in all_ops:
            sub_d = df_d[df_d.op_type == op]
            sub_n = df_n[df_n.op_type == op]
            med_d = sub_d.latency.median() * 1000 if len(sub_d) > 0 else None
            med_n = sub_n.latency.median() * 1000 if len(sub_n) > 0 else None

            if med_d is not None and med_n is not None:
                ratio = med_n / med_d if med_d > 0 else float("inf")
                d_str = f"{med_d:.2f}" if med_d < 100 else f"{med_d:.0f}"
                n_str = f"{med_n:.2f}" if med_n < 100 else f"{med_n:.0f}"
                row_d += f" | {'D:' + d_str:>18}"
                row_n += f" | {'N:' + n_str:>18}"
                row_r += f" | {f'({ratio:.1f}x)':>18}"
            elif med_d is not None:
                d_str = f"{med_d:.2f}" if med_d < 100 else f"{med_d:.0f}"
                row_d += f" | {'D:' + d_str:>18}"
                row_n += f" | {'N:--':>18}"
                row_r += f" | {'':>18}"
            elif med_n is not None:
                n_str = f"{med_n:.2f}" if med_n < 100 else f"{med_n:.0f}"
                row_d += f" | {'D:--':>18}"
                row_n += f" | {'N:' + n_str:>18}"
                row_r += f" | {'':>18}"
            else:
                row_d += f" | {'--':>18}"
                row_n += f" | {'--':>18}"
                row_r += f" | {'':>18}"

        print(row_d)
        print(row_n)
        print(row_r)
        print()


# ── Main ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Compare macrobenchmark latency across backends."
    )
    parser.add_argument(
        "--dolt-dir",
        type=str,
        default="run_stats/macro_dolt_mini_s1",
        help="Directory with Dolt parquet files.",
    )
    parser.add_argument(
        "--neon-dir",
        type=str,
        default="run_stats/macro_neon_mini_s1",
        help="Directory with Neon parquet files.",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="macro-analysis/figures_comparison",
        help="Directory to save figures.",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print("Loading Dolt data...")
    dolt_wfs = load_all_workflows(args.dolt_dir)
    for wf, df in dolt_wfs.items():
        print(f"  {WORKFLOW_LABELS.get(wf, wf)}: {len(df):,} rows")

    print("\nLoading Neon data...")
    neon_wfs = load_all_workflows(args.neon_dir)
    for wf, df in neon_wfs.items():
        print(f"  {WORKFLOW_LABELS.get(wf, wf)}: {len(df):,} rows")

    print("\nPlot 1: Latency box plots (Dolt vs Neon)")
    plot_latency_boxplots(dolt_wfs, neon_wfs, args.outdir)

    print("\nPlot 2: Time breakdown by operation type")
    plot_time_breakdown(dolt_wfs, neon_wfs, args.outdir)

    print("\nPlot 4: Heatmap comparison with ratios")
    plot_heatmap_comparison(dolt_wfs, neon_wfs, args.outdir)

    print_summary(dolt_wfs, neon_wfs)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
