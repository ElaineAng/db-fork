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
  5. Interference latency comparison (baseline vs measurement).
  6. Storage & elapsed time comparison.
  7. Storage delta by operation type.

Usage:
    python macro-analysis/macro_comparison.py \
        --dolt-dir run_stats/dolt_mini \
        --neon-dir run_stats/neon_mini \
        --outdir macro-analysis/figures_comparison
"""

import argparse
import glob
import json
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
    workflows = {}
    for wf in WORKFLOW_ORDER:
        pattern = os.path.join(indir, f"macro_{wf}*.parquet")
        # Exclude interference parquet files
        matches = sorted(
            f for f in glob.glob(pattern) if "_interference" not in f
        )
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


def load_all_interference(indir):
    """Load all *_interference.parquet files, return {workflow_name: DataFrame}."""
    interference = {}
    for wf in WORKFLOW_ORDER:
        pattern = os.path.join(indir, f"macro_{wf}*_interference.parquet")
        matches = sorted(glob.glob(pattern))
        if matches:
            df = pq.read_table(matches[0]).to_pandas()
            interference[wf] = df
            if len(matches) > 1:
                print(
                    f"  Note: multiple interference files for {wf}, using {os.path.basename(matches[0])}"
                )
    return interference


def load_all_storage(indir):
    """Load all *_e2e_stats.json files, return {workflow_name: dict}.

    For Neon backends (new format):
      - Uses ``neon_root_branch_bytes_month`` + ``neon_child_branch_bytes_month``
        as the total storage (no "before" measurement, so this is the delta)

    For Neon backends (legacy format with before/after):
      - Prefers ``neon_delta_bytes`` if available

    For Dolt backends:
      - Uses ``storage_delta_bytes`` directly

    The canonical storage delta is stored in ``storage_delta_bytes`` for
    consistent access across both backends.
    """
    SEED_SIZE_BYTES = 104325120
    storage = {}
    for wf in WORKFLOW_ORDER:
        pattern = os.path.join(indir, f"macro_{wf}*_e2e_stats.json")
        matches = sorted(glob.glob(pattern))
        if matches:
            with open(matches[0]) as f:
                data = json.load(f)

            # For Neon: compute storage delta from available metrics
            if "neon_root_branch_bytes_month" in data:
                # New format: use current storage as delta (no before measurement)
                root = data.get("neon_root_branch_bytes_month", 0)
                child = data.get("neon_child_branch_bytes_month", 0)
                data["storage_delta_bytes"] = root + child
            elif "neon_delta_bytes" in data:
                # Legacy format with before/after: use computed delta
                data["storage_delta_bytes"] = data["neon_delta_bytes"]
            elif "storage_delta_bytes" not in data:
                # Fallback: estimate from neon_after_total_bytes (old old format)
                after = data.get("neon_after_total_bytes", 0)
                if after > 0:
                    data["storage_delta_bytes"] = after - SEED_SIZE_BYTES
                else:
                    data["storage_delta_bytes"] = 0

            storage[wf] = data
            if len(matches) > 1:
                print(
                    f"  Note: multiple e2e_stats files for {wf}, using {os.path.basename(matches[0])}"
                )
    return storage


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
        nrows,
        ncols,
        figsize=(9 * ncols, 5 * nrows),
        sharey=False,
    )
    axes = np.atleast_2d(axes)
    axes_flat = axes.flatten()

    # Canonical display order: branch ops first, then data ops
    # Note: API_RETRY_WAIT (9) is excluded from all plots
    OP_ORDER = [1, 2, 8, 3, 4, 5, 7, 6]

    # Category grouping for bracket labels
    cat_groups = [
        ("Branch Ops", BRANCH_OPS),
        ("Data Ops", DATA_OPS | {6}),  # include commit with data
        # Overhead (API Retry) excluded from all plots
    ]

    for idx, wf in enumerate(common_wfs):
        ax = axes_flat[idx]
        df_d = dolt_wfs[wf]
        df_n = neon_wfs[wf]

        # Ops present in either backend, sorted by canonical order
        # Exclude UNSPECIFIED (0) and API_RETRY_WAIT (9)
        present = set(df_d.op_type.unique()) | set(df_n.op_type.unique())
        present.discard(0)
        present.discard(9)
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
                [x_min, x_max],
                [bracket_y, bracket_y],
                color="#555555",
                linewidth=1.2,
                clip_on=False,
                transform=ax.get_xaxis_transform(),
            )
            for bx in [x_min, x_max]:
                ax.plot(
                    [bx, bx],
                    [tick_y, bracket_y],
                    color="#555555",
                    linewidth=1.2,
                    clip_on=False,
                    transform=ax.get_xaxis_transform(),
                )
            ax.text(
                mid,
                -0.17,
                cat_label,
                ha="center",
                va="top",
                fontsize=8,
                fontweight="bold",
                color="#555555",
                transform=ax.get_xaxis_transform(),
            )

    # Hide unused subplots
    for idx in range(n, len(axes_flat)):
        axes_flat[idx].axis("off")

    # Shared legend
    handles = [
        Patch(
            facecolor=BACKEND_COLORS["Dolt"],
            alpha=0.7,
            edgecolor="black",
            linewidth=1.5,
            linestyle="-",
            label="Dolt (solid)",
        ),
        Patch(
            facecolor=BACKEND_COLORS["Neon"],
            alpha=0.7,
            edgecolor="black",
            linewidth=1.5,
            linestyle="--",
            label="Neon (dashed)",
        ),
    ]
    fig.legend(
        handles=handles,
        loc="upper right",
        fontsize=11,
        framealpha=0.9,
    )

    fig.tight_layout()
    path = os.path.join(outdir, "latency_boxplot_comparison.png")
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
        # API Retry excluded from all plots
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
    path = os.path.join(outdir, "time_breakdown_comparison.png")
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

    # Canonical display order: branch ops first, then data ops
    # Same as box plots: BRANCH_CREATE, BRANCH_CONNECT, BRANCH_DELETE, then data ops
    OP_ORDER = [1, 2, 8, 3, 4, 5, 7, 6]

    all_ops_set = set()
    for wf in common_wfs:
        all_ops_set |= set(dolt_wfs[wf].op_type.unique())
        all_ops_set |= set(neon_wfs[wf].op_type.unique())
    # Exclude UNSPECIFIED (0) and API_RETRY_WAIT (9)
    all_ops_set.discard(0)
    all_ops_set.discard(9)

    # Order operations: branch ops first, then others
    all_ops = [op for op in OP_ORDER if op in all_ops_set]

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

    from matplotlib.colors import TwoSlopeNorm
    import matplotlib.gridspec as gridspec

    fig = plt.figure(figsize=(35, 7))
    gs = gridspec.GridSpec(
        1, 3, figure=fig, wspace=0.12, width_ratios=[1.5, 1.5, 1.5]
    )

    # Single row: Ratio (left), Dolt (middle), Neon (right)
    ax_r = fig.add_subplot(gs[0, 0])
    ax_d = fig.add_subplot(gs[0, 1])
    ax_n = fig.add_subplot(gs[0, 2])

    op_labels = [
        OP_SHORT.get(int(op), str(op)).replace("\n", " ") for op in all_ops
    ]
    wf_labels = [WORKFLOW_LABELS.get(wf, wf) for wf in common_wfs]

    # --- Row 0: Slowdown ratio heatmap ---
    # Red = Neon slower (ratio > 1), Blue = Dolt slower (ratio < 1)
    # Darker color = more slowdown
    # Use log scale for colormap to handle wide range
    with np.errstate(divide="ignore", invalid="ignore"):
        log10_ratio = np.log10(ratio)

    finite = log10_ratio[np.isfinite(log10_ratio)]
    if len(finite) > 0:
        bound = max(abs(finite.min()), abs(finite.max()), 0.5)
    else:
        bound = 1.0
    norm = TwoSlopeNorm(vcenter=0, vmin=-bound, vmax=bound)

    im_r = ax_r.imshow(
        log10_ratio,
        cmap="RdBu_r",
        aspect="auto",
        norm=norm,
    )
    ax_r.set_xticks(range(n_op))
    ax_r.set_xticklabels(op_labels, fontsize=18, rotation=30, ha="right")
    ax_r.set_yticks(range(n_wf))
    ax_r.set_yticklabels(wf_labels, fontsize=20)
    ax_r.set_title(
        "Relative Cost (+ : Neon more expensive, - : Dolt more expensive)",
        fontsize=20,
        fontweight="bold",
    )

    for i in range(n_wf):
        for j in range(n_op):
            if not np.isnan(ratio[i, j]):
                r = ratio[i, j]
                # Positive = Neon more expensive, Negative = Dolt more expensive
                # If ratio > 1: Neon is slower by r times → show +r
                # If ratio < 1: Dolt is slower by 1/r times → show -(1/r)
                if r >= 1:
                    value = r
                    sign = "+"
                else:
                    value = -(1 / r)
                    sign = ""  # Negative sign already included

                # Format value
                if abs(value) >= 100:
                    text = f"{sign}{value:.0f}" if r >= 1 else f"{value:.0f}"
                elif abs(value) >= 10:
                    text = f"{sign}{value:.1f}" if r >= 1 else f"{value:.1f}"
                else:
                    text = f"{sign}{value:.1f}" if r >= 1 else f"{value:.1f}"

                lr = log10_ratio[i, j]
                # Determine text color based on log scale magnitude
                text_color = (
                    "white"
                    if np.isfinite(lr) and abs(lr) > bound * 0.45
                    else "black"
                )
                ax_r.text(
                    j,
                    i,
                    text,
                    ha="center",
                    va="center",
                    fontsize=16,
                    color=text_color,
                    fontweight="bold",
                )
            else:
                ax_r.text(
                    j,
                    i,
                    "--",
                    ha="center",
                    va="center",
                    fontsize=16,
                    color="#cccccc",
                )

    cbar = fig.colorbar(im_r, ax=ax_r, shrink=0.8)
    cbar.set_label("Relative Cost (+ : Neon, - : Dolt)", fontsize=18)
    cbar.ax.tick_params(labelsize=16)
    # Set colorbar ticks to show relative cost on both sides
    tick_logs = np.array([-3, -2, -1, 0, 1, 2, 3])
    tick_logs = tick_logs[(tick_logs >= -bound) & (tick_logs <= bound)]
    cbar.set_ticks(tick_logs)
    # Positive (red) = Neon more expensive, Negative (blue) = Dolt more expensive
    tick_labels = []
    for t in tick_logs:
        value = 10.0 ** abs(t)
        # Positive values for Neon (red), negative for Dolt (blue)
        if t >= 0:
            # Positive side: Neon more expensive
            if value >= 1000:
                label = f"+{int(value)}"
            elif value >= 100:
                label = f"+{int(value)}"
            elif value >= 10:
                label = f"+{int(value)}"
            elif value >= 1:
                label = f"+{value:.0f}"
            else:
                label = f"+{value:.1f}"
        else:
            # Negative side: Dolt more expensive
            if value >= 1000:
                label = f"-{int(value)}"
            elif value >= 100:
                label = f"-{int(value)}"
            elif value >= 10:
                label = f"-{int(value)}"
            elif value >= 1:
                label = f"-{value:.0f}"
            else:
                label = f"-{value:.1f}"
        tick_labels.append(label)
    cbar.set_ticklabels(tick_labels)

    # --- Row 1: Dolt and Neon heatmaps ---
    for ax_idx, (ax, data, title, cmap) in enumerate(
        zip(
            [ax_d, ax_n],
            [data_d, data_n],
            ["Dolt", "Neon"],
            ["Blues", "Reds"],
        )
    ):
        masked = np.ma.masked_invalid(data)
        log_data = np.ma.log10(masked)

        im = ax.imshow(log_data, cmap=cmap, aspect="auto")
        ax.set_xticks(range(n_op))
        ax.set_xticklabels(op_labels, fontsize=18, rotation=30, ha="right")
        ax.set_yticks(range(n_wf))
        ax.set_yticklabels(
            [], fontsize=20
        )  # No y-axis labels for Dolt and Neon
        ax.set_title(title, fontsize=20, fontweight="bold")

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
                        fontsize=16,
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
                        fontsize=16,
                        color="#cccccc",
                    )

        cbar = fig.colorbar(im, ax=ax, shrink=0.8, label="log10(ms)")
        cbar.set_label("log10(ms)", fontsize=18)
        cbar.ax.tick_params(labelsize=16)

    path = os.path.join(outdir, "heatmap_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 5: Interference latency comparison ──────────────────────────


def plot_interference_comparison(dolt_intf, neon_intf, outdir):
    """Grouped bar chart: baseline vs measurement latency per workflow per backend."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_intf or wf in neon_intf
    ]
    if not common_wfs:
        print("  No interference data found, skipping.")
        return

    # Discover query types across all data
    query_types = set()
    for data in (dolt_intf, neon_intf):
        for df in data.values():
            query_types |= set(df["query_type"].unique())
    query_types = sorted(query_types)

    n_qt = len(query_types)
    if n_qt == 0:
        print("  No query types in interference data, skipping.")
        return

    fig, axes = plt.subplots(
        n_qt,
        1,
        figsize=(max(10, 2.5 * len(common_wfs)), 5 * n_qt),
        squeeze=False,
    )

    for qt_idx, qt in enumerate(query_types):
        ax = axes[qt_idx, 0]
        n = len(common_wfs)
        x = np.arange(n)
        bar_w = 0.18
        offsets = [-1.5, -0.5, 0.5, 1.5]

        for wi, wf in enumerate(common_wfs):
            for bi, (label, data, color) in enumerate(
                [
                    ("Dolt", dolt_intf, BACKEND_COLORS["Dolt"]),
                    ("Neon", neon_intf, BACKEND_COLORS["Neon"]),
                ]
            ):
                if wf not in data:
                    continue
                df = data[wf]
                df_qt = df[df["query_type"] == qt]
                if df_qt.empty:
                    continue

                baseline = df_qt[df_qt["phase"] == "baseline"]["latency"] * 1000
                measurement = (
                    df_qt[df_qt["phase"] == "measurement"]["latency"] * 1000
                )
                med_base = baseline.median() if len(baseline) > 0 else 0
                med_meas = measurement.median() if len(measurement) > 0 else 0

                # Baseline bar (solid)
                base_off = offsets[bi * 2]
                ax.bar(
                    x[wi] + base_off * bar_w,
                    med_base,
                    bar_w,
                    color=color,
                    alpha=0.85,
                    edgecolor="black",
                    linewidth=0.8,
                    label=f"{label} baseline" if wi == 0 else None,
                )
                # Measurement bar (hatched)
                meas_off = offsets[bi * 2 + 1]
                ax.bar(
                    x[wi] + meas_off * bar_w,
                    med_meas,
                    bar_w,
                    color=color,
                    alpha=0.85,
                    edgecolor="black",
                    linewidth=0.8,
                    hatch="//",
                    label=f"{label} measurement" if wi == 0 else None,
                )
                # Annotate % change
                if med_base > 0:
                    pct = (med_meas - med_base) / med_base * 100
                    sign = "+" if pct >= 0 else ""
                    ax.text(
                        x[wi] + meas_off * bar_w,
                        med_meas,
                        f"{sign}{pct:.0f}%",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        fontweight="bold",
                    )

        ax.set_xticks(x)
        ax.set_xticklabels(
            [WORKFLOW_LABELS.get(wf, wf) for wf in common_wfs],
            fontsize=10,
        )
        ax.set_ylabel("Latency (ms)", fontsize=11)
        ax.set_yscale("log")
        ax.grid(True, alpha=0.3, axis="y")
        ax.legend(fontsize=9, framealpha=0.9)

    fig.tight_layout()
    path = os.path.join(outdir, "interference_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 6: Storage & elapsed time comparison ────────────────────────


def _human_size(nbytes):
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def _completed_steps(e2e: dict) -> int:
    """Return total completed steps across all workers from an e2e_stats dict."""
    completed = e2e.get("completed_steps", {})
    return sum(int(v) for v in completed.values())


def _total_steps(e2e: dict) -> int:
    """Return total possible steps (workers * steps_per_worker)."""
    return e2e.get("workers", 0) * e2e.get("total_steps", 0)


def plot_storage_comparison(
    dolt_stor, neon_stor, outdir, dolt_wfs=None, neon_wfs=None
):
    """Side-by-side bar charts: elapsed time (stacked by op category) and
    storage delta per workflow."""
    common_wfs = [
        wf for wf in WORKFLOW_ORDER if wf in dolt_stor or wf in neon_stor
    ]
    if not common_wfs:
        print("  No storage data found, skipping.")
        return

    # Categories: Branch ops vs Data ops
    # Use lighter shade for branch, darker for data
    elapsed_categories = [
        ("Branch Ops", BRANCH_OPS),
        ("Data Ops", DATA_OPS | {6}),  # Read/Insert/Update/DDL/Commit
        # API Retry excluded from all plots
    ]

    # Color shades: lighter for branch ops, darker for data ops
    dolt_colors = ["#7FB3D5", "#2176AE"]  # Light blue, Dark blue
    neon_colors = ["#F5A79D", "#E8554E"]  # Light red, Dark red

    n = len(common_wfs)
    bar_w = 0.7
    gap = 1.5  # Gap between Dolt and Neon groups

    # Create positions: Dolt group (0 to n-1), gap, Neon group (n+gap to 2n+gap-1)
    dolt_positions = np.arange(n)
    neon_positions = np.arange(n) + n + gap

    fig, (ax_time, ax_stor) = plt.subplots(1, 2, figsize=(18, 7))

    # Left: elapsed_sec broken down by op category (stacked), grouped by system
    for bi, (label, stor_data, wfs, base_color, positions) in enumerate(
        [
            (
                "Dolt",
                dolt_stor,
                dolt_wfs,
                BACKEND_COLORS["Dolt"],
                dolt_positions,
            ),
            (
                "Neon",
                neon_stor,
                neon_wfs,
                BACKEND_COLORS["Neon"],
                neon_positions,
            ),
        ]
    ):
        elapsed_vals = [
            stor_data[wf]["elapsed_sec"] if wf in stor_data else 0
            for wf in common_wfs
        ]

        # If we have parquet data, compute per-category time fractions
        if wfs:
            # Choose color palette based on backend
            colors = dolt_colors if bi == 0 else neon_colors

            bottoms = np.zeros(n)
            for ci, (cat_name, op_set) in enumerate(elapsed_categories):
                cat_secs = []
                for wi, wf in enumerate(common_wfs):
                    if wf in wfs:
                        df = wfs[wf]
                        cat_secs.append(
                            df[df.op_type.isin(op_set)]["latency"].sum()
                        )
                    else:
                        cat_secs.append(0)
                # Scale category times so they sum to elapsed_sec
                # (latency totals may differ from wall-clock elapsed)
                for wi, wf in enumerate(common_wfs):
                    if wf in wfs:
                        df = wfs[wf]
                        total_lat = df["latency"].sum()
                        if total_lat > 0:
                            cat_secs[wi] = (
                                cat_secs[wi] / total_lat * elapsed_vals[wi]
                            )

                # Add hatching to branch ops (ci == 0)
                hatch = "///" if ci == 0 else None
                bars = ax_time.bar(
                    positions,
                    cat_secs,
                    bar_w,
                    bottom=bottoms,
                    color=colors[ci],
                    alpha=0.9,
                    edgecolor="white",
                    linewidth=0.5,
                    hatch=hatch,
                )
                bottoms += np.array(cat_secs)

            # Total label on top of stacked bar
            for wi, v in enumerate(elapsed_vals):
                if v > 0:
                    wf = common_wfs[wi]
                    bx = positions[wi]
                    # Seconds (black) just above the bar
                    ax_time.annotate(
                        f"{v:.1f}s",
                        xy=(bx, bottoms[wi]),
                        xytext=(0, 2),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                        fontsize=13,
                        fontweight="bold",
                        color="black",
                    )
        else:
            # Fallback: solid bar when no parquet data available
            bars = ax_time.bar(
                positions,
                elapsed_vals,
                bar_w,
                color=base_color,
                alpha=0.9,
                edgecolor="black",
                linewidth=0.8,
            )
            for wi, (bar, v) in enumerate(zip(bars, elapsed_vals)):
                if v > 0:
                    bx = bar.get_x() + bar.get_width() / 2
                    ax_time.annotate(
                        f"{v:.1f}s",
                        xy=(bx, v),
                        xytext=(0, 2),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                        fontsize=13,
                        fontweight="bold",
                        color="black",
                    )

    # Build legend with color-coded categories
    legend_handles = [
        Patch(
            facecolor=dolt_colors[0],
            alpha=0.9,
            label="Dolt Branch Ops",
            edgecolor="white",
            linewidth=0.5,
            hatch="///",
        ),
        Patch(
            facecolor=dolt_colors[1],
            alpha=0.9,
            label="Dolt Data Ops",
            edgecolor="white",
            linewidth=0.5,
        ),
        Patch(
            facecolor=neon_colors[0],
            alpha=0.9,
            label="Neon Branch Ops",
            edgecolor="white",
            linewidth=0.5,
            hatch="///",
        ),
        Patch(
            facecolor=neon_colors[1],
            alpha=0.9,
            label="Neon Data Ops",
            edgecolor="white",
            linewidth=0.5,
        ),
    ]
    ax_time.legend(
        handles=legend_handles,
        fontsize=14,
        framealpha=0.9,
        loc="upper left",
    )

    # X-axis: two groups (Dolt and Neon) with workflow labels under each bar
    all_positions = np.concatenate([dolt_positions, neon_positions])
    all_labels = [WORKFLOW_LABELS.get(wf, wf) for wf in common_wfs] * 2
    ax_time.set_xticks(all_positions)
    ax_time.set_xticklabels(all_labels, fontsize=16, rotation=30, ha="right")

    # Add system labels (Dolt, Neon) outside plot, below x-axis labels
    dolt_center = dolt_positions.mean()
    neon_center = neon_positions.mean()
    ax_time.text(
        dolt_center,
        -0.18,
        "Dolt",
        ha="center",
        va="top",
        fontsize=24,
        fontweight="bold",
        transform=ax_time.get_xaxis_transform(),
    )
    ax_time.text(
        neon_center,
        -0.18,
        "Neon",
        ha="center",
        va="top",
        fontsize=24,
        fontweight="bold",
        transform=ax_time.get_xaxis_transform(),
    )

    ax_time.set_ylabel("Elapsed Time (s)", fontsize=16)
    # Use cube root scale - compresses large values more than sqrt, better for wide ranges
    ax_time.set_yscale(
        "function", functions=(lambda x: np.cbrt(x), lambda x: x**3)
    )
    # Expand y-axis top to avoid clipping bar annotations
    y_lo, y_hi = ax_time.get_ylim()
    ax_time.set_ylim(0, y_hi * 1.2)
    ax_time.tick_params(axis="both", which="major", labelsize=14)
    ax_time.grid(True, alpha=0.3, axis="y")

    # Right: storage_delta_bytes (same grouping as elapsed time)
    for bi, (label, data, color, positions) in enumerate(
        [
            ("Dolt", dolt_stor, BACKEND_COLORS["Dolt"], dolt_positions),
            ("Neon", neon_stor, BACKEND_COLORS["Neon"], neon_positions),
        ]
    ):
        vals_bytes = [
            data[wf]["storage_delta_bytes"] if wf in data else 0
            for wf in common_wfs
        ]
        vals_mb = [v / (1024 * 1024) for v in vals_bytes]
        # Use consistent blue/red colors (dark shades for storage)
        stor_color = dolt_colors[1] if bi == 0 else neon_colors[1]
        bars = ax_stor.bar(
            positions,
            vals_mb,
            bar_w,
            color=stor_color,
            alpha=0.9,
            edgecolor="white",
            linewidth=0.5,
            label=label,
        )
        for bar, vb, vm in zip(bars, vals_bytes, vals_mb):
            if vb > 0:
                ax_stor.text(
                    bar.get_x() + bar.get_width() / 2,
                    vm,
                    _human_size(vb),
                    ha="center",
                    va="bottom",
                    fontsize=13,
                    fontweight="bold",
                    clip_on=True,
                )

    # X-axis: two groups (Dolt and Neon) with workflow labels
    ax_stor.set_xticks(all_positions)
    ax_stor.set_xticklabels(all_labels, fontsize=16, rotation=30, ha="right")

    # Add system labels (Dolt, Neon) outside plot, below x-axis labels
    ax_stor.text(
        dolt_center,
        -0.18,
        "Dolt",
        ha="center",
        va="top",
        fontsize=24,
        fontweight="bold",
        transform=ax_stor.get_xaxis_transform(),
    )
    ax_stor.text(
        neon_center,
        -0.18,
        "Neon",
        ha="center",
        va="top",
        fontsize=24,
        fontweight="bold",
        transform=ax_stor.get_xaxis_transform(),
    )

    ax_stor.set_ylabel("Storage Delta (MB)", fontsize=16)
    ax_stor.set_yscale("log")
    ax_stor.tick_params(axis="both", which="major", labelsize=14)
    ax_stor.grid(True, alpha=0.3, axis="y")
    ax_stor.legend(fontsize=14, framealpha=0.9)

    fig.tight_layout()
    path = os.path.join(outdir, "storage_elapsed_comparison.png")
    fig.savefig(path, dpi=150)
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 7: Steps completion over time ──────────────────────────────


def plot_steps_over_time(
    dolt_wfs,
    neon_wfs,
    outdir,
    dolt_stor=None,
    neon_stor=None,
    label_position=None,
    label_fontsize=16,
):
    """Line plot showing total completed steps over time for each workflow,
    Dolt vs Neon overlaid.

    A step is considered "completed" when the last operation for that
    (thread_id, step_id) pair finishes. Time is reconstructed by cumulative
    sum of operation latencies.

    Args:
        dolt_wfs: Dict of workflow DataFrames for Dolt
        neon_wfs: Dict of workflow DataFrames for Neon
        outdir: Output directory for figures
        dolt_stor: Dict of e2e_stats for Dolt (optional)
        neon_stor: Dict of e2e_stats for Neon (optional)
    """
    # Include workflows that have step_id in at least one backend
    included_wfs = []
    for wf in WORKFLOW_ORDER:
        dolt_has = wf in dolt_wfs and "step_id" in dolt_wfs[wf].columns
        neon_has = wf in neon_wfs and "step_id" in neon_wfs[wf].columns
        if dolt_has or neon_has:
            included_wfs.append(wf)
            if dolt_has and not neon_has:
                print(f"  Note: {wf} only in Dolt")
            elif neon_has and not dolt_has:
                print(f"  Note: {wf} only in Neon")

    if not included_wfs:
        print(
            "  No workflows with step_id found, skipping steps over time plot."
        )
        return

    ncols = 2
    nrows = (len(included_wfs) + ncols - 1) // ncols
    fig, axes = plt.subplots(
        nrows,
        ncols,
        figsize=(9 * ncols, 5 * nrows),
        squeeze=False,
    )
    axes_flat = axes.flatten()

    for idx, wf in enumerate(included_wfs):
        ax = axes_flat[idx]

        # Get total expected steps from e2e_stats (same for both backends)
        total_steps = None
        if dolt_stor and wf in dolt_stor:
            total_steps = _total_steps(dolt_stor[wf])
        elif neon_stor and wf in neon_stor:
            total_steps = _total_steps(neon_stor[wf])

        for backend_label, wfs, color, linestyle in [
            ("Dolt", dolt_wfs, BACKEND_COLORS["Dolt"], "-"),
            ("Neon", neon_wfs, BACKEND_COLORS["Neon"], "--"),
        ]:
            # Skip if workflow doesn't exist in this backend
            if wf not in wfs:
                continue

            df = wfs[wf]

            # Skip if workflow doesn't have step_id column
            if "step_id" not in df.columns:
                continue

            # Only include operations that are part of a step (step_id >= 0)
            df_steps = df[df["step_id"] >= 0].copy()

            if len(df_steps) == 0:
                print(
                    f"  Warning: {backend_label} {wf} has no operations with step_id >= 0"
                )
                continue

            # Sort by thread_id and iteration_number to compute wall-clock time per thread
            df_steps = df_steps.sort_values(
                ["thread_id", "iteration_number"]
            ).copy()

            # Compute wall-clock time: cumulative latency within each thread
            # (assumes all threads start at time 0)
            df_steps["wall_clock_time"] = df_steps.groupby("thread_id")[
                "latency"
            ].cumsum()

            # Find the last operation for each (thread_id, step_id) pair
            # Group by (thread_id, step_id) and get the index of the last row
            last_ops = (
                df_steps.groupby(["thread_id", "step_id"])
                .tail(1)
                .sort_values(
                    "wall_clock_time"
                )  # Sort by wall-clock completion time
            )

            # Extract step completion times (wall-clock time)
            completion_times = []
            for _, row in last_ops.iterrows():
                # Get wall-clock time at this operation
                wall_time = df_steps.loc[
                    df_steps["iteration_number"] == row["iteration_number"],
                    "wall_clock_time",
                ].values[0]
                completion_times.append(wall_time)

            completion_times = sorted(completion_times)

            # Debug output
            num_steps = len(completion_times)
            if num_steps > 0:
                print(
                    f"  {wf} ({backend_label}): {num_steps} steps, max time = {max(completion_times):.2f}s"
                )
            else:
                print(
                    f"  Warning: {wf} ({backend_label}) has 0 completed steps"
                )

            # Build step function: (time, count)
            times = [0] + completion_times
            counts = list(range(len(completion_times) + 1))

            ax.plot(
                times,
                counts,
                color=color,
                linestyle=linestyle,
                linewidth=2.5,
                label=backend_label,
                drawstyle="steps-post",
            )

            # Store label info for bottom right corner placement
            if len(times) > 1 and total_steps is not None:
                final_steps = counts[-1]
                label_text = f"{backend_label}: {final_steps}/{total_steps}"

                # Store for later placement (will be added after both backends are plotted)
                if not hasattr(ax, "_step_labels"):
                    ax._step_labels = []
                ax._step_labels.append((label_text, color, linestyle))

        # Add labels (default: bottom right corner)
        if hasattr(ax, "_step_labels"):
            # Parse label position (format: "x,y" where x,y are in axes coordinates)
            if label_position:
                try:
                    x_pos, y_start = map(float, label_position.split(","))
                except ValueError:
                    print(
                        f"  Warning: Invalid label position '{label_position}', using default"
                    )
                    x_pos, y_start = 0.98, 0.05
            else:
                x_pos, y_start = 0.98, 0.05

            # Determine horizontal alignment based on x position
            ha = "right" if x_pos > 0.5 else "left"

            for i, (label_text, color, lstyle) in enumerate(ax._step_labels):
                ax.text(
                    x_pos,
                    y_start + i * 0.15,
                    label_text,
                    fontsize=label_fontsize,
                    color=color,
                    fontweight="bold",
                    ha=ha,
                    va="bottom",
                    transform=ax.transAxes,
                    bbox=dict(
                        boxstyle="round,pad=0.5",
                        facecolor="white",
                        edgecolor=color,
                        alpha=0.9,
                        linewidth=2,
                        linestyle=lstyle,
                    ),
                )

        ax.set_xlabel("Time Elapsed (s)", fontsize=20)
        ax.set_ylabel("Total Completed Steps", fontsize=20)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=13, framealpha=0.9)
        ax.tick_params(axis="both", which="major", labelsize=12)

        # Set integer ticks on y-axis
        ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    # Hide unused subplots
    for idx in range(len(included_wfs), len(axes_flat)):
        axes_flat[idx].axis("off")

    fig.tight_layout()
    path = os.path.join(outdir, "steps_over_time.png")
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
    # Exclude UNSPECIFIED (0) and API_RETRY_WAIT (9)
    all_ops = sorted(op for op in all_ops if op not in [0, 9])

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
        default="",
        help="Directory with Dolt parquet files.",
    )
    parser.add_argument(
        "--neon-dir",
        type=str,
        default="",
        help="Directory with Neon parquet files.",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="macro-analysis/figures_comparison",
        help="Directory to save figures.",
    )
    parser.add_argument(
        "--label-position",
        type=str,
        help="Position for step labels as 'x,y' in axes coordinates (0-1). Default: '0.98,0.05' (bottom right).",
    )
    parser.add_argument(
        "--label-fontsize",
        type=int,
        default=16,
        help="Font size for step labels. Default: 16.",
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

    # Load storage data
    print("\nLoading storage data...")
    dolt_stor = load_all_storage(args.dolt_dir)
    neon_stor = load_all_storage(args.neon_dir)

    print("\nPlot 5: Storage & elapsed time comparison")
    plot_storage_comparison(
        dolt_stor, neon_stor, args.outdir, dolt_wfs=dolt_wfs, neon_wfs=neon_wfs
    )

    print("\nPlot 6: Steps completion over time")
    plot_steps_over_time(
        dolt_wfs,
        neon_wfs,
        args.outdir,
        dolt_stor,
        neon_stor,
        label_position=args.label_position,
        label_fontsize=args.label_fontsize,
    )

    print_summary(dolt_wfs, neon_wfs)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
