"""Analyze and visualize macrobenchmark results across all 5 workflow categories.

Reads per-workflow parquet files and produces:
  1. Latency breakdown by operation type (per workflow)
  2. Throughput comparison across workflows
  3. Branch vs. data operation cost breakdown
  4. Per-step latency timeline (CDF / over iterations)
  5. Combined summary dashboard

Usage:
    python analysis/macro_analysis.py [--indir /tmp/run_stats]
                                      [--outdir analysis/figures_macro]
"""

import argparse
import os

import numpy as np
import pyarrow.parquet as pq
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


# ── Constants ────────────────────────────────────────────────────────

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
    9: "MERGE",
}

# Short labels for plots
OP_SHORT = {
    1: "Create\nBranch",
    2: "Connect\nBranch",
    3: "Read",
    4: "Insert",
    5: "Update",
    6: "Commit",
    7: "DDL",
    8: "Delete\nBranch",
    9: "Merge",
}

WORKFLOW_LABELS = {
    "software_dev": "Software\nDevelopment",
    "failure_repro": "Failure\nReproduction",
    "data_curation": "Data\nCuration",
    "what_if": "What-If\nAnalysis",
    "simulation": "Simulation",
}

WORKFLOW_ORDER = ["software_dev", "failure_repro", "data_curation", "what_if", "simulation"]

# Branch management ops vs data ops
BRANCH_OPS = {1, 2, 8, 9}  # create, connect, delete, merge
DATA_OPS = {3, 4, 5, 7}     # read, insert, update, ddl

COLORS = {
    1: "#2176AE",   # branch create — blue
    2: "#7FBBDC",   # branch connect — light blue
    3: "#57B894",   # read — green
    4: "#F5A623",   # insert — orange
    5: "#E8554E",   # update — red
    6: "#B0B0B0",   # commit — gray
    7: "#9B59B6",   # DDL — purple
    8: "#3498DB",   # branch delete — blue
    9: "#E67E22",   # merge — dark orange
}

WF_COLORS = {
    "software_dev": "#2176AE",
    "failure_repro": "#E8554E",
    "data_curation": "#57B894",
    "what_if": "#F5A623",
    "simulation": "#9B59B6",
}


# ── Data loading ─────────────────────────────────────────────────────

def load_all_workflows(indir, suffix=""):
    """Load all macro_*.parquet files, return dict of {workflow_name: DataFrame}.

    Args:
        indir: directory containing parquet files
        suffix: optional suffix before .parquet, e.g. "_neon" for macro_*_neon.parquet
    """
    workflows = {}
    for wf in WORKFLOW_ORDER:
        path = os.path.join(indir, f"macro_{wf}{suffix}.parquet")
        if os.path.exists(path):
            df = pq.read_table(path).to_pandas()
            workflows[wf] = df
        else:
            print(f"  Warning: {path} not found, skipping.")
    return workflows


# ── Plot 1: Latency by operation type (per workflow) ─────────────────

def plot_latency_by_op(workflows, outdir):
    """Box plots of latency by operation type, one subplot per workflow."""
    n = len(workflows)
    fig, axes = plt.subplots(1, n, figsize=(4 * n, 5), sharey=False)
    if n == 1:
        axes = [axes]

    for ax, wf in zip(axes, WORKFLOW_ORDER):
        if wf not in workflows:
            continue
        df = workflows[wf]

        op_types = sorted(df.op_type.unique())
        op_labels = [OP_SHORT.get(int(op), str(op)) for op in op_types]
        data = []
        colors = []
        for op in op_types:
            lats = df[df.op_type == op]["latency"].values * 1000  # ms
            data.append(lats)
            colors.append(COLORS.get(int(op), "#999999"))

        bp = ax.boxplot(
            data,
            tick_labels=op_labels,
            patch_artist=True,
            widths=0.6,
            showfliers=False,
            medianprops=dict(color="black", linewidth=1.5),
        )
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)

        ax.set_ylabel("Latency (ms)" if ax == axes[0] else "", fontsize=10)
        ax.set_title(WORKFLOW_LABELS.get(wf, wf), fontsize=11, fontweight="bold")
        ax.tick_params(axis="x", labelsize=8)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Operation Latency by Workflow Category",
                 fontsize=14, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "latency_by_op_type.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 2: Throughput comparison ────────────────────────────────────

def plot_throughput(workflows, outdir):
    """Bar chart: total ops/sec and branch ops/sec per workflow."""
    wf_names = [wf for wf in WORKFLOW_ORDER if wf in workflows]

    total_ops = []
    total_times = []
    branch_ops_count = []
    data_ops_count = []

    for wf in wf_names:
        df = workflows[wf]
        total_time = df["latency"].sum()
        total_times.append(total_time)
        total_ops.append(len(df))
        branch_ops_count.append(len(df[df.op_type.isin(BRANCH_OPS)]))
        data_ops_count.append(len(df[df.op_type.isin(DATA_OPS)]))

    throughputs = [n / t if t > 0 else 0 for n, t in zip(total_ops, total_times)]
    branch_throughputs = [n / t if t > 0 else 0
                          for n, t in zip(branch_ops_count, total_times)]
    data_throughputs = [n / t if t > 0 else 0
                        for n, t in zip(data_ops_count, total_times)]

    x = np.arange(len(wf_names))
    bar_width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.bar(x - bar_width, throughputs, bar_width,
           label="Total", color="#2176AE", edgecolor="white", linewidth=0.5)
    ax.bar(x, data_throughputs, bar_width,
           label="Data Ops", color="#57B894", edgecolor="white", linewidth=0.5)
    ax.bar(x + bar_width, branch_throughputs, bar_width,
           label="Branch Ops", color="#F5A623", edgecolor="white", linewidth=0.5)

    ax.set_xlabel("Workflow Category", fontsize=12)
    ax.set_ylabel("Throughput (ops/sec)", fontsize=12)
    ax.set_title("Throughput by Workflow Category",
                 fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([WORKFLOW_LABELS.get(wf, wf).replace("\n", " ") for wf in wf_names])
    ax.legend(fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3, axis="y")

    fig.tight_layout()
    path = os.path.join(outdir, "throughput_by_workflow.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 3: Branch vs. data cost breakdown ───────────────────────────

def plot_cost_breakdown(workflows, outdir):
    """Stacked bar: time spent on branch ops vs data ops per workflow."""
    wf_names = [wf for wf in WORKFLOW_ORDER if wf in workflows]

    branch_times = []
    data_times = []

    for wf in wf_names:
        df = workflows[wf]
        bt = df[df.op_type.isin(BRANCH_OPS)]["latency"].sum() * 1000  # ms
        dt = df[df.op_type.isin(DATA_OPS)]["latency"].sum() * 1000
        branch_times.append(bt)
        data_times.append(dt)

    x = np.arange(len(wf_names))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Absolute time
    ax1.bar(x, data_times, label="Data Ops", color="#57B894",
            edgecolor="white", linewidth=0.5)
    ax1.bar(x, branch_times, bottom=data_times, label="Branch Ops",
            color="#F5A623", edgecolor="white", linewidth=0.5)
    ax1.set_ylabel("Total Time (ms)", fontsize=11)
    ax1.set_title("Absolute Time", fontsize=12, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
                          for wf in wf_names], fontsize=9)
    ax1.legend(fontsize=9, framealpha=0.9)
    ax1.grid(True, alpha=0.3, axis="y")

    # Percentage breakdown
    totals = [b + d for b, d in zip(branch_times, data_times)]
    branch_pcts = [b / t * 100 if t > 0 else 0 for b, t in zip(branch_times, totals)]
    data_pcts = [d / t * 100 if t > 0 else 0 for d, t in zip(data_times, totals)]

    ax2.barh(x, data_pcts, label="Data Ops", color="#57B894",
             edgecolor="white", linewidth=0.5)
    ax2.barh(x, branch_pcts, left=data_pcts, label="Branch Ops",
             color="#F5A623", edgecolor="white", linewidth=0.5)

    # Annotate percentages
    for i in range(len(wf_names)):
        if data_pcts[i] > 8:
            ax2.text(data_pcts[i] / 2, i, f"{data_pcts[i]:.0f}%",
                     ha="center", va="center", fontsize=10, fontweight="bold",
                     color="white")
        if branch_pcts[i] > 8:
            ax2.text(data_pcts[i] + branch_pcts[i] / 2, i,
                     f"{branch_pcts[i]:.0f}%",
                     ha="center", va="center", fontsize=10, fontweight="bold",
                     color="white")

    ax2.set_xlabel("Time (%)", fontsize=11)
    ax2.set_title("Relative Breakdown", fontsize=12, fontweight="bold")
    ax2.set_yticks(x)
    ax2.set_yticklabels([WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
                          for wf in wf_names], fontsize=9)
    ax2.set_xlim(0, 100)
    ax2.legend(fontsize=9, framealpha=0.9, loc="lower right")
    ax2.grid(True, alpha=0.3, axis="x")

    fig.suptitle("Branch Management vs. Data Operation Cost",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "cost_breakdown.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 4: Latency CDF per workflow ─────────────────────────────────

def plot_latency_cdf(workflows, outdir):
    """CDF of all operation latencies, one line per workflow."""
    fig, ax = plt.subplots(figsize=(8, 5))

    for wf in WORKFLOW_ORDER:
        if wf not in workflows:
            continue
        df = workflows[wf]
        lats = np.sort(df["latency"].values * 1000)  # ms
        cdf = np.arange(1, len(lats) + 1) / len(lats)
        label = WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
        ax.plot(lats, cdf, linewidth=2, color=WF_COLORS[wf], label=label)

    ax.set_xlabel("Latency (ms)", fontsize=12)
    ax.set_ylabel("CDF", fontsize=12)
    ax.set_title("Cumulative Latency Distribution by Workflow",
                 fontsize=13, fontweight="bold")
    ax.set_xscale("log")
    ax.legend(fontsize=10, framealpha=0.9, loc="lower right")
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 1.02)

    fig.tight_layout()
    path = os.path.join(outdir, "latency_cdf.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 5: Per-op-type median latency heatmap ───────────────────────

def plot_latency_heatmap(workflows, outdir):
    """Heatmap: rows = workflows, columns = op types, cells = median latency."""
    all_ops = sorted(set(
        int(op) for df in workflows.values() for op in df.op_type.unique()
    ))
    # Filter to non-trivial ops
    all_ops = [op for op in all_ops if op != 0]
    wf_names = [wf for wf in WORKFLOW_ORDER if wf in workflows]

    data = np.full((len(wf_names), len(all_ops)), np.nan)
    counts = np.full((len(wf_names), len(all_ops)), 0)

    for i, wf in enumerate(wf_names):
        df = workflows[wf]
        for j, op in enumerate(all_ops):
            sub = df[df.op_type == op]
            if len(sub) > 0:
                data[i, j] = np.median(sub["latency"].values) * 1000
                counts[i, j] = len(sub)

    fig, ax = plt.subplots(figsize=(12, 4.5))

    # Use log scale for color mapping since latencies span orders of magnitude
    masked_data = np.ma.masked_invalid(data)
    log_data = np.ma.log10(masked_data)

    im = ax.imshow(log_data, cmap="YlOrRd", aspect="auto")

    ax.set_xticks(range(len(all_ops)))
    ax.set_xticklabels([OP_SHORT.get(op, str(op)).replace("\n", " ")
                         for op in all_ops], fontsize=10)
    ax.set_yticks(range(len(wf_names)))
    ax.set_yticklabels([WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
                         for wf in wf_names], fontsize=10)

    # Annotate cells with actual latency values
    for i in range(len(wf_names)):
        for j in range(len(all_ops)):
            if not np.isnan(data[i, j]):
                val = data[i, j]
                n = counts[i, j]
                if val >= 100:
                    text = f"{val:.0f}ms\n(n={n})"
                elif val >= 1:
                    text = f"{val:.1f}ms\n(n={n})"
                else:
                    text = f"{val:.2f}ms\n(n={n})"
                log_val = np.log10(val) if val > 0 else 0
                log_max = np.nanmax(log_data)
                text_color = "white" if log_val > log_max * 0.6 else "black"
                ax.text(j, i, text, ha="center", va="center",
                        fontsize=8, color=text_color, fontweight="bold")
            else:
                ax.text(j, i, "--", ha="center", va="center",
                        fontsize=9, color="#cccccc")

    cbar = fig.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label("log10(median latency in ms)", fontsize=10)

    ax.set_title("Median Latency by Operation Type and Workflow (ms)",
                 fontsize=13, fontweight="bold")

    fig.tight_layout()
    path = os.path.join(outdir, "latency_heatmap.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Plot 6: Per-step timeline ────────────────────────────────────────

def plot_step_timeline(workflows, outdir):
    """Timeline: cumulative latency over iteration number, per workflow.
    Shows how cost accumulates through the workflow steps."""
    fig, axes = plt.subplots(2, 3, figsize=(15, 9))
    axes_flat = axes.flatten()

    for idx, wf in enumerate(WORKFLOW_ORDER):
        if wf not in workflows:
            continue
        ax = axes_flat[idx]
        df = workflows[wf]

        # Plot cumulative latency per thread
        threads = sorted(df.thread_id.unique())
        for tid in threads:
            sub = df[df.thread_id == tid].sort_values("iteration_number")
            cum_lat = sub["latency"].cumsum().values * 1000  # ms
            iters = range(len(cum_lat))

            # Color segments by op type
            ops = sub["op_type"].values
            lats = sub["latency"].values * 1000
            x_prev, y_prev = 0, 0
            for k in range(len(cum_lat)):
                color = COLORS.get(int(ops[k]), "#999999")
                ax.plot([k, k + 1], [y_prev, cum_lat[k]],
                        color=color, linewidth=1.2, alpha=0.7)
                y_prev = cum_lat[k]

        ax.set_xlabel("Operation #", fontsize=10)
        ax.set_ylabel("Cumulative Latency (ms)" if idx % 3 == 0 else "", fontsize=10)
        ax.set_title(WORKFLOW_LABELS.get(wf, wf).replace("\n", " "),
                     fontsize=11, fontweight="bold")
        ax.grid(True, alpha=0.3)

    # Legend in the empty 6th subplot
    ax_legend = axes_flat[5]
    ax_legend.axis("off")
    handles = [Patch(facecolor=COLORS.get(op, "#999"), label=OP_SHORT.get(op, "").replace("\n", " "))
               for op in sorted(COLORS.keys()) if op in OP_SHORT]
    ax_legend.legend(handles=handles, loc="center", fontsize=10, ncol=2,
                     title="Operation Type", title_fontsize=11)

    fig.suptitle("Cumulative Latency Timeline per Workflow",
                 fontsize=14, fontweight="bold", y=1.01)
    fig.tight_layout()
    path = os.path.join(outdir, "step_timeline.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


# ── Summary table ────────────────────────────────────────────────────

def print_summary(workflows, backend_label="Dolt"):
    """Print a text summary table."""
    print("\n" + "=" * 100)
    print(f"Macrobenchmark Summary ({backend_label} Backend)")
    print("=" * 100)

    header = (f"{'Workflow':>20} | {'Ops':>6} | {'Threads':>7} | "
              f"{'Total (ms)':>10} | {'Median (ms)':>11} | {'P95 (ms)':>10} | "
              f"{'Branch %':>8} | {'Ops/sec':>8}")
    print(header)
    print("-" * len(header))

    for wf in WORKFLOW_ORDER:
        if wf not in workflows:
            continue
        df = workflows[wf]
        n_ops = len(df)
        n_threads = df.thread_id.nunique()
        total_ms = df.latency.sum() * 1000
        median_ms = df.latency.median() * 1000
        p95_ms = df.latency.quantile(0.95) * 1000
        branch_ms = df[df.op_type.isin(BRANCH_OPS)].latency.sum() * 1000
        branch_pct = branch_ms / total_ms * 100 if total_ms > 0 else 0
        ops_sec = n_ops / (total_ms / 1000) if total_ms > 0 else 0

        label = WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
        print(f"{label:>20} | {n_ops:>6} | {n_threads:>7} | "
              f"{total_ms:>10.1f} | {median_ms:>11.2f} | {p95_ms:>10.2f} | "
              f"{branch_pct:>7.1f}% | {ops_sec:>8.1f}")

    # Per-op-type summary
    print(f"\n{'Per-Operation-Type Median Latency (ms)':^100}")
    all_ops = sorted(set(
        int(op) for df in workflows.values() for op in df.op_type.unique()
    ))
    all_ops = [op for op in all_ops if op != 0]

    header2 = f"{'Workflow':>20}"
    for op in all_ops:
        header2 += f" | {OP_SHORT.get(op, str(op)).replace(chr(10), ' '):>12}"
    print(header2)
    print("-" * len(header2))

    for wf in WORKFLOW_ORDER:
        if wf not in workflows:
            continue
        df = workflows[wf]
        label = WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
        row = f"{label:>20}"
        for op in all_ops:
            sub = df[df.op_type == op]
            if len(sub) > 0:
                row += f" | {sub.latency.median() * 1000:>11.2f}ms"
            else:
                row += f" | {'--':>12}"
        print(row)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Analyze macrobenchmark results across all 5 workflow categories."
    )
    parser.add_argument(
        "--indir", type=str, default="/tmp/run_stats",
        help="Directory containing macro_*.parquet files.",
    )
    parser.add_argument(
        "--outdir", type=str, default="analysis/figures_macro",
        help="Directory to save figures.",
    )
    parser.add_argument(
        "--suffix", type=str, default="",
        help="File suffix before .parquet, e.g. '_neon' for macro_*_neon.parquet.",
    )
    parser.add_argument(
        "--backend-label", type=str, default=None,
        help="Backend name for titles, e.g. 'Neon' or 'Dolt'. Auto-detected from suffix if not set.",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print("Loading workflow data...")
    workflows = load_all_workflows(args.indir, suffix=args.suffix)
    for wf, df in workflows.items():
        label = WORKFLOW_LABELS.get(wf, wf).replace("\n", " ")
        print(f"  {label}: {len(df):,} rows, {df.thread_id.nunique()} threads")

    print("\nPlot 1: Latency by operation type")
    plot_latency_by_op(workflows, args.outdir)

    print("\nPlot 2: Throughput comparison")
    plot_throughput(workflows, args.outdir)

    print("\nPlot 3: Branch vs. data cost breakdown")
    plot_cost_breakdown(workflows, args.outdir)

    print("\nPlot 4: Latency CDF")
    plot_latency_cdf(workflows, args.outdir)

    print("\nPlot 5: Latency heatmap")
    plot_latency_heatmap(workflows, args.outdir)

    print("\nPlot 6: Step timeline")
    plot_step_timeline(workflows, args.outdir)

    backend_label = args.backend_label
    if not backend_label:
        backend_label = "Neon" if "neon" in args.suffix.lower() else "Dolt"
    print_summary(workflows, backend_label=backend_label)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
