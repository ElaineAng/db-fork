"""Analyze and visualize isolation overhead benchmark results.

Reads the parquet output from isolation_bench.py and produces:
  1. Phase A — Query latency vs. branch count (structural overhead)
  2. Phase B — OLTP throughput baseline vs. concurrent (interference)
  3. Phase B — OLAP latency baseline vs. concurrent (interference)
  4. Phase B — Interference ratio heatmaps

Usage:
    python analysis/isolation_analysis.py [--input /tmp/run_stats/isolation_test.parquet]
                                          [--outdir analysis/figures]
"""

import argparse
import os

import numpy as np
import pyarrow.parquet as pq
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


# ── Helpers ──────────────────────────────────────────────────────────

def _short_query_name(sql):
    """Extract a short label from the SQL query text."""
    sql_lower = sql.strip().lower()
    if "ol_number" in sql_lower and "sum(ol_quantity)" in sql_lower:
        return "Q1 Pricing"
    if "ol_quantity between" in sql_lower and "ol_amount between" in sql_lower:
        return "Q6 Revenue"
    if "o_carrier_id" in sql_lower and "high_line" in sql_lower:
        return "Q12 Shipping"
    if "w.w_name" in sql_lower and "revenue" in sql_lower:
        return "Q5 Warehouse"
    if "low_stock" in sql_lower:
        return "WF What-If"
    if "stddev" in sql_lower and "total_spend" in sql_lower:
        return "WF Curation"
    if "stockouts" in sql_lower or "avg_stock" in sql_lower:
        return "WF Simulation"
    # Fallback: first 30 chars
    return sql.strip()[:30]


def _map_branch_count_label(bc):
    """Map actual branch counts (which include +2 for oltp/olap branches)
    to the logical target count used in the experiment."""
    mapping = {0: 0, 2: 0, 10: 10, 12: 10, 25: 25, 27: 25,
               50: 50, 52: 50, 100: 100, 102: 100, 200: 200, 202: 200}
    return mapping.get(bc, bc)


COLORS = [
    "#2176AE",  # blue
    "#E8554E",  # red
    "#57B894",  # green
    "#F5A623",  # orange
    "#9B59B6",  # purple
    "#3498DB",  # light blue
    "#E67E22",  # dark orange
]

MARKERS = ["o", "s", "^", "D", "v", "P", "X"]


# ── Phase A ──────────────────────────────────────────────────────────

def plot_phase_a(df, outdir):
    """Phase A: Query latency vs. branch count (one line per query)."""
    phase_a = df[df["table_name"] == "phase_a_scaling"].copy()
    if phase_a.empty:
        print("  No Phase A data found, skipping.")
        return

    phase_a["query_label"] = phase_a["sql_query"].apply(_short_query_name)
    phase_a["bc_label"] = phase_a["branch_count"].apply(_map_branch_count_label)

    queries = sorted(phase_a["query_label"].unique())
    branch_counts = sorted(phase_a["bc_label"].unique())

    fig, ax = plt.subplots(figsize=(8, 5))

    for i, q in enumerate(queries):
        subset = phase_a[phase_a["query_label"] == q]
        medians = []
        p25s = []
        p75s = []
        for bc in branch_counts:
            lats = subset[subset["bc_label"] == bc]["latency"] * 1000  # → ms
            medians.append(np.median(lats) if len(lats) > 0 else np.nan)
            p25s.append(np.percentile(lats, 25) if len(lats) > 0 else np.nan)
            p75s.append(np.percentile(lats, 75) if len(lats) > 0 else np.nan)

        color = COLORS[i % len(COLORS)]
        marker = MARKERS[i % len(MARKERS)]
        ax.plot(branch_counts, medians, marker=marker, color=color,
                linewidth=1.8, markersize=6, label=q)
        ax.fill_between(branch_counts, p25s, p75s, alpha=0.12, color=color)

    ax.set_xlabel("Number of Active Branches", fontsize=12)
    ax.set_ylabel("Query Latency (ms)", fontsize=12)
    ax.set_title("Phase A: Structural Overhead — Query Latency vs. Branch Count",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=9, loc="upper left", framealpha=0.9)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(branch_counts)

    fig.tight_layout()
    path = os.path.join(outdir, "phase_a_latency_vs_branches.pdf")
    fig.savefig(path, dpi=150)
    print(f"  Saved {path}")
    plt.close(fig)


# ── Phase B ──────────────────────────────────────────────────────────

def _extract_phase_b_stats(df):
    """Compute per-(branch_count, config) aggregate statistics for Phase B.

    Returns a dict:
        (table_name_prefix, metric_type) -> {bc_label: {stat: value}}

    Where metric_type is 'oltp' or 'olap'.
    """
    phase_b = df[df["table_name"].str.startswith("phase_b_")].copy()
    phase_b["bc_label"] = phase_b["branch_count"].apply(_map_branch_count_label)

    stats = {}
    for tn in phase_b["table_name"].unique():
        subset = phase_b[phase_b["table_name"] == tn]
        for bc in sorted(subset["bc_label"].unique()):
            lats = subset[subset["bc_label"] == bc]["latency"]
            count = len(lats)
            if count == 0:
                continue
            stats.setdefault(tn, {})[bc] = {
                "median": np.median(lats),
                "mean": np.mean(lats),
                "p50": np.percentile(lats, 50),
                "p95": np.percentile(lats, 95),
                "p99": np.percentile(lats, 99),
                "throughput": count / 60.0,  # ops/sec (60s measurement window)
                "count": count,
            }

    return stats


def plot_phase_b_oltp_throughput(stats, outdir):
    """Phase B: OLTP throughput — baseline vs. concurrent at different OLAP thread counts."""
    branch_counts = sorted(
        set(bc for s in stats.values() for bc in s.keys())
    )
    if not branch_counts:
        print("  No Phase B data, skipping OLTP throughput plot.")
        return

    olap_thread_counts = [1, 2, 4]
    n_groups = len(branch_counts)
    n_bars = 1 + len(olap_thread_counts)  # baseline + concurrent per thread count
    bar_width = 0.8 / n_bars
    x = np.arange(n_groups)

    fig, ax = plt.subplots(figsize=(10, 5.5))

    # Baseline
    baseline_key = "phase_b_oltp_baseline_oltp"
    baseline_tp = [
        stats.get(baseline_key, {}).get(bc, {}).get("throughput", 0)
        for bc in branch_counts
    ]
    ax.bar(x - 0.4 + bar_width * 0.5, baseline_tp, bar_width,
           label="OLTP Baseline", color="#2176AE", edgecolor="white", linewidth=0.5)

    for j, nt in enumerate(olap_thread_counts):
        conc_key = f"phase_b_concurrent_{nt}t_oltp"
        conc_tp = [
            stats.get(conc_key, {}).get(bc, {}).get("throughput", 0)
            for bc in branch_counts
        ]
        ax.bar(x - 0.4 + bar_width * (j + 1.5), conc_tp, bar_width,
               label=f"+ {nt} OLAP thread{'s' if nt > 1 else ''}",
               color=COLORS[(j + 1) % len(COLORS)],
               edgecolor="white", linewidth=0.5)

    ax.set_xlabel("Number of Active Branches", fontsize=12)
    ax.set_ylabel("OLTP Throughput (ops/sec)", fontsize=12)
    ax.set_title("Phase B: OLTP Throughput Under OLAP Interference",
                 fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([str(bc) for bc in branch_counts])
    ax.legend(fontsize=9, framealpha=0.9)
    ax.grid(True, alpha=0.3, axis="y")

    fig.tight_layout()
    path = os.path.join(outdir, "phase_b_oltp_throughput.pdf")
    fig.savefig(path, dpi=150)
    print(f"  Saved {path}")
    plt.close(fig)


def plot_phase_b_oltp_latency(stats, outdir):
    """Phase B: OLTP p50 / p95 latency — baseline vs. concurrent."""
    branch_counts = sorted(
        set(bc for s in stats.values() for bc in s.keys())
    )
    if not branch_counts:
        return

    olap_thread_counts = [1, 2, 4]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5), sharey=False)
    for ax_idx, (percentile, stat_key) in enumerate([("p50", "p50"), ("p95", "p95")]):
        ax = axes[ax_idx]
        n_groups = len(branch_counts)
        n_bars = 1 + len(olap_thread_counts)
        bar_width = 0.8 / n_bars
        x = np.arange(n_groups)

        baseline_key = "phase_b_oltp_baseline_oltp"
        baseline_vals = [
            stats.get(baseline_key, {}).get(bc, {}).get(stat_key, 0) * 1000
            for bc in branch_counts
        ]
        ax.bar(x - 0.4 + bar_width * 0.5, baseline_vals, bar_width,
               label="OLTP Baseline", color="#2176AE",
               edgecolor="white", linewidth=0.5)

        for j, nt in enumerate(olap_thread_counts):
            conc_key = f"phase_b_concurrent_{nt}t_oltp"
            conc_vals = [
                stats.get(conc_key, {}).get(bc, {}).get(stat_key, 0) * 1000
                for bc in branch_counts
            ]
            ax.bar(x - 0.4 + bar_width * (j + 1.5), conc_vals, bar_width,
                   label=f"+ {nt} OLAP thread{'s' if nt > 1 else ''}",
                   color=COLORS[(j + 1) % len(COLORS)],
                   edgecolor="white", linewidth=0.5)

        ax.set_xlabel("Number of Active Branches", fontsize=11)
        ax.set_ylabel(f"OLTP {percentile.upper()} Latency (ms)", fontsize=11)
        ax.set_title(f"OLTP {percentile.upper()} Latency", fontsize=12, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([str(bc) for bc in branch_counts])
        ax.legend(fontsize=8, framealpha=0.9)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Phase B: OLTP Latency Under OLAP Interference",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "phase_b_oltp_latency.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def plot_phase_b_olap_latency(stats, outdir):
    """Phase B: OLAP median latency — baseline vs. concurrent, per OLAP thread count."""
    branch_counts = sorted(
        set(bc for s in stats.values() for bc in s.keys())
    )
    if not branch_counts:
        return

    olap_thread_counts = [1, 2, 4]

    fig, axes = plt.subplots(1, len(olap_thread_counts),
                             figsize=(5 * len(olap_thread_counts), 5), sharey=True)
    if len(olap_thread_counts) == 1:
        axes = [axes]

    for ax_idx, nt in enumerate(olap_thread_counts):
        ax = axes[ax_idx]
        x = np.arange(len(branch_counts))
        bar_width = 0.35

        baseline_key = f"phase_b_olap_baseline_{nt}t_olap"
        conc_key = f"phase_b_concurrent_{nt}t_olap"

        baseline_vals = [
            stats.get(baseline_key, {}).get(bc, {}).get("p50", 0) * 1000
            for bc in branch_counts
        ]
        conc_vals = [
            stats.get(conc_key, {}).get(bc, {}).get("p50", 0) * 1000
            for bc in branch_counts
        ]

        ax.bar(x - bar_width / 2, baseline_vals, bar_width,
               label="OLAP Baseline", color="#57B894",
               edgecolor="white", linewidth=0.5)
        ax.bar(x + bar_width / 2, conc_vals, bar_width,
               label="OLAP + OLTP", color="#E8554E",
               edgecolor="white", linewidth=0.5)

        ax.set_xlabel("Active Branches", fontsize=11)
        if ax_idx == 0:
            ax.set_ylabel("OLAP p50 Latency (ms)", fontsize=11)
        ax.set_title(f"{nt} OLAP Thread{'s' if nt > 1 else ''}",
                     fontsize=12, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels([str(bc) for bc in branch_counts])
        ax.legend(fontsize=8, framealpha=0.9)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle("Phase B: OLAP Latency — Baseline vs. Under OLTP Interference",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "phase_b_olap_latency.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def plot_phase_b_interference_heatmap(stats, outdir):
    """Phase B: Interference ratio heatmaps for OLTP and OLAP.

    Interference = (concurrent_metric - baseline_metric) / baseline_metric
    Positive = degradation under interference.
    """
    branch_counts = sorted(
        set(bc for s in stats.values() for bc in s.keys())
    )
    if not branch_counts:
        return

    olap_thread_counts = [1, 2, 4]

    # OLTP interference: throughput drop
    oltp_interference = np.full((len(olap_thread_counts), len(branch_counts)), np.nan)
    baseline_key = "phase_b_oltp_baseline_oltp"
    for i, nt in enumerate(olap_thread_counts):
        conc_key = f"phase_b_concurrent_{nt}t_oltp"
        for j, bc in enumerate(branch_counts):
            b_tp = stats.get(baseline_key, {}).get(bc, {}).get("throughput", 0)
            c_tp = stats.get(conc_key, {}).get(bc, {}).get("throughput", 0)
            if b_tp > 0:
                oltp_interference[i, j] = (b_tp - c_tp) / b_tp * 100  # % drop

    # OLAP interference: latency increase
    olap_interference = np.full((len(olap_thread_counts), len(branch_counts)), np.nan)
    for i, nt in enumerate(olap_thread_counts):
        baseline_key_olap = f"phase_b_olap_baseline_{nt}t_olap"
        conc_key_olap = f"phase_b_concurrent_{nt}t_olap"
        for j, bc in enumerate(branch_counts):
            b_lat = stats.get(baseline_key_olap, {}).get(bc, {}).get("p50", 0)
            c_lat = stats.get(conc_key_olap, {}).get(bc, {}).get("p50", 0)
            if b_lat > 0:
                olap_interference[i, j] = (c_lat - b_lat) / b_lat * 100  # % increase

    fig, axes = plt.subplots(1, 2, figsize=(14, 4))

    for ax, data, title, cmap, fmt_label in [
        (axes[0], oltp_interference,
         "OLTP Throughput Drop (%)\n(higher = more interference)",
         "Reds", "%.1f%%"),
        (axes[1], olap_interference,
         "OLAP Latency Increase (%)\n(higher = more interference)",
         "Oranges", "%.1f%%"),
    ]:
        im = ax.imshow(data, cmap=cmap, aspect="auto",
                       vmin=0, vmax=max(np.nanmax(data), 1))
        ax.set_xticks(range(len(branch_counts)))
        ax.set_xticklabels([str(bc) for bc in branch_counts])
        ax.set_yticks(range(len(olap_thread_counts)))
        ax.set_yticklabels([f"{nt} OLAP" for nt in olap_thread_counts])
        ax.set_xlabel("Active Branches", fontsize=11)
        ax.set_title(title, fontsize=11, fontweight="bold")

        # Annotate cells
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                val = data[i, j]
                if not np.isnan(val):
                    text_color = "white" if val > np.nanmax(data) * 0.6 else "black"
                    ax.text(j, i, f"{val:.1f}%", ha="center", va="center",
                            fontsize=9, color=text_color, fontweight="bold")

        fig.colorbar(im, ax=ax, shrink=0.8)

    fig.suptitle("Phase B: Interference Ratios",
                 fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    path = os.path.join(outdir, "phase_b_interference_heatmap.pdf")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def print_summary_table(stats):
    """Print a text summary table of Phase B interference metrics."""
    branch_counts = sorted(
        set(bc for s in stats.values() for bc in s.keys())
    )
    if not branch_counts:
        return

    olap_thread_counts = [1, 2, 4]

    print("\n" + "=" * 90)
    print("Phase B Summary: Interference Metrics")
    print("=" * 90)

    # OLTP throughput table
    print(f"\n{'OLTP Throughput (ops/s)':^90}")
    header = f"{'Branches':>10} | {'Baseline':>12}"
    for nt in olap_thread_counts:
        header += f" | {f'+{nt} OLAP':>12} {'Δ%':>7}"
    print(header)
    print("-" * len(header))

    baseline_key = "phase_b_oltp_baseline_oltp"
    for bc in branch_counts:
        b_tp = stats.get(baseline_key, {}).get(bc, {}).get("throughput", 0)
        row = f"{bc:>10} | {b_tp:>12.1f}"
        for nt in olap_thread_counts:
            conc_key = f"phase_b_concurrent_{nt}t_oltp"
            c_tp = stats.get(conc_key, {}).get(bc, {}).get("throughput", 0)
            delta = ((c_tp - b_tp) / b_tp * 100) if b_tp > 0 else 0
            row += f" | {c_tp:>12.1f} {delta:>+6.1f}%"
        print(row)

    # OLAP latency table
    print(f"\n{'OLAP p50 Latency (ms)':^90}")
    header = f"{'Branches':>10} | {'Threads':>8}"
    header += f" | {'Baseline':>12} | {'+ OLTP':>12} {'Δ%':>7}"
    print(header)
    print("-" * len(header))

    for bc in branch_counts:
        for nt in olap_thread_counts:
            baseline_olap = f"phase_b_olap_baseline_{nt}t_olap"
            conc_olap = f"phase_b_concurrent_{nt}t_olap"
            b_lat = stats.get(baseline_olap, {}).get(bc, {}).get("p50", 0) * 1000
            c_lat = stats.get(conc_olap, {}).get(bc, {}).get("p50", 0) * 1000
            delta = ((c_lat - b_lat) / b_lat * 100) if b_lat > 0 else 0
            print(f"{bc:>10} | {nt:>8} | {b_lat:>11.3f}ms | {c_lat:>11.3f}ms {delta:>+6.1f}%")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Analyze isolation overhead benchmark results."
    )
    parser.add_argument(
        "--input", type=str,
        default="/tmp/run_stats/isolation_test.parquet",
        help="Path to the parquet file from isolation_bench.py.",
    )
    parser.add_argument(
        "--outdir", type=str,
        default="analysis/figures",
        help="Directory to save figures.",
    )
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print(f"Loading {args.input}...")
    table = pq.read_table(args.input)
    df = table.to_pandas()
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    # ── Phase A ──
    print("\nPhase A: Structural Overhead")
    plot_phase_a(df, args.outdir)

    # ── Phase B ──
    print("\nPhase B: Computing statistics...")
    stats = _extract_phase_b_stats(df)

    print("\nPhase B: OLTP Throughput")
    plot_phase_b_oltp_throughput(stats, args.outdir)

    print("\nPhase B: OLTP Latency")
    plot_phase_b_oltp_latency(stats, args.outdir)

    print("\nPhase B: OLAP Latency")
    plot_phase_b_olap_latency(stats, args.outdir)

    print("\nPhase B: Interference Heatmaps")
    plot_phase_b_interference_heatmap(stats, args.outdir)

    # ── Summary ──
    print_summary_table(stats)

    print(f"\nAll figures saved to {args.outdir}/")


if __name__ == "__main__":
    main()
