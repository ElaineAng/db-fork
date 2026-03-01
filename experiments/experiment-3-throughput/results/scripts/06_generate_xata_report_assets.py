#!/usr/bin/env python3
"""Generate Xata-only Exp3 figures and markdown tables for REPORT.md updates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABEL = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
TOPO_COLOR = {"spine": "#c0392b", "bushy": "#1f77b4", "fan_out": "#2ca02c"}
MODE_ORDER = ["branch", "crud"]

EXPECTED_POINTS = 30  # 3 topologies x 2 modes x threads {1,2,4,8,16}


def _fmt(x: float, nd: int = 2) -> str:
    if x is None or not np.isfinite(x):
        return "NA"
    return f"{x:.{nd}f}"


def _fmt_ratio(x: float) -> str:
    if x is None or not np.isfinite(x):
        return "NA"
    return f"{x:.3f}"


def _fmt_int(x) -> str:
    try:
        v = float(x)
    except Exception:
        return "NA"
    if not np.isfinite(v):
        return "NA"
    return str(int(v))


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|")
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return "\n".join(out)


def _top_failure_label(series: pd.Series) -> str:
    counts: dict[str, int] = {}
    for raw in series.dropna().astype(str):
        if not raw or raw == "{}":
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        for k, v in obj.items():
            try:
                counts[str(k)] = counts.get(str(k), 0) + int(v)
            except Exception:
                continue
    if not counts:
        return "NA"
    top_k, top_v = max(counts.items(), key=lambda kv: kv[1])
    return f"{top_k} ({top_v})"


def _plot_mode_lines(
    df: pd.DataFrame, y_col: str, y_label: str, title: str, out_path: Path
) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(7.5, 4.8))
    for topo in TOPO_ORDER:
        sub = df[df["topology"] == topo].sort_values("threads")
        if sub.empty:
            continue
        ax.plot(
            sub["threads"],
            sub[y_col],
            marker="o",
            linewidth=2,
            color=TOPO_COLOR[topo],
            label=TOPO_LABEL[topo],
        )
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Threads (T)")
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.grid(alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def _plot_fairness(df: pd.DataFrame, out_path: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(17, 4.5), sharex=True)
    metrics = [
        ("mean_per_thread_goodput_ops_per_sec", "Mean per-thread goodput"),
        ("fairness_cv", "Fairness CV"),
        ("zero_throughput_threads", "Zero-throughput threads"),
    ]
    for ax, (col, ylabel) in zip(axes, metrics):
        for topo in TOPO_ORDER:
            sub = df[df["topology"] == topo].sort_values("threads")
            if sub.empty:
                continue
            ax.plot(
                sub["threads"],
                sub[col],
                marker="o",
                linewidth=2,
                color=TOPO_COLOR[topo],
                label=TOPO_LABEL[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads (T)")
        ax.set_ylabel(ylabel)
        ax.grid(alpha=0.25)
    axes[0].legend(fontsize=8)
    fig.suptitle("Xata Fairness Metrics vs Threads (Exp3 RQ3c)", y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def _plot_failure_rate(df: pd.DataFrame, out_path: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 4.5), sharey=True)
    for ax, mode in zip(axes, MODE_ORDER):
        subm = df[df["mode"] == mode]
        for topo in TOPO_ORDER:
            sub = subm[subm["topology"] == topo].sort_values("threads")
            if sub.empty:
                continue
            ax.plot(
                sub["threads"],
                sub["failure_rate_ops"],
                marker="o",
                linewidth=2,
                color=TOPO_COLOR[topo],
                label=TOPO_LABEL[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads (T)")
        ax.set_ylabel("Failure rate")
        ax.set_ylim(-0.02, 1.02)
        ax.set_title(f"Xata {mode}")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
    fig.suptitle("Xata Failure Rate vs Threads (Exp3 RQ3d)", y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def _coverage_table(rq3d: pd.DataFrame) -> str:
    uniq = rq3d[["topology", "mode", "threads"]].drop_duplicates()
    represented = int(len(uniq))
    partial = int(rq3d["is_partial_by_summary"].fillna(0).astype(int).sum())
    with_main = int(rq3d["has_main_parquet"].fillna(0).astype(int).sum())
    rows = [
        ["xata", str(EXPECTED_POINTS), str(represented), str(max(EXPECTED_POINTS - represented, 0))],
        ["xata (with main parquet)", str(EXPECTED_POINTS), str(with_main), "NA"],
        ["xata (partial-by-summary rows)", "NA", str(partial), "NA"],
    ]
    return _md_table(["Backend", "Expected points", "Represented points", "Missing points"], rows)


def _summary_table_mode(df: pd.DataFrame, value_col: str, title_kind: str) -> str:
    rows: list[list[str]] = []
    for topo in TOPO_ORDER:
        sub = df[df["topology"] == topo].sort_values("threads")
        if sub.empty:
            rows.append(["xata", topo, "NA", "NA", "NA", "NA"])
            continue
        t1 = sub[sub["threads"] == sub["threads"].min()][value_col].iloc[0]
        best_idx = sub[value_col].idxmax()
        best = sub.loc[best_idx]
        tmax = sub[sub["threads"] == sub["threads"].max()].iloc[0]
        ratio = tmax[value_col] / t1 if t1 > 0 else np.nan
        rows.append(
            [
                "xata",
                topo,
                _fmt(float(t1), 2),
                f"{_fmt(float(best[value_col]), 2)} (T={int(best['threads'])})",
                f"{_fmt(float(tmax[value_col]), 2)} (T={int(tmax['threads'])})",
                _fmt_ratio(float(ratio)) if np.isfinite(ratio) else "NA",
            ]
        )
    return _md_table(
        [
            "Backend",
            "Topology",
            f"T1 {title_kind} (ops/s)",
            f"Peak {title_kind}",
            f"Max-thread {title_kind}",
            "Max/T1",
        ],
        rows,
    )


def _fairness_tmax_table(rq3c: pd.DataFrame) -> str:
    rows: list[list[str]] = []
    for topo in TOPO_ORDER:
        sub = rq3c[rq3c["topology"] == topo].sort_values("threads")
        if sub.empty:
            rows.append(["xata", topo, "NA", "NA", "NA", "NA"])
            continue
        tmax = sub[sub["threads"] == sub["threads"].max()].iloc[0]
        rows.append(
            [
                "xata",
                topo,
                str(int(tmax["threads"])),
                _fmt(float(tmax["mean_per_thread_goodput_ops_per_sec"]), 3),
                _fmt(float(tmax["fairness_cv"]), 3),
                _fmt_int(tmax["zero_throughput_threads"]),
            ]
        )
    return _md_table(
        [
            "Backend",
            "Topology",
            "Tmax",
            "Mean per-thread goodput (ops/s/thread)",
            "CV at Tmax",
            "Zero-throughput threads",
        ],
        rows,
    )


def _failure_mode_table(rq3d: pd.DataFrame) -> str:
    rows: list[list[str]] = []
    for mode in MODE_ORDER:
        sub = rq3d[rq3d["mode"] == mode].copy()
        if sub.empty:
            rows.append(["xata", mode, "0", "0", "0", "NA", "NA"])
            continue
        attempted = int(sub["attempted_ops"].fillna(0).sum())
        successful = int(sub["successful_ops"].fillna(0).sum())
        failed = int(sub["failed_total_ops"].fillna(0).sum())
        fail_rate = (failed / attempted) if attempted > 0 else np.nan
        top_cat = _top_failure_label(sub["failure_by_category_json"])
        rows.append(
            [
                "xata",
                mode,
                f"{attempted:,}",
                f"{successful:,}",
                f"{failed:,}",
                _fmt(float(fail_rate) * 100.0, 2) + "%" if np.isfinite(fail_rate) else "NA",
                top_cat,
            ]
        )
    return _md_table(
        [
            "Backend",
            "Mode",
            "Attempted ops",
            "Successful ops",
            "Failed ops",
            "Failure rate",
            "Top failure category",
        ],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Exp3 Xata report assets")
    parser.add_argument(
        "--rq-dir",
        type=Path,
        default=Path("experiments/xata_consolidated/20260226_114337/rq"),
    )
    parser.add_argument(
        "--figures-dir",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/figures"),
    )
    parser.add_argument(
        "--out-md",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/scripts/generated_xata_exp3_latest.md"),
    )
    args = parser.parse_args()

    rq3a = pd.read_parquet(args.rq_dir / "exp3_rq3a_branch_throughput.parquet")
    rq3b = pd.read_parquet(args.rq_dir / "exp3_rq3b_crud_aggregate_goodput.parquet")
    rq3c = pd.read_parquet(args.rq_dir / "exp3_rq3c_per_thread_fairness.parquet")
    rq3d = pd.read_parquet(args.rq_dir / "exp3_rq3d_failure_composition.parquet")

    rq3a = rq3a[rq3a["backend"] == "xata"].copy()
    rq3b = rq3b[rq3b["backend"] == "xata"].copy()
    rq3c = rq3c[rq3c["backend"] == "xata"].copy()
    rq3d = rq3d[rq3d["backend"] == "xata"].copy()

    args.figures_dir.mkdir(parents=True, exist_ok=True)
    _plot_mode_lines(
        rq3a,
        "goodput_success_ops_per_sec",
        "Successful BRANCH_CREATE ops/sec",
        "Xata Branch Throughput vs Threads (Exp3 RQ3a)",
        args.figures_dir / "fig3a_xata_branch_throughput_vs_threads.png",
    )
    _plot_mode_lines(
        rq3b,
        "goodput_success_ops_per_sec",
        "Aggregate successful CRUD ops/sec",
        "Xata CRUD Aggregate Goodput vs Threads (Exp3 RQ3b)",
        args.figures_dir / "fig3b_xata_crud_aggregate_goodput_vs_threads.png",
    )
    _plot_fairness(
        rq3c,
        args.figures_dir / "fig3cde_xata_fairness_vs_threads.png",
    )
    _plot_failure_rate(
        rq3d,
        args.figures_dir / "fig3f_xata_failure_rate_vs_threads.png",
    )

    text = []
    text.append("# Generated Xata Exp3 Section")
    text.append("")
    text.append("## Table X1. Xata Matrix Coverage")
    text.append("")
    text.append(_coverage_table(rq3d))
    text.append("")
    text.append("## Table X2. Xata Branch Throughput Detailed (RQ3a)")
    text.append("")
    text.append(_summary_table_mode(rq3a, "goodput_success_ops_per_sec", "branch-create throughput"))
    text.append("")
    text.append("## Table X3. Xata CRUD Aggregate Throughput Detailed (RQ3b)")
    text.append("")
    text.append(_summary_table_mode(rq3b, "goodput_success_ops_per_sec", "aggregate CRUD throughput"))
    text.append("")
    text.append("## Table X4. Xata Fairness Metrics at Max Thread Count (RQ3c)")
    text.append("")
    text.append(_fairness_tmax_table(rq3c))
    text.append("")
    text.append("## Table X5. Xata Failure Summary by Mode (RQ3d)")
    text.append("")
    text.append(_failure_mode_table(rq3d))
    text.append("")

    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(text), encoding="utf-8")
    print("\n".join(text))
    print(f"Saved {args.out_md}")


if __name__ == "__main__":
    main()
