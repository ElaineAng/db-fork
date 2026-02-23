#!/usr/bin/env python3
"""Generate RQ3 fairness-only figures and tables for Exp3.

Focus metrics:
1) Mean per-thread goodput (successful CRUD ops/sec/thread)
2) Fairness CV (std/mean of per-thread goodput)
3) Zero-throughput thread count
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RUN_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_crud_[A-Za-z0-9]+\.parquet$"
)

BACKEND_ORDER = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABEL = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
BACKEND_LABEL = {"dolt": "Dolt", "file_copy": "file_copy", "neon": "Neon"}
TOPO_COLOR = {"spine": "#c0392b", "bushy": "#1f77b4", "fan_out": "#2ca02c"}


@dataclass
class FairnessRow:
    backend: str
    topology: str
    threads: int
    mean_per_thread_goodput: float
    fairness_cv: float
    zero_threads: int
    aggregate_goodput: float


def _fmt(x: float, nd: int = 3) -> str:
    if x is None or not np.isfinite(x):
        return "NA"
    return f"{x:.{nd}f}"


def _md_table(headers: list[str], rows: Iterable[list[str]]) -> str:
    rows = list(rows)
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def _parse_run(path: Path):
    m = RUN_RE.match(path.name)
    if not m:
        return None
    return {
        "backend": m.group(1),
        "topology": m.group(2),
        "threads": int(m.group(3)),
    }


def load_rows(data_dir: Path, duration_seconds: float) -> list[FairnessRow]:
    rows: list[FairnessRow] = []
    for p in sorted(data_dir.glob("exp3_*_crud_*.parquet")):
        meta = _parse_run(p)
        if not meta:
            continue
        threads = int(meta["threads"])
        df = pd.read_parquet(p, columns=["thread_id", "outcome_success"])
        success = df["outcome_success"].fillna(False).astype(bool)
        succ_counts = (
            df[success]
            .groupby("thread_id")
            .size()
            .reindex(range(threads), fill_value=0)
            .astype(float)
        )
        per_thread = succ_counts / float(duration_seconds)

        mean_pt = float(per_thread.mean())
        cv_pt = float(per_thread.std(ddof=0) / mean_pt) if mean_pt > 0 else np.nan
        zero_threads = int((per_thread == 0).sum())
        aggregate = float(per_thread.sum())

        rows.append(
            FairnessRow(
                backend=str(meta["backend"]),
                topology=str(meta["topology"]),
                threads=threads,
                mean_per_thread_goodput=mean_pt,
                fairness_cv=cv_pt,
                zero_threads=zero_threads,
                aggregate_goodput=aggregate,
            )
        )
    return rows


def rows_df(rows: list[FairnessRow]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "backend": r.backend,
                "topology": r.topology,
                "threads": r.threads,
                "mean_per_thread_goodput": r.mean_per_thread_goodput,
                "fairness_cv": r.fairness_cv,
                "zero_threads": r.zero_threads,
                "aggregate_goodput": r.aggregate_goodput,
            }
            for r in rows
        ]
    )


def _plot_metric(
    df: pd.DataFrame,
    out_path: Path,
    *,
    metric: str,
    ylabel: str,
    title: str,
) -> None:
    fig, axes = plt.subplots(1, len(BACKEND_ORDER), figsize=(18, 5), sharey=False)
    for i, backend in enumerate(BACKEND_ORDER):
        ax = axes[i]
        b = df[df["backend"] == backend]
        for topo in TOPO_ORDER:
            t = b[b["topology"] == topo].sort_values("threads")
            if t.empty:
                continue
            ax.plot(
                t["threads"],
                t[metric],
                marker="o",
                linewidth=2,
                label=TOPO_LABEL[topo],
                color=TOPO_COLOR[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads / Branches (N)")
        ax.set_ylabel(ylabel)
        ax.set_title(BACKEND_LABEL.get(backend, backend))
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)

    fig.suptitle(title, fontsize=14, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def generate_figures(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _plot_metric(
        df,
        out_dir / "fig3c_rq3_mean_per_thread_goodput_vs_threads.png",
        metric="mean_per_thread_goodput",
        ylabel="Mean per-thread successful CRUD ops/sec",
        title="Fig 3c. Mean Per-thread Goodput vs Threads (RQ3)",
    )
    _plot_metric(
        df,
        out_dir / "fig3d_rq3_fairness_cv_vs_threads.png",
        metric="fairness_cv",
        ylabel="Fairness CV (std / mean)",
        title="Fig 3d. Fairness CV vs Threads (RQ3)",
    )
    _plot_metric(
        df,
        out_dir / "fig3e_rq3_zero_threads_vs_threads.png",
        metric="zero_threads",
        ylabel="Zero-throughput threads",
        title="Fig 3e. Zero-throughput Threads vs Threads (RQ3)",
    )


def table_max_thread(df: pd.DataFrame) -> str:
    rows = []
    for backend in BACKEND_ORDER:
        b = df[df["backend"] == backend]
        if b.empty:
            continue
        tmax = int(b["threads"].max())
        for topo in TOPO_ORDER:
            cur = b[(b["topology"] == topo) & (b["threads"] == tmax)]
            if cur.empty:
                rows.append([backend, topo, str(tmax), "NA", "NA", "NA"])
                continue
            r = cur.iloc[0]
            rows.append(
                [
                    backend,
                    topo,
                    str(tmax),
                    _fmt(float(r["mean_per_thread_goodput"]), 3),
                    _fmt(float(r["fairness_cv"]), 3),
                    str(int(r["zero_threads"])),
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


def table_topology_spread(df: pd.DataFrame) -> str:
    rows = []
    for backend in BACKEND_ORDER:
        b = df[df["backend"] == backend]
        if b.empty:
            continue
        tmax = int(b["threads"].max())
        cur = b[b["threads"] == tmax]
        if cur.empty:
            continue

        mean_vals = cur["mean_per_thread_goodput"].to_numpy(dtype=float)
        cv_vals = cur["fairness_cv"].to_numpy(dtype=float)
        zero_vals = cur["zero_threads"].to_numpy(dtype=float)

        mean_spread_pct = np.nan
        if np.nanmean(mean_vals) > 0:
            mean_spread_pct = (
                (np.nanmax(mean_vals) - np.nanmin(mean_vals)) / np.nanmean(mean_vals)
            ) * 100.0

        cv_finite = cv_vals[np.isfinite(cv_vals)]
        if cv_finite.size > 0:
            cv_range = f"{_fmt(float(np.min(cv_finite)), 3)} - {_fmt(float(np.max(cv_finite)), 3)}"
        else:
            cv_range = "NA - NA"

        rows.append(
            [
                backend,
                str(tmax),
                _fmt(float(mean_spread_pct), 1) if np.isfinite(mean_spread_pct) else "NA",
                cv_range,
                f"{int(np.nanmin(zero_vals))} - {int(np.nanmax(zero_vals))}",
            ]
        )
    return _md_table(
        [
            "Backend",
            "Tmax",
            "Mean-goodput spread across topology (%)",
            "CV range across topology",
            "Zero-thread range across topology",
        ],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Exp3 RQ3 fairness figures and tables")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/data"),
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/figures"),
    )
    parser.add_argument(
        "--table-out",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/scripts/generated_rq3_fairness_tables.md"),
    )
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    args = parser.parse_args()

    rows = load_rows(args.data_dir, args.duration_seconds)
    if not rows:
        raise SystemExit("No Exp3 CRUD parquet files found.")
    df = rows_df(rows)

    generate_figures(df, args.out_dir)

    table_text = []
    table_text.append("# Generated RQ3 Fairness Tables")
    table_text.append("")
    table_text.append("Definitions:")
    table_text.append("- Mean per-thread goodput = successful CRUD ops / 30s / thread.")
    table_text.append("- CV = std(per-thread goodput) / mean(per-thread goodput).")
    table_text.append("- Zero-throughput threads = number of threads with 0 successful CRUD ops in the run.")
    table_text.append("")
    table_text.append("## Table RQ3-1. Fairness Metrics at Max Thread Count")
    table_text.append("")
    table_text.append(table_max_thread(df))
    table_text.append("")
    table_text.append("## Table RQ3-2. Topology Spread at Max Thread Count")
    table_text.append("")
    table_text.append(table_topology_spread(df))
    table_text.append("")
    text = "\n".join(table_text)

    args.table_out.parent.mkdir(parents=True, exist_ok=True)
    args.table_out.write_text(text, encoding="utf-8")
    print(text)
    print(f"Saved {args.table_out}")


if __name__ == "__main__":
    main()
