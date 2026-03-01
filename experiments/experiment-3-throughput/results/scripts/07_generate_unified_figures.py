#!/usr/bin/env python3
"""Generate per-backend individual figures for Exp3 REPORT.md 2x2 grids.

Produces one PNG per (backend, metric) combination:
  fig_unified_{metric}_{backend}.png

Metrics:
  rq3a  - Branch-create throughput (ops/s)
  rq3b  - CRUD aggregate goodput (ops/s)
  rq3c  - Mean per-thread goodput (ops/s/thread)
  rq3d_cv   - Fairness CV
  rq3d_zero - Zero-throughput threads
  rq3e  - Failure rate (branch + crud panels)

Non-Xata data: computed from raw parquets in --data-dir.
Xata data: pre-aggregated RQ parquets in --xata-rq-dir.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BACKEND_ORDER = ["dolt", "file_copy", "neon", "xata"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABEL = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
BACKEND_LABEL = {"dolt": "Dolt", "file_copy": "file_copy", "neon": "Neon", "xata": "Xata"}
TOPO_COLOR = {"spine": "#c0392b", "bushy": "#1f77b4", "fan_out": "#2ca02c"}

NON_XATA_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_tpcc\.parquet$"
)

DURATION = 30.0


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_non_xata_raw(data_dir: Path) -> pd.DataFrame:
    """Load non-Xata raw parquets, return per-run aggregates."""
    records = []
    for p in sorted(data_dir.glob("exp3_*_tpcc.parquet")):
        m = NON_XATA_RE.match(p.name)
        if not m:
            continue
        backend, topo, threads_s, mode = m.group(1), m.group(2), m.group(3), m.group(4)
        threads = int(threads_s)

        cols = ["thread_id", "outcome_success", "failure_reason"]
        try:
            df = pd.read_parquet(p, columns=cols)
        except Exception:
            continue

        df["outcome_success"] = df["outcome_success"].fillna(False).astype(bool)
        attempted = len(df)
        successful = int(df["outcome_success"].sum())
        failed = attempted - successful

        # per-thread fairness (only meaningful for crud)
        mean_pt_goodput = np.nan
        cv = np.nan
        zero_threads = np.nan
        if mode == "crud" and threads > 0:
            succ_per_thread = (
                df[df["outcome_success"]]
                .groupby("thread_id")
                .size()
                .reindex(range(threads), fill_value=0)
                .astype(float)
            )
            pt_goodput = succ_per_thread / DURATION
            mean_pt_goodput = float(pt_goodput.mean())
            cv = float(pt_goodput.std(ddof=0) / mean_pt_goodput) if mean_pt_goodput > 0 else 0.0
            zero_threads = int((pt_goodput == 0).sum())

        # failure categorisation
        fail_df = df[~df["outcome_success"]]
        fail_reason = fail_df["failure_reason"].fillna("").astype(str)
        failed_slow = int(fail_reason.str.startswith("Slow operation:").sum())
        failed_exception = failed - failed_slow

        records.append({
            "backend": backend,
            "topology": topo,
            "threads": threads,
            "mode": mode,
            "attempted_ops": attempted,
            "successful_ops": successful,
            "failed_ops": failed,
            "failed_exception_ops": failed_exception,
            "failed_slow_ops": failed_slow,
            "goodput": successful / DURATION,
            "failure_rate": failed / attempted if attempted > 0 else 0.0,
            "mean_per_thread_goodput": mean_pt_goodput,
            "fairness_cv": cv,
            "zero_throughput_threads": zero_threads,
        })
    return pd.DataFrame(records)


def load_xata_rq(rq_dir: Path) -> dict[str, pd.DataFrame]:
    """Load Xata consolidated RQ parquets."""
    out = {}
    for name in ["exp3_rq3a_branch_throughput", "exp3_rq3b_crud_aggregate_goodput",
                  "exp3_rq3c_per_thread_fairness", "exp3_rq3d_failure_composition"]:
        p = rq_dir / f"{name}.parquet"
        if p.exists():
            out[name] = pd.read_parquet(p)
    return out


def build_xata_unified(xata_rqs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Convert Xata RQ parquets into the same shape as non-Xata raw aggregates."""
    records = []

    # rq3a: branch throughput
    rq3a = xata_rqs.get("exp3_rq3a_branch_throughput", pd.DataFrame())
    if not rq3a.empty:
        for _, r in rq3a.iterrows():
            records.append({
                "backend": "xata", "topology": r["topology"], "threads": int(r["threads"]),
                "mode": "branch",
                "attempted_ops": int(r.get("attempted_ops", 0)),
                "successful_ops": int(r.get("successful_ops", 0)),
                "failed_ops": int(r.get("attempted_ops", 0)) - int(r.get("successful_ops", 0)),
                "goodput": float(r.get("goodput_success_ops_per_sec", 0)),
                "failure_rate": 1.0 - float(r.get("success_rate", 0)) if float(r.get("attempted_ops", 0)) > 0 else 0.0,
                "mean_per_thread_goodput": np.nan,
                "fairness_cv": np.nan,
                "zero_throughput_threads": np.nan,
            })

    # rq3b: crud goodput
    rq3b = xata_rqs.get("exp3_rq3b_crud_aggregate_goodput", pd.DataFrame())
    if not rq3b.empty:
        for _, r in rq3b.iterrows():
            records.append({
                "backend": "xata", "topology": r["topology"], "threads": int(r["threads"]),
                "mode": "crud",
                "attempted_ops": int(r.get("attempted_ops", 0)),
                "successful_ops": int(r.get("successful_ops", 0)),
                "failed_ops": int(r.get("attempted_ops", 0)) - int(r.get("successful_ops", 0)),
                "goodput": float(r.get("goodput_success_ops_per_sec", 0)),
                "failure_rate": 1.0 - float(r.get("success_rate", 0)) if float(r.get("attempted_ops", 0)) > 0 else 0.0,
                "mean_per_thread_goodput": np.nan,
                "fairness_cv": np.nan,
                "zero_throughput_threads": np.nan,
            })

    # rq3c: fairness (overwrite fairness cols for crud rows)
    rq3c = xata_rqs.get("exp3_rq3c_per_thread_fairness", pd.DataFrame())
    if not rq3c.empty:
        for _, r in rq3c.iterrows():
            topo, threads = r["topology"], int(r["threads"])
            for rec in records:
                if rec["backend"] == "xata" and rec["topology"] == topo and rec["threads"] == threads and rec["mode"] == "crud":
                    rec["mean_per_thread_goodput"] = float(r.get("mean_per_thread_goodput_ops_per_sec", np.nan))
                    rec["fairness_cv"] = float(r.get("fairness_cv", np.nan))
                    rec["zero_throughput_threads"] = float(r.get("zero_throughput_threads", np.nan))

    # rq3d: failure composition - overwrite failure_rate for both modes
    rq3d = xata_rqs.get("exp3_rq3d_failure_composition", pd.DataFrame())
    if not rq3d.empty:
        for _, r in rq3d.iterrows():
            topo, threads, mode = r["topology"], int(r["threads"]), r["mode"]
            fr = float(r.get("failure_rate_ops", 0))
            for rec in records:
                if rec["backend"] == "xata" and rec["topology"] == topo and rec["threads"] == threads and rec["mode"] == mode:
                    rec["failure_rate"] = fr

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

def _plot_single_backend(
    df: pd.DataFrame,
    backend: str,
    y_col: str,
    ylabel: str,
    title: str,
    out_path: Path,
    *,
    mode_filter: str | None = None,
    ylim_bottom: float | None = 0,
) -> None:
    b = df[df["backend"] == backend]
    if mode_filter:
        b = b[b["mode"] == mode_filter]

    fig, ax = plt.subplots(1, 1, figsize=(5.5, 4.0))
    for topo in TOPO_ORDER:
        t = b[b["topology"] == topo].sort_values("threads")
        if t.empty or t[y_col].isna().all():
            continue
        ax.plot(
            t["threads"], t[y_col],
            marker="o", linewidth=2,
            label=TOPO_LABEL[topo], color=TOPO_COLOR[topo],
        )
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Threads (T)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if ylim_bottom is not None:
        ymax = float(b[y_col].max()) if not b.empty and not b[y_col].isna().all() else 0.0
        if np.isfinite(ymax) and ymax > 0:
            ax.set_ylim(ylim_bottom, ymax * 1.08)
        else:
            ax.set_ylim(ylim_bottom, 1.0)
    ax.grid(alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def _plot_failure_single_backend(
    df: pd.DataFrame,
    backend: str,
    title: str,
    out_path: Path,
) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.0), sharey=True)
    for ax, mode in zip(axes, ["branch", "crud"]):
        sub = df[(df["backend"] == backend) & (df["mode"] == mode)]
        for topo in TOPO_ORDER:
            t = sub[sub["topology"] == topo].sort_values("threads")
            if t.empty:
                continue
            ax.plot(
                t["threads"], t["failure_rate"],
                marker="o", linewidth=2,
                label=TOPO_LABEL[topo], color=TOPO_COLOR[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads (T)")
        ax.set_ylabel("Failure rate")
        ax.set_ylim(-0.02, 1.02)
        ax.set_title(f"{mode}")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
    fig.suptitle(title, fontsize=12, y=1.02)
    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate per-backend Exp3 figures for 2x2 grids")
    parser.add_argument("--data-dir", type=Path,
                        default=Path("experiments/experiment-3-throughput/results/data"))
    parser.add_argument("--xata-rq-dir", type=Path,
                        default=Path("experiments/xata_consolidated/20260226_114337/rq"))
    parser.add_argument("--out-dir", type=Path,
                        default=Path("experiments/experiment-3-throughput/results/figures"))
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    # Load and merge all backends
    non_xata = load_non_xata_raw(args.data_dir)
    xata_rqs = load_xata_rq(args.xata_rq_dir)
    xata = build_xata_unified(xata_rqs)
    all_df = pd.concat([non_xata, xata], ignore_index=True)

    for backend in BACKEND_ORDER:
        label = BACKEND_LABEL[backend]

        # RQ3a: Branch-create throughput
        _plot_single_backend(
            all_df, backend, "goodput", "Branch-create throughput (ops/s)",
            f"{label} — Branch Throughput",
            args.out_dir / f"fig_unified_rq3a_{backend}.png",
            mode_filter="branch",
        )

        # RQ3b: CRUD aggregate goodput
        _plot_single_backend(
            all_df, backend, "goodput", "CRUD aggregate goodput (ops/s)",
            f"{label} — CRUD Goodput",
            args.out_dir / f"fig_unified_rq3b_{backend}.png",
            mode_filter="crud",
        )

        # RQ3c: Mean per-thread goodput
        _plot_single_backend(
            all_df, backend, "mean_per_thread_goodput", "Mean per-thread goodput (ops/s/thread)",
            f"{label} — Per-thread Goodput",
            args.out_dir / f"fig_unified_rq3c_{backend}.png",
            mode_filter="crud",
        )

        # RQ3d-CV: Fairness CV
        _plot_single_backend(
            all_df, backend, "fairness_cv", "Fairness CV (std/mean)",
            f"{label} — Fairness CV",
            args.out_dir / f"fig_unified_rq3d_cv_{backend}.png",
            mode_filter="crud",
            ylim_bottom=None,
        )

        # RQ3d-zero: Zero-throughput threads
        _plot_single_backend(
            all_df, backend, "zero_throughput_threads", "Zero-throughput threads",
            f"{label} — Zero-throughput Threads",
            args.out_dir / f"fig_unified_rq3d_zero_{backend}.png",
            mode_filter="crud",
        )

        # RQ3e: Failure rate (branch + crud side by side)
        _plot_failure_single_backend(
            all_df, backend,
            f"{label} — Failure Rate",
            args.out_dir / f"fig_unified_rq3e_{backend}.png",
        )

    print(f"\nDone. Generated {6 * len(BACKEND_ORDER)} figures in {args.out_dir}")


if __name__ == "__main__":
    main()
