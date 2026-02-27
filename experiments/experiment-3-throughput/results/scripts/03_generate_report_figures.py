#!/usr/bin/env python3
"""Generate consolidated Experiment 3 report figures from manifest + parquet data."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BACKEND_ORDER = ["dolt", "file_copy", "neon"]
MODE_ORDER = ["branch", "crud"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABEL = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
BACKEND_LABEL = {
    "dolt": "Dolt",
    "file_copy": "file_copy",
    "neon": "Neon",
}
TOPO_COLOR = {
    "spine": "#c0392b",
    "bushy": "#1f77b4",
    "fan_out": "#2ca02c",
}

FAILURE_CATEGORY_ORDER = [
    "FAILURE_LOCK_CONTENTION",
    "FAILURE_TIMEOUT",
    "FAILURE_RESOURCE_LIMIT",
    "FAILURE_CONNECTION",
    "FAILURE_BACKEND_STATE_CONFLICT",
    "FAILURE_CONSTRAINT_OR_DATA",
    "FAILURE_INTERNAL_BUG",
    "FAILURE_UNKNOWN",
]
FAILURE_CATEGORY_COLOR = {
    "FAILURE_LOCK_CONTENTION": "#8c564b",
    "FAILURE_TIMEOUT": "#ff7f0e",
    "FAILURE_RESOURCE_LIMIT": "#e377c2",
    "FAILURE_CONNECTION": "#17becf",
    "FAILURE_BACKEND_STATE_CONFLICT": "#9467bd",
    "FAILURE_CONSTRAINT_OR_DATA": "#bcbd22",
    "FAILURE_INTERNAL_BUG": "#7f7f7f",
    "FAILURE_UNKNOWN": "#d62728",
}

BRANCH_CREATE_OP_TYPE = 1
RUN_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_[A-Za-z0-9]+$"
)


@dataclass
class RunRow:
    run_id: str
    backend: str
    shape: str
    mode: str
    threads: int
    attempted_ops: int
    successful_ops: int
    failed_exception_ops: int
    failed_slow_ops: int
    success_rate: float
    summary: dict


def _to_int(value, default=0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_run_id(run_id: str):
    m = RUN_RE.match(run_id)
    if not m:
        return None
    return {
        "backend": m.group(1),
        "shape": m.group(2),
        "threads": int(m.group(3)),
        "mode": m.group(4),
    }


def _count_ops_from_parquet(parquet_path: Path):
    cols = ["outcome_success", "failure_reason"]
    try:
        df = pd.read_parquet(parquet_path, columns=cols)
    except Exception:
        return 0, 0, 0, 0, 0.0

    if "outcome_success" not in df.columns:
        df["outcome_success"] = True
    df["outcome_success"] = df["outcome_success"].fillna(False).astype(bool)

    if "failure_reason" not in df.columns:
        df["failure_reason"] = ""
    df["failure_reason"] = df["failure_reason"].fillna("").astype(str)

    attempted_ops = int(len(df))
    successful_ops = int(df["outcome_success"].sum())
    failed_ops = attempted_ops - successful_ops
    failed_df = df[~df["outcome_success"]]
    failed_slow_ops = int(failed_df["failure_reason"].str.startswith("Slow operation:", na=False).sum())
    failed_exception_ops = int(failed_ops - failed_slow_ops)
    success_rate = (float(successful_ops) / float(attempted_ops)) if attempted_ops > 0 else 0.0
    return attempted_ops, successful_ops, failed_exception_ops, failed_slow_ops, success_rate


def load_runs(manifest_path: Path, data_dir: Path) -> List[RunRow]:
    manifest = pd.DataFrame()
    if manifest_path.exists():
        manifest = pd.read_csv(manifest_path)

    by_run_id = {}
    if not manifest.empty and {"run_id", "backend", "shape", "mode", "threads"}.issubset(manifest.columns):
        for _, r in manifest.iterrows():
            run_id = str(r["run_id"])
            meta = _parse_run_id(run_id)
            if meta is None:
                meta = {
                    "backend": str(r["backend"]),
                    "shape": str(r["shape"]),
                    "mode": str(r["mode"]),
                    "threads": _to_int(r["threads"]),
                }
            by_run_id[run_id] = {
                "run_id": run_id,
                "backend": meta["backend"],
                "shape": meta["shape"],
                "mode": meta["mode"],
                "threads": int(meta["threads"]),
                "manifest_row": r.to_dict(),
            }

    for parquet_path in sorted(data_dir.glob("exp3_*.parquet")):
        if parquet_path.name.endswith("_setup.parquet"):
            continue
        run_id = parquet_path.stem
        meta = _parse_run_id(run_id)
        if meta is None:
            continue
        if run_id not in by_run_id:
            by_run_id[run_id] = {
                "run_id": run_id,
                "backend": meta["backend"],
                "shape": meta["shape"],
                "mode": meta["mode"],
                "threads": int(meta["threads"]),
                "manifest_row": {},
            }

    runs: List[RunRow] = []

    for run_id in sorted(by_run_id.keys()):
        info = by_run_id[run_id]
        r = info["manifest_row"]
        summary_path = data_dir / f"{run_id}_summary.json"
        summary = {}
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                summary = {}

        if summary:
            attempted_ops = _to_int(summary.get("attempted_ops", r.get("attempted_ops", 0)))
            successful_ops = _to_int(summary.get("successful_ops", r.get("successful_ops", 0)))
            failed_exception_ops = _to_int(
                summary.get("failed_exception_ops", r.get("failed_exception_ops", 0))
            )
            failed_slow_ops = _to_int(summary.get("failed_slow_ops", r.get("failed_slow_ops", 0)))
            success_rate = _to_float(summary.get("success_rate", r.get("success_rate", 0.0)))
        else:
            attempted_ops, successful_ops, failed_exception_ops, failed_slow_ops, success_rate = (
                _count_ops_from_parquet(data_dir / f"{run_id}.parquet")
            )

        runs.append(
            RunRow(
                run_id=run_id,
                backend=str(info["backend"]),
                shape=str(info["shape"]),
                mode=str(info["mode"]),
                threads=int(info["threads"]),
                attempted_ops=attempted_ops,
                successful_ops=successful_ops,
                failed_exception_ops=failed_exception_ops,
                failed_slow_ops=failed_slow_ops,
                success_rate=success_rate,
                summary=summary,
            )
        )

    return runs


def runs_to_df(runs: Iterable[RunRow]) -> pd.DataFrame:
    rows = []
    for rr in runs:
        rows.append(
            {
                "run_id": rr.run_id,
                "backend": rr.backend,
                "shape": rr.shape,
                "mode": rr.mode,
                "threads": rr.threads,
                "attempted_ops": rr.attempted_ops,
                "successful_ops": rr.successful_ops,
                "failed_exception_ops": rr.failed_exception_ops,
                "failed_slow_ops": rr.failed_slow_ops,
                "success_rate": rr.success_rate,
                "failure_rate": (
                    (rr.failed_exception_ops + rr.failed_slow_ops) / rr.attempted_ops
                    if rr.attempted_ops > 0
                    else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def plot_fig3a_branch_throughput(
    df: pd.DataFrame,
    data_dir: Path,
    out_dir: Path,
    duration: float,
    *,
    split: bool = False,
) -> None:
    sub = df[df["mode"] == "branch"].copy()

    def _branch_create_throughput(run_id: str) -> float:
        parquet_path = data_dir / f"{run_id}.parquet"
        if not parquet_path.exists():
            return np.nan
        try:
            cols = ["op_type", "outcome_success"]
            run_df = pd.read_parquet(parquet_path, columns=cols)
        except Exception:
            return np.nan

        if "op_type" not in run_df.columns:
            return np.nan
        if "outcome_success" not in run_df.columns:
            run_df["outcome_success"] = True
        run_df["outcome_success"] = run_df["outcome_success"].fillna(False).astype(bool)

        success = ((run_df["op_type"] == BRANCH_CREATE_OP_TYPE) & run_df["outcome_success"]).sum()
        return float(success) / float(duration)

    sub["throughput"] = sub["run_id"].map(_branch_create_throughput)
    sub = sub[np.isfinite(sub["throughput"])]

    def _plot_one_backend(ax, backend):
        b = sub[sub["backend"] == backend]
        for topo in TOPO_ORDER:
            t = b[b["shape"] == topo].sort_values("threads")
            if t.empty:
                continue
            ax.plot(
                t["threads"],
                t["throughput"],
                marker="o",
                linewidth=2,
                label=TOPO_LABEL[topo],
                color=TOPO_COLOR[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads (T)")
        ax.set_ylabel("Successful BRANCH_CREATE ops/sec")
        ax.set_title(BACKEND_LABEL.get(backend, backend))
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)

    fig, axes = plt.subplots(1, len(BACKEND_ORDER), figsize=(18, 5), sharey=False)
    for i, backend in enumerate(BACKEND_ORDER):
        _plot_one_backend(axes[i], backend)

    fig.suptitle("Fig 3a. Branch Creation Throughput vs Threads", fontsize=14, y=1.02)
    fig.tight_layout()
    path = out_dir / "fig3a_branch_throughput_vs_threads.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, sax = plt.subplots(1, 1, figsize=(7, 5))
            _plot_one_backend(sax, backend)
            sfig.suptitle("Fig 3a. Branch Creation Throughput vs Threads", fontsize=14, y=1.02)
            sfig.tight_layout()
            spath = out_dir / f"fig3a_branch_throughput_vs_threads_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def plot_fig3b_crud_goodput(
    df: pd.DataFrame, out_dir: Path, duration: float, *, split: bool = False
) -> None:
    sub = df[df["mode"] == "crud"].copy()
    sub["goodput"] = sub["successful_ops"] / duration

    def _plot_one_backend(ax, backend):
        b = sub[sub["backend"] == backend]

        for topo in TOPO_ORDER:
            t = b[b["shape"] == topo].sort_values("threads")
            if t.empty:
                continue
            ax.plot(
                t["threads"],
                t["goodput"],
                marker="o",
                linewidth=2,
                label=TOPO_LABEL[topo],
                color=TOPO_COLOR[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_xlabel("Threads / Branches (N)")
        ax.set_ylabel("Aggregate successful CRUD ops/sec")
        ax.set_title(BACKEND_LABEL.get(backend, backend))
        ymax = float(b["goodput"].max()) if not b.empty else 0.0
        if np.isfinite(ymax) and ymax > 0:
            ax.set_ylim(0, ymax * 1.08)
        else:
            ax.set_ylim(0, 1.0)
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)

    fig, axes = plt.subplots(1, len(BACKEND_ORDER), figsize=(18, 5), sharey=False)
    for i, backend in enumerate(BACKEND_ORDER):
        _plot_one_backend(axes[i], backend)

    fig.suptitle("Fig 3b. Aggregate CRUD Goodput vs Branch Count", fontsize=14, y=1.02)
    fig.tight_layout()
    path = out_dir / "fig3b_crud_aggregate_goodput_vs_threads.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, sax = plt.subplots(1, 1, figsize=(7, 5))
            _plot_one_backend(sax, backend)
            sfig.suptitle("Fig 3b. Aggregate CRUD Goodput vs Branch Count", fontsize=14, y=1.02)
            sfig.tight_layout()
            spath = out_dir / f"fig3b_crud_aggregate_goodput_vs_threads_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def _load_per_thread_goodput(parquet_path: Path, threads: int, duration: float) -> pd.DataFrame:
    cols = ["thread_id", "outcome_success"]
    df = pd.read_parquet(parquet_path, columns=cols)
    if "outcome_success" not in df.columns:
        df["outcome_success"] = True
    df["outcome_success"] = df["outcome_success"].fillna(False).astype(bool)

    succ = df[df["outcome_success"]]
    counts = succ.groupby("thread_id").size().reindex(range(threads), fill_value=0)

    out = pd.DataFrame({
        "thread_id": counts.index.astype(int),
        "goodput": counts.values.astype(float) / duration,
    })
    return out


def plot_fig3c_distribution_max_threads(
    runs: List[RunRow], data_dir: Path, out_dir: Path, duration: float, *, split: bool = False
) -> None:
    def _plot_one_backend(ax, backend):
        """Plot boxplot for *backend* onto *ax*. Returns False if nothing drawn."""
        b_runs = [r for r in runs if r.backend == backend and r.mode == "crud"]
        if not b_runs:
            ax.set_visible(False)
            return False

        max_t = max(r.threads for r in b_runs)
        values = []
        labels = []

        for topo in TOPO_ORDER:
            rr = next((r for r in b_runs if r.shape == topo and r.threads == max_t), None)
            if rr is None:
                continue
            p = data_dir / f"{rr.run_id}.parquet"
            if not p.exists():
                continue
            per_thread = _load_per_thread_goodput(p, rr.threads, duration)
            values.append(per_thread["goodput"].tolist())
            labels.append(TOPO_LABEL[topo])

        if not values:
            ax.set_visible(False)
            return False

        bp = ax.boxplot(values, patch_artist=True, tick_labels=labels)
        for j, patch in enumerate(bp["boxes"]):
            topo = TOPO_ORDER[j]
            patch.set_facecolor(TOPO_COLOR[topo])
            patch.set_alpha(0.45)

        ax.set_title(f"{BACKEND_LABEL.get(backend, backend)} (Tmax={max_t})")
        ax.set_ylabel("Per-thread successful CRUD ops/sec")
        ax.grid(axis="y", alpha=0.25)
        return True

    fig, axes = plt.subplots(1, len(BACKEND_ORDER), figsize=(18, 5), sharey=False)
    for i, backend in enumerate(BACKEND_ORDER):
        _plot_one_backend(axes[i], backend)

    fig.suptitle("Fig 3c. Per-thread CRUD Throughput Distribution at Max Thread Count", fontsize=14, y=1.02)
    fig.tight_layout()
    path = out_dir / "fig3c_crud_per_thread_distribution_max_threads.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, sax = plt.subplots(1, 1, figsize=(7, 5))
            _plot_one_backend(sax, backend)
            sfig.suptitle(
                "Fig 3c. Per-thread CRUD Throughput Distribution at Max Thread Count",
                fontsize=14, y=1.02,
            )
            sfig.tight_layout()
            spath = out_dir / f"fig3c_crud_per_thread_distribution_max_threads_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def plot_fig3d_spine_vs_thread_index(
    runs: List[RunRow], data_dir: Path, out_dir: Path, duration: float, *, split: bool = False
) -> None:
    def _plot_one_backend(ax, backend):
        """Plot spine per-thread goodput for *backend* onto *ax*. Returns False if nothing drawn."""
        b_runs = [
            r
            for r in runs
            if r.backend == backend and r.mode == "crud" and r.shape == "spine"
        ]
        if not b_runs:
            ax.set_visible(False)
            return False

        # Prefer highest thread count that has a parquet payload.
        rr = None
        p = None
        for cand in sorted(b_runs, key=lambda x: x.threads, reverse=True):
            cand_p = data_dir / f"{cand.run_id}.parquet"
            if cand_p.exists():
                rr = cand
                p = cand_p
                break
        if rr is None or p is None:
            ax.set_visible(False)
            return False

        per_thread = _load_per_thread_goodput(p, rr.threads, duration)
        x = per_thread["thread_id"].values + 1
        y = per_thread["goodput"].values

        ax.plot(x, y, marker="o", linewidth=1.5, color="#34495e")
        ax.set_title(f"{BACKEND_LABEL.get(backend, backend)} spine (T={rr.threads})")
        ax.set_xlabel("Thread index (1-based)")
        ax.set_ylabel("Per-thread successful CRUD ops/sec")
        ax.grid(alpha=0.25)
        return True

    fig, axes = plt.subplots(1, len(BACKEND_ORDER), figsize=(18, 5), sharey=False)
    for i, backend in enumerate(BACKEND_ORDER):
        _plot_one_backend(axes[i], backend)

    fig.suptitle("Fig 3d. Spine Per-thread Goodput vs Thread Index", fontsize=14, y=1.02)
    fig.tight_layout()
    path = out_dir / "fig3d_spine_per_thread_goodput_vs_thread_index.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, sax = plt.subplots(1, 1, figsize=(7, 5))
            _plot_one_backend(sax, backend)
            sfig.suptitle("Fig 3d. Spine Per-thread Goodput vs Thread Index", fontsize=14, y=1.02)
            sfig.tight_layout()
            spath = out_dir / f"fig3d_spine_per_thread_goodput_vs_thread_index_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def plot_fig3e_failure_rate(df: pd.DataFrame, out_dir: Path, *, split: bool = False) -> None:
    def _plot_one_cell(ax, mode, backend, show_ylabel=True):
        sub = df[(df["mode"] == mode) & (df["backend"] == backend)]
        for topo in TOPO_ORDER:
            t = sub[sub["shape"] == topo].sort_values("threads")
            if t.empty:
                continue
            ax.plot(
                t["threads"],
                t["failure_rate"],
                marker="o",
                linewidth=2,
                color=TOPO_COLOR[topo],
                label=TOPO_LABEL[topo],
            )
        ax.set_xscale("log", base=2)
        ax.set_ylim(-0.02, 1.02)
        ax.set_title(f"{BACKEND_LABEL.get(backend, backend)} / {mode}")
        ax.set_xlabel("Threads")
        if show_ylabel:
            ax.set_ylabel("Failure rate")
        ax.grid(alpha=0.25)
        ax.legend(fontsize=7)

    fig, axes = plt.subplots(len(MODE_ORDER), len(BACKEND_ORDER), figsize=(18, 9), sharey=True)
    for r_i, mode in enumerate(MODE_ORDER):
        for c_i, backend in enumerate(BACKEND_ORDER):
            _plot_one_cell(axes[r_i][c_i], mode, backend, show_ylabel=(c_i == 0))

    fig.suptitle("Fig 3e. Failure Rate vs Threads (by mode/topology/backend)", fontsize=14, y=1.01)
    fig.tight_layout()
    path = out_dir / "fig3e_failure_rate_vs_threads.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, saxes = plt.subplots(len(MODE_ORDER), 1, figsize=(7, 9), sharey=True)
            for r_i, mode in enumerate(MODE_ORDER):
                _plot_one_cell(saxes[r_i], mode, backend, show_ylabel=True)
            sfig.suptitle(
                "Fig 3e. Failure Rate vs Threads (by mode/topology/backend)",
                fontsize=14, y=1.01,
            )
            sfig.tight_layout()
            spath = out_dir / f"fig3e_failure_rate_vs_threads_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def _failure_counts_from_summary(summary: Dict) -> Dict[str, int]:
    by_cat = summary.get("failure_by_category", {})
    out: Dict[str, int] = {}
    for k, v in by_cat.items():
        if k == "FAILURE_NONE":
            continue
        out[str(k)] = _to_int(v)
    return out


def plot_fig3f_failure_category_stack(runs: List[RunRow], out_dir: Path, *, split: bool = False) -> None:
    def _plot_one_cell(ax, mode, backend, show_ylabel=True):
        candidates = [r for r in runs if r.backend == backend and r.mode == mode]
        if not candidates:
            ax.set_visible(False)
            return

        max_t = max(r.threads for r in candidates)
        rows = [r for r in candidates if r.threads == max_t]
        rows.sort(key=lambda x: TOPO_ORDER.index(x.shape))

        x = np.arange(len(rows))
        bottoms = np.zeros(len(rows))
        xlabels = [TOPO_LABEL[r.shape] for r in rows]

        for cat in FAILURE_CATEGORY_ORDER:
            vals = []
            for rr in rows:
                counts = _failure_counts_from_summary(rr.summary)
                vals.append(counts.get(cat, 0))
            if not any(vals):
                continue
            ax.bar(
                x,
                vals,
                bottom=bottoms,
                color=FAILURE_CATEGORY_COLOR[cat],
                label=cat.replace("FAILURE_", ""),
            )
            bottoms += np.array(vals)

        ax.set_xticks(x)
        ax.set_xticklabels(xlabels)
        ax.set_title(f"{BACKEND_LABEL.get(backend, backend)} / {mode} (Tmax={max_t})")
        if show_ylabel:
            ax.set_ylabel("Failed op count")
        ax.grid(axis="y", alpha=0.25)

    def _dedup_legend(axes_list):
        """Return deduplicated (handles, labels) across an iterable of axes."""
        handles, labels = [], []
        seen = set()
        for ax in axes_list:
            h, l = ax.get_legend_handles_labels()
            for hh, ll in zip(h, l):
                if ll not in seen:
                    seen.add(ll)
                    handles.append(hh)
                    labels.append(ll)
        return handles, labels

    fig, axes = plt.subplots(len(MODE_ORDER), len(BACKEND_ORDER), figsize=(18, 9), sharey=True)
    for r_i, mode in enumerate(MODE_ORDER):
        for c_i, backend in enumerate(BACKEND_ORDER):
            _plot_one_cell(axes[r_i][c_i], mode, backend, show_ylabel=(c_i == 0))

    # Deduplicate legend entries from all axes.
    handles, labels = _dedup_legend(ax for row_axes in axes for ax in row_axes)
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=4, fontsize=8)

    fig.suptitle("Fig 3f. Failure Category Composition at Max Thread Count", fontsize=14, y=1.03)
    fig.tight_layout()
    path = out_dir / "fig3f_failure_reason_stack_max_threads.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {path}")

    if split:
        for backend in BACKEND_ORDER:
            sfig, saxes = plt.subplots(len(MODE_ORDER), 1, figsize=(7, 9), sharey=True)
            for r_i, mode in enumerate(MODE_ORDER):
                _plot_one_cell(saxes[r_i], mode, backend, show_ylabel=True)

            handles, labels = _dedup_legend(saxes)
            if handles:
                sfig.legend(handles, labels, loc="upper center", ncol=4, fontsize=8)

            sfig.suptitle(
                "Fig 3f. Failure Category Composition at Max Thread Count",
                fontsize=14, y=1.03,
            )
            sfig.tight_layout()
            spath = out_dir / f"fig3f_failure_reason_stack_max_threads_{backend}.png"
            sfig.savefig(spath, dpi=180, bbox_inches="tight")
            plt.close(sfig)
            print(f"Saved {spath}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Exp3 report figures")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/run_manifest.csv"),
    )
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
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--split", action="store_true")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    runs = load_runs(args.manifest, args.data_dir)
    df = runs_to_df(runs)

    plot_fig3a_branch_throughput(
        df, args.data_dir, args.out_dir, args.duration_seconds, split=args.split
    )
    plot_fig3b_crud_goodput(df, args.out_dir, args.duration_seconds, split=args.split)
    plot_fig3c_distribution_max_threads(runs, args.data_dir, args.out_dir, args.duration_seconds, split=args.split)
    plot_fig3d_spine_vs_thread_index(runs, args.data_dir, args.out_dir, args.duration_seconds, split=args.split)
    plot_fig3e_failure_rate(df, args.out_dir, split=args.split)
    plot_fig3f_failure_category_stack(runs, args.out_dir, split=args.split)


if __name__ == "__main__":
    main()
