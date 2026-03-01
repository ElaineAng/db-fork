#!/usr/bin/env python3
"""Generate Exp3 non-Xata report figure(s).

Current report usage:
- fig3b_crud_aggregate_goodput_vs_threads.png
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BACKEND_ORDER = ["dolt", "file_copy", "neon"]
TOPO_ORDER = ["spine", "bushy", "fan_out"]
TOPO_LABEL = {"spine": "Spine", "bushy": "Bushy", "fan_out": "Fan-out"}
BACKEND_LABEL = {"dolt": "Dolt", "file_copy": "file_copy", "neon": "Neon"}
TOPO_COLOR = {"spine": "#c0392b", "bushy": "#1f77b4", "fan_out": "#2ca02c"}

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
    failed_slow_ops = int(
        failed_df["failure_reason"].str.startswith("Slow operation:", na=False).sum()
    )
    failed_exception_ops = int(failed_ops - failed_slow_ops)
    success_rate = (float(successful_ops) / float(attempted_ops)) if attempted_ops > 0 else 0.0
    return attempted_ops, successful_ops, failed_exception_ops, failed_slow_ops, success_rate


def load_runs(manifest_path: Path, data_dir: Path) -> list[RunRow]:
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

    runs: list[RunRow] = []
    for run_id in sorted(by_run_id.keys()):
        info = by_run_id[run_id]
        manifest_row = info["manifest_row"]

        summary_path = data_dir / f"{run_id}_summary.json"
        summary = {}
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                summary = {}

        if summary:
            attempted_ops = _to_int(summary.get("attempted_ops", manifest_row.get("attempted_ops", 0)))
            successful_ops = _to_int(summary.get("successful_ops", manifest_row.get("successful_ops", 0)))
            failed_exception_ops = _to_int(
                summary.get("failed_exception_ops", manifest_row.get("failed_exception_ops", 0))
            )
            failed_slow_ops = _to_int(summary.get("failed_slow_ops", manifest_row.get("failed_slow_ops", 0)))
            success_rate = _to_float(summary.get("success_rate", manifest_row.get("success_rate", 0.0)))
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
            )
        )

    return runs


def runs_to_df(runs: Iterable[RunRow]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_id": rr.run_id,
                "backend": rr.backend,
                "shape": rr.shape,
                "mode": rr.mode,
                "threads": rr.threads,
                "attempted_ops": rr.attempted_ops,
                "successful_ops": rr.successful_ops,
            }
            for rr in runs
        ]
    )


def plot_fig3b_crud_goodput(df: pd.DataFrame, out_dir: Path, duration: float) -> None:
    sub = df[(df["mode"] == "crud") & (df["backend"].isin(BACKEND_ORDER))].copy()
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Exp3 report figure: non-Xata CRUD aggregate goodput")
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
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    runs = load_runs(args.manifest, args.data_dir)
    df = runs_to_df(runs)
    plot_fig3b_crud_goodput(df, args.out_dir, args.duration_seconds)


if __name__ == "__main__":
    main()
