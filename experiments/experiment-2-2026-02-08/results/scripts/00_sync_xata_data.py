#!/usr/bin/env python3
"""Sync finalized Exp2 Xata artifacts into this experiment's local data dir.

Copies Xata Exp2 parquet + summary files from finalized/retry worker outputs in:
  experiments/xata_11proc_runs/<run_tag>/run_stats/<worker_id>/

into:
  experiments/experiment-2-2026-02-08/results/data/

This script does not mutate source files.
"""

from __future__ import annotations

import shutil
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_ROOT = SCRIPT_DIR.parent
DATA_DIR = RESULTS_ROOT / "data"
REPO_ROOT = SCRIPT_DIR.parents[3]
SOURCE_ROOT = REPO_ROOT / "experiments" / "xata_11proc_runs"


SOURCE_WORKERS = [
    ("xata_retry_stopped_20260226_003724", "p04_exp2a_spine"),
    ("xata_retry_manual_20260226_083824", "p05_exp2a_bushy"),
    ("xata_retry_stopped_20260226_003724", "p06_exp2a_fan_out"),
    ("xata_full_20260225_153852", "p07_exp2b_r1_10"),
    ("xata_retry_p08_k9_20260226_091210", "p08_exp2b_r50_100"),
]


def should_copy(path: Path) -> bool:
    name = path.name
    if not name.startswith("xata_tpcc_"):
        return False
    if name.endswith(".parquet"):
        return True
    if name.endswith("_summary.json"):
        return True
    return False


def sync_one(src_dir: Path, dst_dir: Path) -> tuple[int, int]:
    copied = 0
    skipped = 0
    for src in sorted(src_dir.iterdir()):
        if not src.is_file() or not should_copy(src):
            continue
        dst = dst_dir / src.name
        if dst.exists() and dst.stat().st_size == src.stat().st_size:
            skipped += 1
            continue
        shutil.copy2(src, dst)
        copied += 1
    return copied, skipped


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_copied = 0
    total_skipped = 0

    for run_tag, worker_id in SOURCE_WORKERS:
        src_dir = SOURCE_ROOT / run_tag / "run_stats" / worker_id
        if not src_dir.exists():
            raise FileNotFoundError(f"Missing source directory: {src_dir}")
        copied, skipped = sync_one(src_dir, DATA_DIR)
        total_copied += copied
        total_skipped += skipped
        print(
            f"[{worker_id}] copied={copied} skipped={skipped} "
            f"from {src_dir}"
        )

    xata_parquet = list(DATA_DIR.glob("xata_tpcc_*.parquet"))
    print()
    print(f"Sync complete. copied={total_copied} skipped={total_skipped}")
    print(f"Local Xata parquet files in data dir: {len(xata_parquet)}")
    print(f"Destination: {DATA_DIR}")


if __name__ == "__main__":
    main()
