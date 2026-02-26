#!/usr/bin/env python3
"""Generate markdown-ready summary tables for Exp2 report updates."""

from __future__ import annotations

import argparse
import glob
import os
import re
from pathlib import Path

import pandas as pd


BACKENDS = ["dolt", "file_copy", "neon", "xata"]
BACKEND_LABELS = {
    "dolt": "Dolt",
    "file_copy": "file_copy",
    "neon": "Neon",
    "xata": "Xata",
}


def fmt_bytes(b: float) -> str:
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.0f} KB"
    return f"{b:.0f} B"


def parse_measurement_filename(filepath: str) -> dict | None:
    stem = Path(filepath).stem
    if stem.endswith("_setup"):
        return None
    m = re.match(
        r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_(update|range_update)(?:_r(\d+))?$",
        stem,
    )
    if not m:
        return None
    return {
        "backend": m.group(1),
        "N": int(m.group(2)),
        "topology": m.group(3),
        "operation": m.group(4).upper(),
        "range_size": int(m.group(5)) if m.group(5) else None,
    }


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    dfs: list[pd.DataFrame] = []
    for f in files:
        meta = parse_measurement_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        df["operation"] = meta["operation"]
        df["range_size"] = meta["range_size"]
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No measurement parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


def print_overview_table(df: pd.DataFrame) -> None:
    print("### RQ1 overview table (paste into REPORT.md)")
    print("| Backend | Total ops | Non-zero deltas | Non-zero fraction |")
    print("|---------|-----------|-----------------|-------------------|")
    for backend in BACKENDS:
        sub = df[df.backend == backend]
        if sub.empty:
            continue
        total = len(sub)
        nz = int((sub.storage_delta != 0).sum())
        frac = nz / total * 100
        print(
            f"| **{BACKEND_LABELS[backend]}** | {total:,} | {nz:,} | {frac:.1f}% |"
        )
    print()


def print_per_key_table(df: pd.DataFrame) -> None:
    print("### Per-key table (spine, all N aggregated)")
    print("| Backend | r=1 | r=10 | r=20 | r=50 | r=100 |")
    print("|---------|-----|------|------|------|-------|")
    ru_spine = df[(df.operation == "RANGE_UPDATE") & (df.topology == "spine")].copy()
    ru_spine["per_key_delta"] = ru_spine["storage_delta"] / ru_spine["num_keys_touched"]
    for backend in BACKENDS:
        bsub = ru_spine[ru_spine.backend == backend]
        if bsub.empty:
            continue
        vals: list[str] = []
        for r in [1, 10, 20, 50, 100]:
            rsub = bsub[bsub.range_size == r]
            vals.append("—" if rsub.empty else fmt_bytes(rsub.per_key_delta.mean()))
        print(f"| {BACKEND_LABELS[backend]} | {vals[0]} | {vals[1]} | {vals[2]} | {vals[3]} | {vals[4]} |")
    print()


def print_xata_digest(df: pd.DataFrame) -> None:
    print("### Xata digest (for narrative updates)")
    xsub = df[df.backend == "xata"]
    if xsub.empty:
        print("No Xata rows found.")
        return

    total = len(xsub)
    nz = int((xsub.storage_delta != 0).sum())
    print(f"- Xata total ops: {total}")
    print(f"- Xata non-zero deltas: {nz} ({nz / total * 100:.1f}%)")

    for op_name, op_filter in [
        ("UPDATE", (xsub.operation == "UPDATE")),
        (
            "RANGE_UPDATE (r=20)",
            (xsub.operation == "RANGE_UPDATE") & (xsub.range_size == 20),
        ),
    ]:
        sub = xsub[op_filter]
        if sub.empty:
            continue
        print(f"- {op_name}:")
        for topo in ["spine", "bushy", "fan_out"]:
            tsub = sub[sub.topology == topo]
            if tsub.empty:
                continue
            by_n = tsub.groupby("N")["storage_delta"].mean().sort_index()
            n_min = int(by_n.index.min())
            n_max = int(by_n.index.max())
            print(
                f"  - {topo}: mean delta N={n_min} {fmt_bytes(by_n.iloc[0])}, "
                f"N={n_max} {fmt_bytes(by_n.iloc[-1])}"
            )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 2 markdown summary")
    parser.add_argument(
        "--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data")
    )
    args = parser.parse_args()
    data_dir = os.path.abspath(args.data_dir)

    df = load_all(data_dir)
    print_overview_table(df)
    print_per_key_table(df)
    print_xata_digest(df)


if __name__ == "__main__":
    main()
