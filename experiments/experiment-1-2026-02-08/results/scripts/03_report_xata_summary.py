#!/usr/bin/env python3
"""Generate markdown-ready summary tables/snippets for Exp1 Xata report updates."""

from __future__ import annotations

import argparse
import glob
import os
import re
from pathlib import Path

import pandas as pd


def fmt_bytes(b: float) -> str:
    if abs(b) >= 1 << 30:
        return f"{b / (1 << 30):.2f} GB"
    if abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b:.0f} B"


def parse_filename(filepath: str) -> dict | None:
    stem = Path(filepath).stem
    m = re.match(
        r"^(dolt|file_copy|neon|xata)_tpcc_(\d+)_(spine|bushy|fan_out)_branch_setup$",
        stem,
    )
    if not m:
        return None
    return {"backend": m.group(1), "N": int(m.group(2)), "topology": m.group(3)}


def load_all(data_dir: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(data_dir, "*_branch_setup.parquet")))
    dfs: list[pd.DataFrame] = []
    for f in files:
        meta = parse_filename(f)
        if meta is None:
            continue
        df = pd.read_parquet(f)
        df["backend"] = meta["backend"]
        df["N"] = meta["N"]
        df["topology"] = meta["topology"]
        df["storage_delta"] = df["disk_size_after"] - df["disk_size_before"]
        dfs.append(df)
    if not dfs:
        raise RuntimeError(f"No setup parquet files found in {data_dir}")
    return pd.concat(dfs, ignore_index=True)


def print_data_totals(df: pd.DataFrame) -> None:
    files = (
        df[["backend", "topology", "N"]]
        .drop_duplicates()
        .shape[0]
    )
    print("### Data totals")
    print(f"- setup file count (loaded): {files}")
    print(f"- row count: {len(df)}")
    print()


def print_xata_coverage(df: pd.DataFrame) -> None:
    xsub = df[df.backend == "xata"]
    print("### Xata coverage")
    if xsub.empty:
        print("- no Xata rows")
        print()
        return
    for topo in ["spine", "bushy", "fan_out"]:
        nvals = sorted(xsub[xsub.topology == topo]["N"].unique())
        if len(nvals) == 0:
            print(f"- {topo}: no data")
        else:
            print(f"- {topo}: N={','.join(str(n) for n in nvals)}")
    print()


def print_xata_table(df: pd.DataFrame) -> None:
    xsub = df[(df.backend == "xata") & (df.disk_size_before > 0) & (df.disk_size_after > 0)]
    print("### Xata marginal storage delta table")
    print("| N | Spine | Bushy | Fan-out |")
    print("|---|-------|-------|---------|")
    for n in [1, 2, 4, 8, 16]:
        vals = []
        for topo in ["spine", "bushy", "fan_out"]:
            tsub = xsub[(xsub.topology == topo) & (xsub.N == n)]
            vals.append("—" if tsub.empty else fmt_bytes(tsub.storage_delta.mean()))
        print(f"| {n} | {vals[0]} | {vals[1]} | {vals[2]} |")
    print()


def print_xata_latency(df: pd.DataFrame) -> None:
    xsub = df[df.backend == "xata"]
    print("### Xata branch creation latency (mean)")
    print("| N | Spine (ms) | Bushy (ms) | Fan-out (ms) |")
    print("|---|------------|------------|--------------|")
    for n in [1, 2, 4, 8, 16]:
        vals = []
        for topo in ["spine", "bushy", "fan_out"]:
            tsub = xsub[(xsub.topology == topo) & (xsub.N == n)]
            vals.append("—" if tsub.empty else f"{(tsub.latency.mean() * 1000):.1f}")
        print(f"| {n} | {vals[0]} | {vals[1]} | {vals[2]} |")
    print()


def print_xata_ratio(df: pd.DataFrame) -> None:
    xsub = df[(df.backend == "xata") & (df.disk_size_before > 0) & (df.disk_size_after > 0)]
    print("### Xata topology ratio at common max N")
    common_ns = set(xsub[xsub.topology == "spine"]["N"].unique())
    common_ns &= set(xsub[xsub.topology == "bushy"]["N"].unique())
    common_ns &= set(xsub[xsub.topology == "fan_out"]["N"].unique())
    if not common_ns:
        print("- no common N across all three topologies")
        print()
        return
    n = max(common_ns)
    s = xsub[(xsub.topology == "spine") & (xsub.N == n)]["storage_delta"].mean()
    b = xsub[(xsub.topology == "bushy") & (xsub.N == n)]["storage_delta"].mean()
    f = xsub[(xsub.topology == "fan_out") & (xsub.N == n)]["storage_delta"].mean()
    s_f = s / f if f != 0 else float("inf")
    b_f = b / f if f != 0 else float("inf")
    print(f"- common max N: {n}")
    print(f"- spine/fan_out: {s_f:.3f}")
    print(f"- bushy/fan_out: {b_f:.3f}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 1 markdown summary")
    parser.add_argument(
        "--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data")
    )
    args = parser.parse_args()
    data_dir = os.path.abspath(args.data_dir)

    df = load_all(data_dir)
    print_data_totals(df)
    print_xata_coverage(df)
    print_xata_table(df)
    print_xata_latency(df)
    print_xata_ratio(df)


if __name__ == "__main__":
    main()
