#!/usr/bin/env python3
"""Generate all Exp3 report tables from parquet files only."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

RUN_RE = re.compile(
    r"^exp3_(dolt|file_copy|neon)_(spine|bushy|fan_out)_(\d+)t_(branch|crud)_[A-Za-z0-9]+\.parquet$"
)

SHAPES = ["spine", "bushy", "fan_out"]
MODES = ["branch", "crud"]
BACKEND_ORDER = ["dolt", "file_copy", "neon"]

CATEGORY_NAME = {
    0: "FAILURE_NONE",
    1: "FAILURE_LOCK_CONTENTION",
    2: "FAILURE_TIMEOUT",
    3: "FAILURE_RESOURCE_LIMIT",
    4: "FAILURE_CONNECTION",
    5: "FAILURE_BACKEND_STATE_CONFLICT",
    6: "FAILURE_CONSTRAINT_OR_DATA",
    7: "FAILURE_INTERNAL_BUG",
    8: "FAILURE_UNKNOWN",
}
BRANCH_CREATE_OP_TYPE = 1


@dataclass
class RunStats:
    run_id: str
    backend: str
    shape: str
    mode: str
    threads: int
    attempted_ops: int
    successful_ops: int
    failed_ops: int
    failed_slow_ops: int
    failed_exception_ops: int
    success_rate: float
    throughput_ops_sec: float
    branch_create_throughput_ops_sec: float
    per_thread_mean: float
    per_thread_cv: float
    zero_threads: int
    failure_categories: dict[int, int]


def fmt(x: float, nd: int = 2) -> str:
    return f"{x:.{nd}f}"


def fmt_pct(x: float, nd: int = 2) -> str:
    return f"{100.0 * x:.{nd}f}%"


def fmt_opt(x: float | None, nd: int = 2) -> str:
    if x is None:
        return "NA"
    if not np.isfinite(x):
        return "NA"
    return fmt(float(x), nd)


def md_table(headers: list[str], rows: Iterable[list[str]]) -> str:
    rows = list(rows)
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def parse_run(path: Path):
    if path.name.endswith("_setup.parquet"):
        return None
    m = RUN_RE.match(path.name)
    if not m:
        return None
    return {
        "backend": m.group(1),
        "shape": m.group(2),
        "threads": int(m.group(3)),
        "mode": m.group(4),
        "run_id": path.stem,
    }


def load_stats(data_dir: Path, duration: float) -> list[RunStats]:
    stats: list[RunStats] = []
    for p in sorted(data_dir.glob("exp3_*.parquet")):
        meta = parse_run(p)
        if not meta:
            continue

        cols = ["thread_id", "op_type", "outcome_success", "failure_category", "failure_reason"]
        try:
            df = pd.read_parquet(p, columns=cols)
        except Exception:
            # Backward-compatibility for older parquet rows without op_type.
            fallback_cols = ["thread_id", "outcome_success", "failure_category", "failure_reason"]
            df = pd.read_parquet(p, columns=fallback_cols)
            df["op_type"] = np.nan

        if "outcome_success" not in df.columns:
            df["outcome_success"] = True
        df["outcome_success"] = df["outcome_success"].fillna(False).astype(bool)

        if "failure_category" not in df.columns:
            df["failure_category"] = 0
        df["failure_category"] = df["failure_category"].fillna(0).astype(int)

        if "failure_reason" not in df.columns:
            df["failure_reason"] = ""
        df["failure_reason"] = df["failure_reason"].fillna("").astype(str)

        attempted = int(len(df))
        successful = int(df["outcome_success"].sum())
        failed = attempted - successful
        branch_create_successful = int(
            ((df["op_type"] == BRANCH_CREATE_OP_TYPE) & df["outcome_success"]).sum()
        )

        failed_mask = ~df["outcome_success"]
        failed_df = df[failed_mask]
        slow_mask = failed_df["failure_reason"].str.startswith("Slow operation:", na=False)
        failed_slow = int(slow_mask.sum())
        failed_exc = int(failed - failed_slow)

        # Per-thread goodput (successes only), including zero-success threads.
        succ_counts = (
            df[df["outcome_success"]]
            .groupby("thread_id")
            .size()
            .reindex(range(meta["threads"]), fill_value=0)
        )
        per_thread = succ_counts.values.astype(float) / float(duration)
        mean_pt = float(per_thread.mean())
        cv_pt = float(per_thread.std(ddof=0) / mean_pt) if mean_pt > 0 else 0.0
        zero_threads = int((per_thread == 0).sum())

        cat_counts = failed_df["failure_category"].value_counts().to_dict() if failed > 0 else {}

        stats.append(
            RunStats(
                run_id=meta["run_id"],
                backend=meta["backend"],
                shape=meta["shape"],
                mode=meta["mode"],
                threads=meta["threads"],
                attempted_ops=attempted,
                successful_ops=successful,
                failed_ops=failed,
                failed_slow_ops=failed_slow,
                failed_exception_ops=failed_exc,
                success_rate=(successful / attempted) if attempted else 0.0,
                throughput_ops_sec=(successful / float(duration)),
                branch_create_throughput_ops_sec=(branch_create_successful / float(duration)),
                per_thread_mean=mean_pt,
                per_thread_cv=cv_pt,
                zero_threads=zero_threads,
                failure_categories={int(k): int(v) for k, v in cat_counts.items()},
            )
        )

    return stats


def index_stats(stats: list[RunStats]) -> dict[tuple[str, str, str, int], RunStats]:
    return {(s.backend, s.shape, s.mode, s.threads): s for s in stats}


def load_manifest_points(manifest_path: Path) -> list[tuple[str, str, str, int]]:
    if not manifest_path.exists():
        return []
    df = pd.read_csv(manifest_path)
    required = {"backend", "shape", "mode", "threads"}
    if not required.issubset(set(df.columns)):
        return []
    points: list[tuple[str, str, str, int]] = []
    for _, r in df.iterrows():
        try:
            points.append((str(r["backend"]), str(r["shape"]), str(r["mode"]), int(r["threads"])))
        except Exception:
            continue
    return points


def ordered_backends(backends: set[str] | list[str]) -> list[str]:
    bset = set(backends)
    out = [b for b in BACKEND_ORDER if b in bset]
    out.extend(sorted(b for b in bset if b not in BACKEND_ORDER))
    return out


def infer_threads_by_backend(
    stats: list[RunStats], manifest_points: list[tuple[str, str, str, int]]
) -> dict[str, list[int]]:
    out: dict[str, set[int]] = {}
    for b, _sh, _m, t in manifest_points:
        out.setdefault(b, set()).add(int(t))
    if not out:
        for s in stats:
            out.setdefault(s.backend, set()).add(int(s.threads))
    return {b: sorted(ts) for b, ts in out.items()}


def pick_best(
    ix: dict[tuple[str, str, str, int], RunStats], backend: str, shape: str, mode: str, threads: list[int]
) -> RunStats | None:
    vals = [ix.get((backend, shape, mode, t)) for t in threads]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    if mode == "branch":
        return max(vals, key=lambda x: x.branch_create_throughput_ops_sec)
    return max(vals, key=lambda x: x.throughput_ops_sec)


def gen_table_1_coverage(
    stats: list[RunStats],
    manifest_points: list[tuple[str, str, str, int]],
    backends: list[str],
    threads_by_backend: dict[str, list[int]],
) -> str:
    present = {(s.backend, s.shape, s.mode, s.threads) for s in stats}
    rows = []
    total_expected = 0
    total_found = 0

    expected_by_backend: dict[str, set[tuple[str, str, str, int]]] = {}
    if manifest_points:
        for point in manifest_points:
            b = point[0]
            expected_by_backend.setdefault(b, set()).add(point)
    else:
        for b in backends:
            expected_by_backend[b] = {
                (b, sh, m, t)
                for sh in SHAPES
                for m in MODES
                for t in threads_by_backend.get(b, [])
            }

    for b in backends:
        expected = expected_by_backend.get(b, set())
        found = sum(1 for k in expected if k in present)
        missing = max(len(expected) - found, 0)
        total_expected += len(expected)
        total_found += found
        rows.append([b, str(len(expected)), str(found), str(missing)])

    rows.append(["TOTAL", str(total_expected), str(total_found), str(total_expected - total_found)])
    return md_table(["Backend", "Expected points", "Found points", "Missing points"], rows)


def gen_table_2_branch_backend_summary(
    ix: dict[tuple[str, str, str, int], RunStats],
    threads_by_backend: dict[str, list[int]],
    backends: list[str],
) -> str:
    rows = []
    for b in backends:
        threads = threads_by_backend.get(b, [])
        if not threads:
            continue

        t1_thread = threads[0]
        tmax_thread = threads[-1]
        t1_vals = []
        tmax_vals = []
        best: RunStats | None = None

        for sh in SHAPES:
            t1 = ix.get((b, sh, "branch", t1_thread))
            tmax = ix.get((b, sh, "branch", tmax_thread))
            if t1 is not None:
                t1_vals.append(t1.branch_create_throughput_ops_sec)
            if tmax is not None:
                tmax_vals.append(tmax.branch_create_throughput_ops_sec)
            for t in threads:
                cur = ix.get((b, sh, "branch", t))
                if cur is None:
                    continue
                if (
                    best is None
                    or cur.branch_create_throughput_ops_sec
                    > best.branch_create_throughput_ops_sec
                ):
                    best = cur

        t1_range = f"{fmt(min(t1_vals))} - {fmt(max(t1_vals))}" if t1_vals else "NA"
        tmax_range = f"{fmt(min(tmax_vals))} - {fmt(max(tmax_vals))}" if tmax_vals else "NA"
        peak = "NA"
        if best is not None:
            peak = (
                f"{fmt(best.branch_create_throughput_ops_sec)}"
                f" ({best.shape}, T={best.threads})"
            )

        rows.append(
            [
                b,
                t1_range,
                peak,
                tmax_range,
                f"T={tmax_thread}",
            ]
        )

    return md_table(
        [
            "Backend",
            "T1 branch-create throughput (ops/s, min-max over topology)",
            "Peak branch-create throughput (ops/s)",
            "Max-thread branch-create throughput (ops/s, min-max over topology)",
            "Max-thread definition",
        ],
        rows,
    )


def gen_table_3_branch_detailed(
    ix: dict[tuple[str, str, str, int], RunStats],
    threads_by_backend: dict[str, list[int]],
    backends: list[str],
) -> str:
    rows = []
    for b in backends:
        threads = threads_by_backend.get(b, [])
        if not threads:
            continue
        t1_thread = threads[0]
        tmax_thread = threads[-1]

        for sh in SHAPES:
            t1 = ix.get((b, sh, "branch", t1_thread))
            tmax = ix.get((b, sh, "branch", tmax_thread))
            best = pick_best(ix, b, sh, "branch", threads)
            ratio = np.nan
            if (
                t1 is not None
                and tmax is not None
                and t1.branch_create_throughput_ops_sec > 0
            ):
                ratio = (
                    tmax.branch_create_throughput_ops_sec
                    / t1.branch_create_throughput_ops_sec
                )

            t1_throughput = fmt_opt(
                t1.branch_create_throughput_ops_sec if t1 is not None else None
            )
            peak = (
                f"{fmt(best.branch_create_throughput_ops_sec)} (T={best.threads})"
                if best is not None
                else "NA"
            )
            tmax_label = (
                f"{fmt(tmax.branch_create_throughput_ops_sec)} (T={tmax.threads})"
                if tmax is not None
                else f"NA (T={tmax_thread})"
            )

            rows.append(
                [
                    b,
                    sh,
                    t1_throughput,
                    peak,
                    tmax_label,
                    (fmt(ratio, 3) if np.isfinite(ratio) else "NA"),
                ]
            )

    return md_table(
        [
            "Backend",
            "Topology",
            "T1 branch-create throughput (ops/s)",
            "Peak branch-create throughput",
            "Max-thread branch-create throughput",
            "Max/T1",
        ],
        rows,
    )


def gen_table_4_crud_detailed(
    ix: dict[tuple[str, str, str, int], RunStats],
    threads_by_backend: dict[str, list[int]],
    backends: list[str],
) -> str:
    rows = []
    for b in backends:
        threads = threads_by_backend.get(b, [])
        if not threads:
            continue
        t1_thread = threads[0]
        tmax_thread = threads[-1]

        for sh in SHAPES:
            t1 = ix.get((b, sh, "crud", t1_thread))
            tmax = ix.get((b, sh, "crud", tmax_thread))
            best = pick_best(ix, b, sh, "crud", threads)
            ratio = np.nan
            if t1 is not None and tmax is not None and t1.throughput_ops_sec > 0:
                ratio = tmax.throughput_ops_sec / t1.throughput_ops_sec

            t1_throughput = fmt_opt(t1.throughput_ops_sec if t1 is not None else None)
            peak = f"{fmt(best.throughput_ops_sec)} (T={best.threads})" if best is not None else "NA"
            tmax_label = (
                f"{fmt(tmax.throughput_ops_sec)} (T={tmax.threads})"
                if tmax is not None
                else f"NA (T={tmax_thread})"
            )

            rows.append(
                [
                    b,
                    sh,
                    t1_throughput,
                    peak,
                    tmax_label,
                    (fmt(ratio, 3) if np.isfinite(ratio) else "NA"),
                ]
            )

    return md_table(
        [
            "Backend",
            "Topology",
            "T1 aggregate CRUD throughput (ops/s)",
            "Peak aggregate throughput",
            "Max-thread aggregate throughput",
            "Max/T1",
        ],
        rows,
    )


def gen_table_5_per_thread_degradation(
    ix: dict[tuple[str, str, str, int], RunStats],
    threads_by_backend: dict[str, list[int]],
    backends: list[str],
) -> str:
    rows = []
    for b in backends:
        threads = threads_by_backend.get(b, [])
        if not threads:
            continue
        t1_thread = threads[0]
        tmax = threads[-1]

        for sh in SHAPES:
            t1 = ix.get((b, sh, "crud", t1_thread))
            tm = ix.get((b, sh, "crud", tmax))
            d = np.nan
            if t1 is not None and tm is not None and t1.per_thread_mean > 0:
                d = 1.0 - (tm.per_thread_mean / t1.per_thread_mean)

            rows.append(
                [
                    b,
                    sh,
                    fmt_opt(t1.per_thread_mean if t1 is not None else None),
                    fmt_opt(tm.per_thread_mean if tm is not None else None),
                    (fmt_pct(d) if np.isfinite(d) else "NA"),
                    str(tm.zero_threads) if tm is not None else "NA",
                ]
            )

    return md_table(
        [
            "Backend",
            "Topology",
            "T1 per-thread goodput (ops/s/thread)",
            "Max-thread per-thread goodput",
            "Per-thread degradation T1->Tmax",
            "Zero-throughput threads at Tmax",
        ],
        rows,
    )


def gen_table_6_fairness_max(
    ix: dict[tuple[str, str, str, int], RunStats],
    threads_by_backend: dict[str, list[int]],
    backends: list[str],
) -> str:
    rows = []
    for b in backends:
        threads = threads_by_backend.get(b, [])
        if not threads:
            continue
        tmax = threads[-1]

        for sh in SHAPES:
            s = ix.get((b, sh, "crud", tmax))
            rows.append(
                [
                    b,
                    sh,
                    str(tmax),
                    fmt_opt(s.per_thread_mean if s is not None else None, 3),
                    fmt_opt(s.per_thread_cv if s is not None else None, 3),
                    str(s.zero_threads) if s is not None else "NA",
                ]
            )

    return md_table(
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


def gen_table_7_failure_backend(stats: list[RunStats]) -> str:
    backends = ordered_backends({s.backend for s in stats})
    rows = []
    for b in backends:
        sub = [s for s in stats if s.backend == b]
        attempted = sum(s.attempted_ops for s in sub)
        successful = sum(s.successful_ops for s in sub)
        failed = sum(s.failed_ops for s in sub)
        failed_slow = sum(s.failed_slow_ops for s in sub)
        failed_exc = sum(s.failed_exception_ops for s in sub)
        sr = (successful / attempted) if attempted > 0 else np.nan

        cat_counts: dict[int, int] = {}
        for s in sub:
            for k, v in s.failure_categories.items():
                if k == 0:
                    continue
                cat_counts[k] = cat_counts.get(k, 0) + v

        if cat_counts:
            top_cat, top_count = max(cat_counts.items(), key=lambda kv: kv[1])
            top_label = f"{CATEGORY_NAME.get(top_cat, str(top_cat))} ({top_count})"
        else:
            top_label = "NONE"

        rows.append(
            [
                b,
                f"{attempted:,}",
                f"{successful:,}",
                f"{failed:,}",
                f"{failed_exc:,}",
                f"{failed_slow:,}",
                fmt_pct(sr),
                top_label,
            ]
        )

    return md_table(
        [
            "Backend",
            "Attempted ops",
            "Successful ops",
            "Failed ops",
            "Failed exception ops",
            "Failed slow ops",
            "Success rate",
            "Top failure category",
        ],
        rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Exp3 report tables from parquet")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/data"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("experiments/experiment-3-throughput/results/run_manifest.csv"),
    )
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    args = parser.parse_args()

    stats = load_stats(args.data_dir, args.duration_seconds)
    if not stats:
        raise SystemExit("No Exp3 parquet files found.")

    ix = index_stats(stats)
    manifest_points = load_manifest_points(args.manifest)
    threads_by_backend = infer_threads_by_backend(stats, manifest_points)
    backends = ordered_backends(set(threads_by_backend.keys()) | {s.backend for s in stats})

    max_thread_note = ", ".join(
        [f"`{b}=T{threads_by_backend[b][-1]}`" for b in backends if threads_by_backend.get(b)]
    )

    print("# Generated Tables (Exp3)")
    print("")
    print("Definitions used by this generator:")
    print("- Branch tables (RQ1): `successful BRANCH_CREATE rows / 30s`.")
    print("- CRUD tables (RQ2/RQ3): `successful CRUD rows / 30s`.")
    print("- `T1 throughput`: throughput at thread count `T=1`.")
    print("- `Max-thread throughput`: throughput at backend-specific maximum T.")
    if max_thread_note:
        print(f"  (From manifest/data: {max_thread_note}).")
    print("")

    tables = [
        (
            "Table 1. Matrix Coverage",
            gen_table_1_coverage(stats, manifest_points, backends, threads_by_backend),
        ),
        (
            "Table 2. Branch Throughput Summary by Backend",
            gen_table_2_branch_backend_summary(ix, threads_by_backend, backends),
        ),
        (
            "Table 3. Branch Throughput Detailed (Backend x Topology)",
            gen_table_3_branch_detailed(ix, threads_by_backend, backends),
        ),
        (
            "Table 4. CRUD Aggregate Throughput Detailed (Backend x Topology)",
            gen_table_4_crud_detailed(ix, threads_by_backend, backends),
        ),
        (
            "Table 5. CRUD Per-thread Degradation (T1 -> Tmax)",
            gen_table_5_per_thread_degradation(ix, threads_by_backend, backends),
        ),
        (
            "Table 6. Fairness at Max Thread Count (CRUD)",
            gen_table_6_fairness_max(ix, threads_by_backend, backends),
        ),
        ("Table 7. Failure Summary by Backend", gen_table_7_failure_backend(stats)),
    ]

    for title, body in tables:
        print(f"## {title}")
        print("")
        print(body)
        print("")


if __name__ == "__main__":
    main()
