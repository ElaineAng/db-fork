"""Report and compare macrobenchmark runs across backends.

Reads the ``<run_id>_e2e_stats.json`` + ``<run_id>.parquet`` pairs that
``macrobench.runner`` writes, groups them by (scenario, backend) and
produces:

  1. ``summary.md`` / ``summary.csv``: one row per run with elapsed time,
     setup time, operation counts, per-verb median latency, support,
     invariants and the scenario's own metrics (time to recovery, merge
     counts, ...).
  2. ``elapsed.png``: end-to-end time per scenario, backends side by side,
     split into setup and workload.
  3. ``time_breakdown.png``: where the workload time went (branch verbs,
     data statements, connects), stacked per scenario and backend.
  4. ``latency_by_op.png``: median latency per operation type, one panel
     per scenario, backends side by side (log scale, p90 whiskers).
  5. ``latency_cdf_<scenario>.png``: latency CDF of every branch verb
     with a backend per line.
  6. ``storage.png``: database size over the run when the runs were made
     with ``--measure-storage``.

Backends are discovered from the stats files, so one directory holding
dolt and neon runs compares them directly. Several directories can be
given; filters narrow the set.

Usage:
    uv run python scripts/plotting/plot_macrobench.py \
        --data-dir run_stats --outdir figures/macro

    uv run python scripts/plotting/plot_macrobench.py \
        --data-dir run_stats/dolt --data-dir run_stats/neon \
        --backends DOLT NEON --scenarios ops_agent data_agent \
        --outdir figures/macro
"""

from __future__ import annotations

import argparse
import glob
import json
import os
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SCENARIO_ORDER = ["rl_env", "context_mgmt", "multi_agent", "dev_agent", "ops_agent", "data_agent"]
SCENARIO_TITLES = {
    "rl_env": "S1 RL env",
    "context_mgmt": "S2 Context mgmt",
    "multi_agent": "S3 Multi-agent",
    "dev_agent": "S4 Dev agent",
    "ops_agent": "S5 Ops agent",
    "data_agent": "S6 Data agent",
}
BRANCH_VERBS = ["BRANCH", "COMMIT", "DIFF", "LOG", "MERGE", "REBASE", "REVERT", "RESET", "DELETE"]
DATA_OPS = ["READ", "INSERT", "UPDATE", "DDL"]
# EXEC rows summarise the statements they contain, so they are excluded from
# time sums to avoid double counting.
TIME_GROUPS = {
    "branch verbs": BRANCH_VERBS,
    "data statements": DATA_OPS,
    "connect": ["CONNECT"],
    "api retry wait": ["API_RETRY_WAIT"],
}
OP_ORDER = BRANCH_VERBS + ["CONNECT"] + DATA_OPS + ["EXEC"]
PALETTE = plt.get_cmap("tab10")


@dataclass
class Run:
    run_id: str
    backend: str
    scenario: str
    stats: dict
    ops: pd.DataFrame
    path: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.backend


# ── Loading ───────────────────────────────────────────────────────────


def load_runs(data_dirs: list[str], backends: list[str] | None,
              scenarios: list[str] | None, run_glob: str) -> list[Run]:
    runs: list[Run] = []
    for d in data_dirs:
        for stats_path in sorted(glob.glob(os.path.join(d, "**", f"{run_glob}_e2e_stats.json"),
                                           recursive=True)):
            with open(stats_path) as f:
                stats = json.load(f)
            scenario = stats.get("scenario")
            backend = str(stats.get("backend", "?"))
            if not scenario:
                continue  # not a macrobench stats file
            if backends and backend not in backends:
                continue
            if scenarios and scenario not in scenarios:
                continue
            parquet = stats_path.replace("_e2e_stats.json", ".parquet")
            ops = pd.read_parquet(parquet) if os.path.exists(parquet) else pd.DataFrame()
            if not ops.empty:
                ops = ops[ops["run_id"] == stats["run_id"]] if "run_id" in ops else ops
            runs.append(Run(stats["run_id"], backend, scenario, stats, ops, stats_path))
    if not runs:
        raise SystemExit(f"no *_e2e_stats.json macrobench files under {data_dirs}")
    runs.sort(key=lambda r: (SCENARIO_ORDER.index(r.scenario) if r.scenario in SCENARIO_ORDER else 99,
                             r.backend, r.run_id))
    return runs


def dedupe_latest(runs: list[Run]) -> list[Run]:
    """Keep the newest run per (scenario, backend) unless --all-runs."""
    latest: dict[tuple[str, str], Run] = {}
    for r in runs:
        key = (r.scenario, r.backend)
        if key not in latest or os.path.getmtime(r.path) > os.path.getmtime(latest[key].path):
            latest[key] = r
    return sorted(latest.values(), key=lambda r: (SCENARIO_ORDER.index(r.scenario)
                                                  if r.scenario in SCENARIO_ORDER else 99, r.backend))


# ── Per-run aggregates ────────────────────────────────────────────────


def timed_ops(run: Run) -> pd.DataFrame:
    """Rows that carry a latency and are not the EXEC summary."""
    if run.ops.empty:
        return run.ops
    df = run.ops
    return df[(df["status"] == "OK") & (df["op_name"] != "EXEC")]


def time_by_group(run: Run) -> dict[str, float]:
    df = timed_ops(run)
    out = {}
    for group, names in TIME_GROUPS.items():
        out[group] = float(df[df["op_name"].isin(names)]["latency"].sum()) if not df.empty else 0.0
    return out


def latency_stats(run: Run) -> pd.DataFrame:
    """median/p90/count per op_name (OK rows only)."""
    df = run.ops
    if df.empty:
        return pd.DataFrame(columns=["op_name", "median", "p90", "count"])
    ok = df[df["status"] == "OK"]
    g = ok.groupby("op_name")["latency"]
    return pd.DataFrame({"median": g.median(), "p90": g.quantile(0.9), "count": g.size()}).reset_index()


def summary_row(run: Run) -> dict:
    s = run.stats
    inv = s.get("invariants", {})
    row = {
        "scenario": run.scenario,
        "backend": run.backend,
        "run_id": run.run_id,
        "status": s.get("status"),
        "timed_out": s.get("timed_out"),
        "elapsed_sec": s.get("elapsed_sec"),
        "setup_sec": (s.get("setup") or {}).get("setup_sec"),
        "scale_factor": (s.get("schema") or {}).get("scaleFactor"),
        "branch_intensity": (s.get("workload") or {}).get("branchIntensity", 1.0),
        "data_intensity": (s.get("workload") or {}).get("dataIntensity", 1.0),
        "workflow_supported": s.get("workflow_supported"),
        "unsupported_ops": ",".join(s.get("unsupported_ops", [])),
        "invariants_passed": inv.get("passed"),
        "invariants_failed": inv.get("failed"),
        "invariants_na": inv.get("not_applicable"),
    }
    counts = s.get("op_status_counts", {})
    for name in OP_ORDER:
        c = counts.get(name)
        if c:
            row[f"n_{name}"] = c.get("ok", 0)
            if c.get("failed"):
                row[f"failed_{name}"] = c["failed"]
    lat = latency_stats(run).set_index("op_name")
    for name in OP_ORDER:
        if name in lat.index:
            row[f"p50_{name}_ms"] = round(lat.loc[name, "median"] * 1000, 2)
    for group, secs in time_by_group(run).items():
        row[f"time_{group.replace(' ', '_')}_sec"] = round(secs, 2)
    spine = s.get("spine_load") or {}
    if spine:
        txns = spine.get("transactions")
        row["spine_txns"] = sum(txns.values()) if isinstance(txns, dict) else txns
        row["spine_failures"] = spine.get("failures")
    for k, v in (s.get("metrics") or {}).items():
        if isinstance(v, (int, float, str, bool)):
            row[f"m_{k}"] = v
    if s.get("storage_before_bytes") is not None:
        row["storage_before_bytes"] = s.get("storage_before_bytes")
        row["storage_after_bytes"] = s.get("storage_after_bytes")
    return row


# ── Figures ───────────────────────────────────────────────────────────


def _grid(runs: list[Run]):
    scenarios = [s for s in SCENARIO_ORDER if any(r.scenario == s for r in runs)]
    scenarios += sorted({r.scenario for r in runs} - set(scenarios))
    backends = sorted({r.backend for r in runs})
    return scenarios, backends


def _find(runs, scenario, backend):
    for r in runs:
        if r.scenario == scenario and r.backend == backend:
            return r
    return None


def plot_elapsed(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    x = np.arange(len(scenarios))
    width = 0.8 / max(1, len(backends))
    fig, ax = plt.subplots(figsize=(1.6 * len(scenarios) + 3, 4.5))
    for i, b in enumerate(backends):
        setup = [(_find(runs, s, b).stats.get("setup") or {}).get("setup_sec", 0) if _find(runs, s, b) else 0
                 for s in scenarios]
        total = [_find(runs, s, b).stats.get("elapsed_sec", 0) if _find(runs, s, b) else 0 for s in scenarios]
        work = [max(0.0, t - st) for t, st in zip(total, setup)]
        pos = x + (i - (len(backends) - 1) / 2) * width
        ax.bar(pos, setup, width, color=PALETTE(i), alpha=0.4, label=f"{b} setup")
        ax.bar(pos, work, width, bottom=setup, color=PALETTE(i), label=f"{b} workload")
        for p, t, s in zip(pos, total, scenarios):
            r = _find(runs, s, b)
            if r and r.stats.get("timed_out"):
                ax.text(p, t, "timed out", ha="center", va="bottom", fontsize=7, color="red")
            elif r and r.stats.get("status") not in (None, "completed"):
                ax.text(p, t, r.stats.get("status"), ha="center", va="bottom", fontsize=7, color="red")
    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_TITLES.get(s, s) for s in scenarios])
    ax.set_ylabel("seconds")
    ax.set_title("End-to-end time per scenario")
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "elapsed.png"), dpi=150)
    plt.close(fig)


def plot_time_breakdown(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    groups = list(TIME_GROUPS)
    x = np.arange(len(scenarios))
    width = 0.8 / max(1, len(backends))
    fig, ax = plt.subplots(figsize=(1.6 * len(scenarios) + 3, 4.5))
    hatches = ["", "//", "..", "xx"]
    for i, b in enumerate(backends):
        pos = x + (i - (len(backends) - 1) / 2) * width
        bottom = np.zeros(len(scenarios))
        for gi, g in enumerate(groups):
            vals = np.array([time_by_group(_find(runs, s, b))[g] if _find(runs, s, b) else 0.0
                             for s in scenarios])
            ax.bar(pos, vals, width, bottom=bottom, color=PALETTE(i), hatch=hatches[gi % len(hatches)],
                   edgecolor="white", label=f"{b}: {g}" if vals.sum() > 0 else None)
            bottom += vals
    ax.set_xticks(x)
    ax.set_xticklabels([SCENARIO_TITLES.get(s, s) for s in scenarios])
    ax.set_ylabel("summed operation latency (s)")
    ax.set_title("Workload time by operation group (sum over all threads)")
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "time_breakdown.png"), dpi=150)
    plt.close(fig)


def plot_latency_by_op(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 3.8 * nrows), squeeze=False)
    for idx, s in enumerate(scenarios):
        ax = axes[idx // ncols][idx % ncols]
        present = [r for r in runs if r.scenario == s]
        ops = [o for o in OP_ORDER if any(o in latency_stats(r)["op_name"].values for r in present)]
        x = np.arange(len(ops))
        width = 0.8 / max(1, len(backends))
        for i, b in enumerate(backends):
            r = _find(runs, s, b)
            if r is None:
                continue
            lat = latency_stats(r).set_index("op_name")
            med = np.array([lat.loc[o, "median"] * 1000 if o in lat.index else np.nan for o in ops])
            p90 = np.array([lat.loc[o, "p90"] * 1000 if o in lat.index else np.nan for o in ops])
            pos = x + (i - (len(backends) - 1) / 2) * width
            err = np.nan_to_num(p90 - med, nan=0.0)
            ax.bar(pos, np.nan_to_num(med), width, yerr=[np.zeros_like(err), err],
                   color=PALETTE(i), label=b, capsize=2, error_kw={"lw": 0.8})
        ax.set_yscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels(ops, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("median ms (whisker to p90)")
        ax.set_title(SCENARIO_TITLES.get(s, s))
        ax.grid(axis="y", alpha=0.3)
        if idx == 0:
            ax.legend(fontsize=8)
    for idx in range(len(scenarios), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("Operation latency per scenario and backend")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "latency_by_op.png"), dpi=150)
    plt.close(fig)


def plot_latency_cdf(runs: list[Run], outdir: str) -> None:
    scenarios, backends = _grid(runs)
    for s in scenarios:
        present = [r for r in runs if r.scenario == s and not r.ops.empty]
        verbs = [v for v in BRANCH_VERBS
                 if any(((r.ops["op_name"] == v) & (r.ops["status"] == "OK")).any() for r in present)]
        if not verbs:
            continue
        ncols = min(3, len(verbs))
        nrows = int(np.ceil(len(verbs) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 3.2 * nrows), squeeze=False)
        for vi, v in enumerate(verbs):
            ax = axes[vi // ncols][vi % ncols]
            for i, b in enumerate(backends):
                r = _find(present, s, b)
                if r is None:
                    continue
                lat = np.sort(r.ops[(r.ops["op_name"] == v) & (r.ops["status"] == "OK")]["latency"].values * 1000)
                if len(lat) == 0:
                    continue
                ax.step(lat, np.arange(1, len(lat) + 1) / len(lat), where="post", color=PALETTE(i),
                        label=f"{b} (n={len(lat)})")
            ax.set_xscale("log")
            ax.set_xlabel("ms")
            ax.set_ylabel("CDF")
            ax.set_title(v, fontsize=10)
            ax.grid(alpha=0.3)
            ax.legend(fontsize=7)
        for vi in range(len(verbs), nrows * ncols):
            axes[vi // ncols][vi % ncols].axis("off")
        fig.suptitle(f"{SCENARIO_TITLES.get(s, s)}: branch verb latency")
        fig.tight_layout()
        fig.savefig(os.path.join(outdir, f"latency_cdf_{s}.png"), dpi=150)
        plt.close(fig)


def plot_storage(runs: list[Run], outdir: str) -> None:
    with_storage = [r for r in runs if not r.ops.empty and (r.ops["disk_size_after"] > 0).any()]
    if not with_storage:
        return
    scenarios, backends = _grid(with_storage)
    ncols = min(3, len(scenarios))
    nrows = int(np.ceil(len(scenarios) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 3.5 * nrows), squeeze=False)
    for idx, s in enumerate(scenarios):
        ax = axes[idx // ncols][idx % ncols]
        for i, b in enumerate(backends):
            r = _find(with_storage, s, b)
            if r is None:
                continue
            df = r.ops[r.ops["disk_size_after"] > 0].sort_values("end_time")
            t0 = df["end_time"].iloc[0]
            ax.plot(df["end_time"] - t0, df["disk_size_after"] / 1e6, color=PALETTE(i), label=b)
        ax.set_xlabel("seconds into run")
        ax.set_ylabel("MB")
        ax.set_title(SCENARIO_TITLES.get(s, s))
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
    for idx in range(len(scenarios), nrows * ncols):
        axes[idx // ncols][idx % ncols].axis("off")
    fig.suptitle("Database size over the run")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "storage.png"), dpi=150)
    plt.close(fig)


# ── Summary table ─────────────────────────────────────────────────────


def write_summary(runs: list[Run], outdir: str) -> pd.DataFrame:
    df = pd.DataFrame([summary_row(r) for r in runs])
    df.to_csv(os.path.join(outdir, "summary.csv"), index=False)
    lines = ["# Macrobench summary", ""]
    head = ["scenario", "backend", "status", "elapsed_sec", "setup_sec", "workflow_supported",
            "unsupported_ops", "invariants_passed", "invariants_failed", "invariants_na"]
    lines.append("| " + " | ".join(head) + " |")
    lines.append("|" + "---|" * len(head))
    for _, row in df.iterrows():
        lines.append("| " + " | ".join("" if pd.isna(row.get(c)) else str(row.get(c)) for c in head) + " |")
    lines += ["", "## Median latency per operation (ms)", ""]
    lat_cols = [c for c in df.columns if c.startswith("p50_")]
    head2 = ["scenario", "backend"] + [c[4:-3] for c in lat_cols]
    lines.append("| " + " | ".join(head2) + " |")
    lines.append("|" + "---|" * len(head2))
    for _, row in df.iterrows():
        vals = [row["scenario"], row["backend"]] + ["" if pd.isna(row.get(c)) else f"{row[c]:.2f}" for c in lat_cols]
        lines.append("| " + " | ".join(str(v) for v in vals) + " |")
    lines += ["", "## Scenario metrics", ""]
    for r in runs:
        metrics = r.stats.get("metrics") or {}
        if metrics:
            items = ", ".join(f"{k}={v}" for k, v in metrics.items() if isinstance(v, (int, float, str, bool)))
            lines.append(f"- **{r.scenario} / {r.backend}**: {items}")
    inv_fail = [(r, x) for r in runs for x in (r.stats.get("invariants", {}).get("results") or [])
                if x.get("passed") is False]
    if inv_fail:
        lines += ["", "## Failed invariants", ""]
        for r, x in inv_fail:
            lines.append(f"- {r.scenario} / {r.backend}: {x.get('name')}: {x.get('detail', '')}")
    with open(os.path.join(outdir, "summary.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", action="append", required=True,
                    help="directory with <run_id>_e2e_stats.json/<run_id>.parquet (repeatable, searched recursively)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--backends", nargs="*", help="only these backends (names as in the stats, e.g. DOLT NEON)")
    ap.add_argument("--scenarios", nargs="*", help="only these scenarios (rl_env, context_mgmt, ...)")
    ap.add_argument("--run-glob", default="*", help="glob on the run_id, e.g. 'macro_*_mini_*'")
    ap.add_argument("--all-runs", action="store_true",
                    help="keep every run instead of the newest per (scenario, backend)")
    args = ap.parse_args()

    runs = load_runs(args.data_dir, [b.upper() for b in args.backends] if args.backends else None,
                     args.scenarios, args.run_glob)
    if not args.all_runs:
        runs = dedupe_latest(runs)
    os.makedirs(args.outdir, exist_ok=True)

    df = write_summary(runs, args.outdir)
    plot_elapsed(runs, args.outdir)
    plot_time_breakdown(runs, args.outdir)
    plot_latency_by_op(runs, args.outdir)
    plot_latency_cdf(runs, args.outdir)
    plot_storage(runs, args.outdir)

    cols = ["scenario", "backend", "status", "elapsed_sec", "workflow_supported",
            "invariants_passed", "invariants_failed"]
    print(df[[c for c in cols if c in df.columns]].to_string(index=False))
    print(f"\nwrote {len(runs)} run(s) to {args.outdir}: summary.md, summary.csv, "
          f"elapsed.png, time_breakdown.png, latency_by_op.png, latency_cdf_<scenario>.png"
          + (", storage.png" if os.path.exists(os.path.join(args.outdir, 'storage.png')) else ""))


if __name__ == "__main__":
    main()
