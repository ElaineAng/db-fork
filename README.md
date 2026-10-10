# Database Benchmarking Framework

A parametrized and extensible benchmarking framework for branchable database
backends (Dolt, Neon, Xata, SeekDB, plain Postgres copies, ...). Every backend
is driven through one git-like API (`dblib/db_api.py`): `branch`, `commit`,
`diff`, `log`, `merge`, `rebase`, `revert`, `reset`, `delete`, plus `exec()`
to run a workload script on one or more branches. Each operation is timed and
storage-measured, and an operation a backend cannot perform is recorded as
unsupported instead of stopping the workload. Includes both macrobenchmark and
microbenchmark workloads.

## Quick Start

```bash
# 1. Setup environment (installs Python 3.13 and all dependencies into .venv)
#    Requires uv (https://docs.astral.sh/uv/) and protoc (brew install protobuf)
uv sync

# 2. Run a macrobenchmark scenario (schema and data are generated)
# Mini config, always start with this
./scripts/run_macrobench.sh --mini --outdir run_stats rl_env dolt

# Full config at W=5 with a 2h timeout
./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 data_agent dolt 5

# 3. Run a microbenchmark (latency)
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# 4. Run a microbenchmark (throughput)
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional

# 5. Run the unit tests (fake backend, no server needed)
uv run python -m pytest tests/
```

All commands are run from the repository root.

### Repository Layout

```
dblib/              # The git-like DB API, backend implementations, result collection
microbench/         # Microbenchmark runner and operations
macrobench/         # Macrobenchmark scenarios, schema generators and runner
util/               # Shared helpers (SQL loading, DB utilities)
db_setup/           # SQL dumps/schemas and database setup scripts
scripts/            # Benchmark entry points (run_*.sh)
scripts/plotting/   # Plotting and analysis scripts
tests/              # API tests against an in-memory fake backend
```

---

## Table of Contents

- [Database API](#database-api)
- [Macrobenchmarks](#macrobenchmarks)
- [Microbenchmarks](#microbenchmarks)
  - [Latency Benchmarks](#latency-benchmarks)
  - [Throughput Benchmarks](#throughput-benchmarks)
- [Plotting Results](#plotting-results)
- [Output Files](#output-files)

---

## Database API

`dblib/db_api.py` defines `DBToolSuite`, the interface every backend
implements. A backend overrides the protected hooks it supports
(`_branch_impl`, `_commit_impl`, ..., `_storage_bytes`, `_connect_impl`); the
public verbs wrap them with timing, storage measurement and result recording.

### Verbs

| Verb | Meaning | Returns |
|------|---------|---------|
| `branch(name, from_ref)` | create a branch | |
| `commit(ref, message)` | snapshot the branch's working state | commit id |
| `diff(ref_a, ref_b)` | differences between two refs | backend-specific summary |
| `log(ref, limit)` | recent commits | list of dicts |
| `merge(into, source, message, on_conflict)` | merge `source` into `into` | `fast_forward`, `conflicts`, `conflict_tables` (Dolt) |
| `rebase(ref, onto, on_conflict)` | replay `ref`'s commits on `onto` | `conflicts`, `conflict_tables` (Dolt) |
| `revert(ref, commit)` | add a commit undoing `commit` | |
| `reset(ref, to)` | move `ref` to a commit (or restore point) | |
| `delete(ref)` | delete a branch | |

A ref is a branch name or `branch@commit` on backends with commits. On a
backend without commits, a commit ref falls back to the branch head with a
warning and the row is flagged `commit_ref_fallback`.

`merge()` and `rebase()` take `on_conflict`: `"ours"` (default), `"theirs"`,
or a callable `resolve(db, conflicts)` that the backend calls on the
half-merged working set with a session and a list of
`{"table": name, "rows": [...]}` entries (Dolt's base/our/their columns).
The callable resolves conflicts with SQL through `db.sql()`; anything it
leaves is resolved as "ours". Schema conflicts (a dropped table modified on
the other side, two indexes on the same columns) cannot be resolved in place:
the backend aborts and the verb is recorded as FAILED with the reason.

Every verb returns an `OpResult` with `status` OK, UNSUPPORTED (the backend
has no such operation: zero latency, workload continues) or FAILED (the
backend tried and errored). Nothing raises unless `raise_on_error=True`;
`result.raise_for_status()` raises on demand. `DBToolSuite.capabilities()`
reports which verbs a backend supports, and `DBToolSuite.implementation()`
how each one is realised: `native` (one backend primitive), `composed`
(several native primitives driven by the backend class) or `simulated` (SQL
emulation of something the backend lacks). The macrobench e2e stats carry
both under `capabilities` and `implementation`.

| Backend | branch | commit/diff/log | merge | rebase/revert | reset | delete | commit refs | multi-branch exec | exec_async |
|---------|--------|-----------------|-------|---------------|-------|--------|-------------|-------------------|------------|
| `dolt`, `dolt_mysql` | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| `neon` | yes | yes (LSN + log table) | yes (SQL three-way over postgres_fdw) | yes | yes (restore to LSN) | yes | yes | per-ref fallback | yes |
| `xata` | yes | | | | | yes | | | yes |
| `seekdb` | yes | yes (SCN snapshots) | yes (SQL three-way) | yes | yes | yes | yes | yes | yes |
| `matrixone` | yes | yes (snapshots) | yes (DATA BRANCH MERGE) | yes | yes (diff-driven) | yes | yes | yes | yes |
| `file_copy` | yes | | | | | yes | | | yes |

### exec()

```python
results = db.exec(script, refs=["feature"], mode="per_ref", label="eval")
```

runs `script` on each ref in turn (`mode="per_ref"`), or once with every ref
addressable from one session (`mode="multi"`, Dolt, SeekDB and MatrixOne only; other
backends record it as UNSUPPORTED). A script is a list of SQL statements
(strings or `(sql, params)`), Python source, or a callable. Python source
runs with `db` (the session), `params` and `suite` in scope and may define
`run(db)`, whose return value becomes the result's `value`:

```python
script = """
def run(db):
    db.sql("UPDATE stock SET s_quantity = s_quantity - %s WHERE s_i_id = %s", (params["qty"], params["item"]))
    return db.sql("SELECT count(*) FROM stock WHERE s_quantity = 0")[0][0]
"""
```

Every statement issued through `db.sql()` autocommits and is recorded as its
own row (READ/INSERT/UPDATE/DELETE_ROWS/DDL) whose `num_keys_touched` is the rows affected (writes) or returned (reads), unless the script set it with `db.record_keys_touched(n)`. Per ref, `exec()` also records a CONNECT
row when it had to switch the connection to that branch, and one EXEC row
with the script's total latency and storage delta. `exec_async()` runs the
same thing on a connection pool (`open_async_pool(size)`) for throughput
measurement; its scripts must be a SQL list, a coroutine function, or source
defining `async def run(db)`.

### Result rows

Each parquet row carries `op_name`, `status`, `ref`, `label`, `exec_id`
(grouping the rows of one `exec()`), `latency`, `disk_size_before/after`,
`sql_query` (the statement, or the script text on EXEC rows),
`error_message`, `commit_ref_fallback` and the driver context (table, step,
thread). Run summaries include `workflow_supported`, the list of
`unsupported_ops`, per-op status counts and the backend's capabilities.

---

## Macrobenchmarks

The macrobenchmark runs one of six agent-workflow scenarios (BranchBench
S1-S6) against a backend through the git-like API, and records every branch
op and every statement, which verbs the backend could not perform, and
whether the sentinel-based invariance checks held.

| Scenario | Key | Shape | Branch ops |
|----------|-----|-------|------------|
| S1 Agentic RL environment | `rl_env` | T task branches, G rollout leaves each, forks from recorded steps | branch (from commit), commit, diff, delete |
| S2 Agent context management | `context_mgmt` | long spine, candidate branches per compaction | branch, commit, merge (ff), revert, rebase, delete |
| S3 Multi-agent collaboration | `multi_agent` | N agents merging into a spine that keeps changing | branch, commit, log, merge (with conflicts), delete |
| S4 Development agent | `dev_agent` | dev branches off a busy production spine, never merged | branch, commit, rebase (with conflicts), delete |
| S5 Operations agent | `ops_agent` | dense commits, bad deploy, investigation branches, PITR | branch (from commit), commit, diff, reset, delete |
| S6 Data agent | `data_agent` | ingestion batches rebased and merged into a warehouse | branch, commit, reset, rebase, merge, delete |

Each scenario lives in `macrobench/scenarios/s<N>_<key>.py` with its
parametrized SQL and exec() scripts; `macrobench/faults.py` holds the fault
catalog and the TPC-C consistency conditions S1 and S5 use;
`macrobench/tpcc.py` holds the TPC-C transactions and CH queries that the
optional spine load runs.

### Configuration

A run is one `MacroBenchConfig` textproto (`macrobench/task.proto`):

```
run_id: "macro_data_agent_dolt"
backend: DOLT
database_setup { db_name: "macro_data_agent" cleanup: true generated {} }
schema { base: CH_BENCH scale_factor: 1 }                 # generated CH-benCHmark at W=1
workload {
  branch_ops { commit_interval: 2 }                        # commit every 2 steps
  data_ops { statements_per_step: 8 rows_per_write: 5     # exec() intensity
             write_fraction: 0.8 spine_clients: 4 analytical_fraction: 0.8 }
  data_agent { batches: 32 concurrent_batches: 4 batch_rows: 2000
               days_back: 3 steps_per_batch: 10 reset_prob: 0.1 }
}
```

- `schema` picks the base (CH-benCHmark or none), the scale factor W and
  row-density knobs, and the extension tables (`macrobench/schema/*.sql`;
  by default the scenario's own). With `database_setup.generated`, the
  runner creates an empty database and seeds it with the generators in
  `macrobench/datagen/`; `sql_dump` and `existing_db` still work.
- `workload.branch_ops` and `workload.data_ops` set the shared intensity
  (commit interval, retention, live-branch cap; statements per step, rows per
  write, read/write/DDL mix, worker threads, background spine clients).
- The scenario message holds the paper's structural parameters (T/G/S, N,
  fan-out, cycles, D, p, ...).
- `workload.branch_intensity` and `workload.data_intensity` are multipliers
  applied on top of the explicit knobs before the run (0 or 1 = unchanged).
  Branch intensity scales the number of branch verbs (branches, forks,
  rounds, rebases, investigation points) and divides `commit_interval`;
  data intensity scales the SQL per branch (statements per step, rows per
  write, spine clients, per-branch steps and rows). Thread counts, caps and
  probabilities are not scaled. `macrobench/intensity.py` lists the exact
  fields; the effective values are printed and written to the e2e stats.
- `invariants { disabled: true }` turns the sentinel checks off;
  `fail_fast: true` stops at the first failure.

`macrobench/configs/<key>.textproto` holds the paper-scale parameters and
`<key>_mini.textproto` a smoke-test size.

### Running

```bash
./scripts/run_macrobench.sh [--mini] [--outdir DIR] [--max-runtime-sec N] [--measure-storage] \
    [--storage-sample-interval SEC] [--branch-intensity X] [--data-intensity Y] \
    <scenario> <backend> [scale_factor]
```

| Argument | Description |
|----------|-------------|
| `scenario` | `rl_env`, `context_mgmt`, `multi_agent`, `dev_agent`, `ops_agent`, `data_agent` |
| `backend` | `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `xata`, `file_copy` |
| `scale_factor` | W warehouses for the generated data (default: the config's) |
| `--storage-sample-interval SEC` | seconds between background storage samples (default 5, 0 = off) |
| `--measure-storage` | also measure storage around every operation |

```bash
./scripts/run_macrobench.sh --mini rl_env dolt
./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 multi_agent dolt
# twice the branch churn, half the SQL per branch
./scripts/run_macrobench.sh --mini --branch-intensity 2 --data-intensity 0.5 rl_env dolt
./scripts/run_macrobench.sh --measure-storage data_agent dolt 5
# or directly
uv run python -m macrobench.runner --config macrobench/configs/ops_agent_mini.textproto --outdir run_stats
```

A backend that lacks a verb still runs the whole scenario: the verb is
recorded as UNSUPPORTED, `workflow_supported` is false in the e2e stats, and
invariance checks that depend on it are reported as not applicable.

#### Storage

Every run records the backend's storage size (`DBToolSuite._storage_bytes()`:
the database's own directory on Dolt, the whole server data directory on
SeekDB and MatrixOne, which declare `STORAGE_SCOPE = "server"`) at four
workflow-level points, written to the e2e stats under `storage.points`:

| Point | When |
|---|---|
| `after_setup` | schema and seed data loaded, before the scenario |
| `after_workflow` | the scenario is done (its branches deleted) |
| `after_gc` | after the backend's GC/flush hook (`gc()`: `dolt_gc`, MatrixOne checkpoint; UNSUPPORTED on SeekDB) |
| `after_cleanup` | the run's database dropped (and, on Dolt, the dropped databases purged) |

A background sampler also records the size every 5 s during the scenario
(`storage.samples`, wall-clock seconds and bytes); `--storage-sample-interval
SEC` changes the interval (`0` turns it off), as does
`storage_sample_interval_sec` in the config. `--measure-storage` additionally
measures before and after every verb and every exec() call (two directory
walks per operation; off by default, and not attributable under concurrent
workers). The report's `storage.png` draws the sampled series with the four
points as markers; `summary.md` carries them as `storage_after_*_mb`.

### Output

```
run_stats/
├── <run_id>.parquet            # one row per branch op, statement, exec() and connect
└── <run_id>_e2e_stats.json     # status, support summary, invariants, scenario metrics
```

The e2e stats carry the workload parameters, the seed statistics,
`capabilities` and `implementation` (how the backend realises each verb:
native, composed or simulated), `workflow_supported` with `unsupported_ops`
and `op_status_counts`, the `invariants` results, the scenario's own
`metrics` (e.g. S3 merge order and conflicts, S5 time to recovery, S6
fast-forward vs three-way merges), the spine load's transaction counts and
the `storage` block (the four workflow-level points, the GC status, the
sampled series; see [Storage](#storage)).

---

## Microbenchmarks

Microbenchmarks measure latency and throughput for specific database operations under controlled conditions.

### Latency Benchmarks

Measure single-threaded operation latency with varying numbers of branches. For
multi-threaded load, use the throughput benchmarks.

#### Single-Threaded Latency

Use `scripts/run_single_thread_bench.sh` to measure single-threaded operation latency:

```bash
./scripts/run_single_thread_bench.sh <backend> <sql_dump_path> <num_branches> [OPTIONS]
```

##### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `xata`, `file_copy` |
| `sql_dump_path` | Path to SQL dump file (e.g., `db_setup/tpcc_schema.sql`) |
| `num_branches` | Number of branches to create for testing |

##### Options

| Option | Description |
|--------|-------------|
| `--seed <seed>` | Random seed for reproducibility |
| `--shape <shape>` | Branch tree shape: `spine`, `bushy`, or `fan_out` (default: `spine`) |
| `--operations <ops>` | Comma-separated list (e.g., `UPDATE,RANGE_UPDATE`) |
| `--range-size <n>` | Range size for RANGE_UPDATE operation (default: 200) |
| `--num-ops <n>` | Number of operations to perform |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

##### Examples

```bash
# Run with 16 branches
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# Run with custom seed and bushy branch shape
./scripts/run_single_thread_bench.sh neon db_setup/tpcc_schema.sql 32 --seed 12345 --shape bushy

# Run with custom range size
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16 --operations RANGE_UPDATE --range-size 500
```

#### Output Files

Latency benchmark results are saved to the output directory:

```
<output_dir>/
├── single_thread/
│   ├── branch/
│   │   ├── <backend>_<dataset>_<N>_<shape>_branch.parquet
│   │   ├── <backend>_<dataset>_<N>_<shape>_branch_setup.parquet
│   │   └── <backend>_<dataset>_<N>_<shape>_branch_summary.json
│   └── connect/
│       └── (similar structure)
```

---

### Throughput Benchmarks

Measure throughput (operations per second) with independent control over threads, branches and concurrent requests.

#### Running Throughput Benchmarks

Use `scripts/run_throughput_bench.sh` with one of three sweep modes:

```bash
# Sweep concurrency (fix threads and branches, vary concurrent requests per thread)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-concurrency --threads <N> --branches <N> [OPTIONS]

# Sweep branches (fix threads, vary branches)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-branches --threads <N> [OPTIONS]

# Sweep proportionally (vary both threads and branches together)
./scripts/run_throughput_bench.sh <backend> <sql_dump_path> --sweep-proportional [OPTIONS]
```

The script runs in **async mode** by default: each thread keeps
`--concurrent-requests` ops in flight, each on its own pooled connection
checked out on the thread's branch. Async mode supports `dolt`, `dolt_mysql`,
`seekdb` and `neon`; use `--mode sync` for the other backends (one op at a time per
thread). Every point of a sweep uses the same mode, including concurrency 1.

Each thread works on one branch, assigned round-robin. With fewer threads
than branches, the extra branches exist but get no load; with more threads
than branches, threads share branches.

#### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `seekdb`, `matrixone`, `neon`, `xata`, `file_copy` |
| `sql_dump_path` | Path to SQL dump file |
| `--sweep-concurrency` | Fix threads and branches, vary concurrent requests (requires `--threads` and `--branches`; async only) |
| `--sweep-branches` | Fix threads, vary branches (requires `--threads`) |
| `--sweep-proportional` | Vary both threads and branches proportionally |

#### Options

| Option | Description |
|--------|-------------|
| `--mode <async\|sync>` | Runner to use (default: `async`; `sync` for backends without async support) |
| `--threads <N>` | Fixed thread count (for `--sweep-concurrency` and `--sweep-branches`) |
| `--branches <N>` | Fixed branch count (for `--sweep-concurrency`) |
| `--threads-per-branch <N>` | Threads per branch ratio for `--sweep-proportional` (default: 4) |
| `--branch-list <list>` | Comma-separated branch counts (e.g., `1,2,4,8,16`) |
| `--concurrency-list <list>` | Comma-separated concurrency levels for `--sweep-concurrency` (default: `1,2,4,...,1024`) |
| `--concurrent-requests <n>` | Ops in flight per thread for the other sweeps (default: 1; > 1 needs async) |
| `--num-ops <n>` | Number of operations per thread (overrides the per-operation defaults) |
| `--point-ops <n>` / `--range-ops <n>` | Operations per thread for point / range operations |
| `--warmup-ops <n>` / `--warmup-fraction <f>` | Warm-up operations per thread, not counted in throughput |
| `--operations <ops>` | Comma-separated list (e.g., `READ,RANGE_READ`) |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

Total timed operations per run are `num_ops x threads`; concurrent requests
overlap them but do not add to the count. Keep `threads x (concurrent requests + 1)`
below the server's connection limit.

#### Examples

```bash
# Fix 8 threads on 1 branch, vary concurrency: 1,2,4,8,16,32
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-concurrency --threads 8 --branches 1 --concurrency-list "1,2,4,8,16,32"

# Fix 4 threads x 16 concurrent requests, vary branches (4 active, the rest idle)
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-branches --threads 4 --concurrent-requests 16 --branch-list "4,16,64,256,1024"

# One thread per branch with 4 requests in flight each, 1-128 branches
./scripts/run_throughput_bench.sh neon db_setup/ch-w1.sql --sweep-proportional --threads-per-branch 1 --concurrent-requests 4

# Backend without async support
./scripts/run_throughput_bench.sh xata db_setup/ch-w1.sql --sweep-branches --threads 16 --mode sync

# Run only specific operations
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional --operations READ,RANGE_READ
```

#### Output Files

Throughput benchmark results are saved to the output directory:

```
<output_dir>/
├── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>].parquet
├── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>]_<operation>_threads<threads>_summary.json
└── <backend>_<dataset>_tp_t<threads>_b<branches>[_cr<concurrency>]_setup.parquet
```

Async runs include `_cr<concurrency>` in the name (also at concurrency 1); sync
runs do not. The summary JSON records `execution_mode` and `concurrent_requests`.

---

## Plotting Results

After running benchmarks, use the plotting scripts in the `scripts/plotting/` directory to generate visualizations.

### Macrobenchmark Report

`plot_macrobench.py` reads every `<run_id>_e2e_stats.json` / `<run_id>.parquet`
pair under one or more directories, groups the runs by scenario and backend,
and writes a summary plus comparison figures. Backends are read from the
stats files, so a directory holding Dolt and Neon runs compares them directly:

```bash
uv run python scripts/plotting/plot_macrobench.py --data-dir run_stats --outdir figures/macro

# several directories, filtered to two backends and two scenarios
uv run python scripts/plotting/plot_macrobench.py \
    --data-dir run_stats/dolt --data-dir run_stats/neon \
    --backends DOLT NEON --scenarios ops_agent data_agent \
    --outdir figures/macro
```

| Output | Content |
|---|---|
| `data/summary.md`, `data/summary.csv` | per run: status, elapsed/setup time, support, invariants, agent op counts and median latency per verb (spine load excluded), spine throughput and statement latency, scenario metrics |
| `time_breakdown.png` | summed latency split into branch ops (verbs plus the connection switch `exec()` does to reach a ref) and data ops (statements); spine load excluded |
| `latency_by_op_branch.png`, `latency_by_op_data.png`, `data/latency_by_op.md/.csv` | one panel per operation (branch verbs + CONNECT in one figure; statements inside `exec()` + EXEC in the other, with READ/UPDATE/DELETE split into point vs range by rows touched, READ:scan for aggregate/join reads, INSERT:bulk for multi-row inserts): median latency with a 95% CI, grouped by scenario (shaded bands), colour = backend; ops some backends lack are grouped last; the .md adds a backend x op support matrix |
| `latency_exec_by_label.png`, `data/exec_by_label.md/.csv` | `exec()` latency per script label (rollout_step, ingest, spine, ...) with statements per exec, and the mean exec time split into READ/INSERT/UPDATE/DELETE_ROWS/DDL/CONNECT/other |
| `progress.png` | completed agent steps (branch verbs and `exec()` runs, background load excluded) against elapsed seconds, one curve per backend |
| `exec_time_by_op.png` | per scenario, one horizontal 100% bar per script label and backend (adjacent rows) showing where `exec()` time goes: statement types, CONNECT, and `other` (untimed BEGIN/COMMIT/ROLLBACK round trips plus the Python between statements); mean ms and count at the bar end |
| `data/data_ops.md/.csv` | data statements (READ/INSERT/UPDATE/DELETE_ROWS/DDL) split by workload role: spine traffic vs. the agent's own statements (ingest, backfill, rollout_step, ...) |
| `cdf/latency_cdf_<scenario>.png` | latency CDF of each branch verb and data statement type (agent rows only), one line per backend |
| `storage.png` | storage over the run: the 5 s sampled series (or the per-operation sizes of runs made with `--measure-storage`) with the four workflow-level points as markers; log axis when database-scope and server-scope backends share a panel |

Only the newest run per (scenario, backend) is used unless `--all-runs` is
given; `--run-glob` narrows by run id (e.g. `'macro_*_mini_*'`).

### Microbenchmark Latency Plots

Plot microbenchmark latency results:

```bash
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir <data_directory> \
    --outdir <output_figures_dir> \
    --operation <operation_type>
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `--data-dir` | Directory with microbenchmark parquet files |
| `--branch-dir` | Directory with branch operation data (for combined plots) |
| `--connect-dir` | Directory with connect operation data (for combined plots) |
| `--outdir` | Directory to save figures |
| `--operation` | Operation type: `branch`, `connect`, `both`, or `combined` |

#### Examples

```bash
# Plot branch creation latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/branch \
    --outdir figures/ \
    --operation branch

# Plot branch connection latency only
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation connect

# Plot combined (branch and connect on single plot)
uv run python scripts/plotting/plot_branch_latency_micro.py \
    --branch-dir run_stats_final/micro/single_thread/branch \
    --connect-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation combined
```

### Microbenchmark Throughput Plots

Plot throughput vs threads/branches:

```bash
uv run python scripts/plotting/plot_throughput_experiments.py \
    --data-dir <data_directory> \
    --output <output_figures_path> 
```

---

## Output Files

Benchmark results are saved as Parquet files and JSON summaries.

### Macrobenchmark Output Structure

```
run_stats/
├── macro_rl_env_mini_dolt.parquet
├── macro_rl_env_mini_dolt_e2e_stats.json
├── macro_data_agent_dolt_w5.parquet
└── macro_data_agent_dolt_w5_e2e_stats.json
```

### Microbenchmark Output Structure

```
run_stats_final/micro/
├── single_thread/      # Single-threaded latency benchmarks
│   ├── branch/
│   └── connect/
├── multithread/        # Multi-threaded latency benchmarks
├── tp_fix_branch/      # Throughput: fixed branches, varying threads
├── tp_fix_thread/      # Throughput: fixed threads, varying branches
└── tp_proportional/    # Throughput: proportional threads and branches
```

### Parquet Schema

See [Result rows](#result-rows); the authoritative definition is
`dblib/result.proto`.

---

## Prerequisites

1. **[uv](https://docs.astral.sh/uv/)**: manages Python (3.13, pinned in
   `.python-version`) and the dependencies (locked in `uv.lock`)
2. **protoc** (`brew install protobuf`): the install step compiles the
   `.proto` files into the untracked `*_pb2.py` modules
3. **PostgreSQL-compatible backend**:
   - **Dolt**: Follow setup at https://github.com/dolthub/doltgresql
   - **Neon**: Configure via Neon console
4. **psql** client for database setup

Install with `uv sync` (add `--group dev` for pytest, then run
`uv run pytest tests/`). The project is installed in editable mode, and
`uv sync` / `uv run` recompile the protos whenever a `.proto` file changes.
Run Python through `uv run` (e.g. `uv run python scripts/...`), or activate
the environment with `source .venv/bin/activate`. The `scripts/run_*.sh` scripts
already use `uv run`. To add a dependency, use `uv add <package>` and commit
the updated `uv.lock`.

---

## Dolt (MySQL) backend — `dolt_mysql`

Runs against Dolt's MySQL-compatible server (port 3306). Start it with:

`dolt sql-server --host 127.0.0.1 --port 3306 --data-dir ~/dolt/databases`

and set a commit identity (`dolt config --global --add user.name` / `user.email`).

The loader reads Postgres `pg_dump` files (e.g. `ch-w1.sql`) directly and
converts them to MySQL, so no separate MySQL schema is needed.

Supports single-threaded, multi-threaded, and async (`concurrent_requests > 1`)
microbenchmark runs via `runner2.py`. In async mode each thread opens a pool
of `concurrent_requests` connections; `exec_async()` checks out the target
branch on a pooled connection the first time it is used there.

## SeekDB backend — `seekdb`

Runs against SeekDB (OceanBase's MySQL-compatible server, port 2881). Install
and start it with `brew tap oceanbase/seekdb && brew install seekdb &&
seekdb-start`, or run a source build's `bin/seekdb` from its base directory.

Connection settings come from `SEEKDB_HOST` (default `127.0.0.1`),
`SEEKDB_PORT` (`2881`), `SEEKDB_USER` (`root`) and `SEEKDB_PASSWORD` (empty).
Storage is measured on the server's data directory, `SEEKDB_DATA_DIR`
(default: `~/seekdb/store` if it exists, else the brew install's
`/opt/homebrew/var/seekdb/data`); it covers the whole server, since SeekDB
has no per-database directory. It uses the same `pg_dump` loader as
`dolt_mysql`. Every connection raises `ob_query_timeout` to one hour: the
default 10 s is too short for seeding and for the joins behind merge/rebase.

SeekDB branches by forking whole databases (copy-on-write, milliseconds), so
each branch is its own database: `main` is `<db_name>`, and branch `X` is
`<db_name>__X`. `branch()` runs `FORK DATABASE`, connecting runs `USE`, and
`delete()` runs `DROP DATABASE` (forked databases can take several seconds to
drop). A multi-branch script addresses another branch as
`` `<db>__<branch>`.`<table>` ``.

SeekDB has no commits, so the history verbs are built from two primitives it
does have: `current_scn()` with flashback reads (`<table> AS OF SNAPSHOT
<scn>`), and database forks.

- `commit()` records the current SCN in the branch database's `_bb_commits`
  table (its id is the SCN in hex) together with the table and column list at
  that point. Forks copy that table, so a branch inherits its parent's log;
  `log()` reads it.
- `diff()`, `reset()`, `revert()` and `branch(name, "b@commit")` read the
  commit's snapshot with flashback queries and apply the differences with
  anti-joins (`DELETE ... NOT IN`, `REPLACE INTO ... SELECT`). A fork's
  tables have no history from before the fork, so each log row names the
  database whose flashback holds its snapshot; a snapshot older than the
  server's `undo_retention` (default 30 min; raise it with
  `ALTER SYSTEM SET undo_retention = 86400`) can no longer be read. Flashback
  cannot roll back DDL, so a restore drops the tables and columns the commit's
  recorded schema does not list (indexes stay). An index built after row
  updates makes that table's earlier snapshots unreadable for good (error
  1412): a restore then keeps the table's current rows, a merge or rebase
  uses the fallback fork point or, failing that, a two-way merge for it,
  and the verb's result lists the table (`not_restored`,
  `fallback_base_tables`, `two_way_tables`) with a warning.
- `merge()` and `rebase()` are a SQL three-way merge: base = the fork point
  of whichever side descends from the other (the fork's own content right
  after the fork; the parent at the SCN just before the fork is kept as a
  fallback for tables whose history later DDL made unreadable), ours = the
  target database, theirs = the other branch. Rows are matched by primary key; a row changed differently on
  both sides is a conflict, resolved per `on_conflict` (a callable gets rows
  in Dolt's `base_*` / `our_*` / `their_*` shape). Tables and columns added
  on one side are added to the other; a differing primary key is a schema
  conflict and the verb fails without touching data. Tables without a
  primary key (`history`) only receive the other side's new rows. `rebase()`
  reads the upstream at one SCN, which becomes the branch's new fork point,
  and the merge after it is then a fast-forward.
- `SEEKDB_NATIVE_MERGE=1` makes `merge()` use SeekDB's own `MERGE TABLE ...
  STRATEGY OURS|THEIRS` per table instead. It has no common ancestor (every
  differing row is a conflict; the source's deletes are never applied) but
  measures the native primitive; conflicts are counted with `STRATEGY FAIL`.

A commit ref used with `exec()` is materialised as a temporary fork
(`<db>__tmp_*`) restored to that snapshot and dropped when the connection
closes.

Async mode (`use_async` / `concurrent_requests > 1`) uses an aiomysql pool,
like `dolt_mysql`. Each pool connection is opened on the worker's branch
database.

---

## MatrixOne backend — `matrixone`

Runs against MatrixOne (MySQL-compatible, port 6001). Build it from source
(`make build` in a checkout of https://github.com/matrixorigin/matrixone, Go
1.26+ and cmake needed; v4.2.5 was used here) and start it with
`./mo-service -launch ./etc/launch/launch.toml`, or run the
`matrixorigin/matrixone` image. Connection settings come from `MO_HOST`
(default `127.0.0.1`), `MO_PORT` (`6001`), `MO_USER` (`root`) and
`MO_PASSWORD` (`111`, the standalone default). Storage is measured on the
server's data directory, `MO_DATA_DIR` (default `~/mo/matrixone/mo-data`),
which covers the whole server.

MatrixOne ships "git for data" primitives: database and table branches with
recorded lineage (`DATA BRANCH CREATE DATABASE ... FROM ... {snapshot}`), a
three-way `DATA BRANCH DIFF` / `DATA BRANCH MERGE` that finds the lowest
common ancestor itself, `DATA BRANCH PICK ... BETWEEN SNAPSHOT`, named
snapshots with time-travel reads (`t{snapshot = 'x'}`, readable even after
the database is dropped) and `RESTORE DATABASE ... {snapshot}`. As on
SeekDB, each branch is its own database (`main` is `<db_name>`, branch `X`
is `<db_name>__X`), and a multi-branch script addresses another branch as
`` `<db>__<branch>`.`<table>` `` or a commit as `` `<db>`.`<table>`{snapshot = '...'} ``.

A commit is a database snapshot; its message and order live in the branch
database's `_bb_commits` table, which branches inherit. Per verb
(`MatrixOneToolSuite.IMPLEMENTATION` / `IMPLEMENTATION_NOTES` carry the
same summary for the report):

- `branch()` (native): `CREATE SNAPSHOT` on the parent (the fork point)
  and `DATA BRANCH CREATE DATABASE ... FROM ... {snapshot}`; the new branch
  is snapshotted too so its own delta can be read later.
- `commit()` (composed): `CREATE SNAPSHOT FOR DATABASE` plus the log row.
  `log()` (simulated) reads the table.
- `diff()` (native): `DATA BRANCH DIFF ... OUTPUT SUMMARY` per table.
- `merge()` (composed): `DATA BRANCH MERGE <src>.<t> INTO <dst>.<t> WHEN
  CONFLICT SKIP|ACCEPT` per table ("ours" | "theirs"); the conflict count,
  and the rows a resolve callable gets (Dolt's `base_*`/`our_*`/`their_*`
  shape), come from the two sides' native diffs against the fork snapshot.
  A callable runs over the SKIP result. Tables and columns the source has
  and the target lacks are added first (`DATA BRANCH CREATE TABLE ... FROM`,
  `ALTER TABLE ADD COLUMN`); a differing primary key is a schema conflict
  and the verb fails without touching data. Set `MO_COUNT_CONFLICTS=0` to
  skip the conflict count when `on_conflict` is "ours"/"theirs".
- `rebase()` (composed): a re-fork. A temporary clone of the upstream's
  head snapshot receives the branch's delta since its fork point through
  `DATA BRANCH PICK ... BETWEEN SNAPSHOT` (the branch's own snapshots;
  key-less tables are replayed row by row), conflicts with the upstream's
  delta over the same period are resolved per `on_conflict` ("ours" = the
  upstream, "theirs" = the branch, as in git), and the clone replaces the
  branch. Its lineage now starts at the upstream head, so MatrixOne's LCA
  for a later merge is that head and the merge sees only newer changes.
- `reset()` (composed): `DATA BRANCH DIFF` of the head against the commit's
  snapshot, undone with SQL (rows added since are deleted, rows changed or
  deleted put back, tables and columns created since dropped). The native
  `RESTORE DATABASE <branch> {snapshot}` (`MO_NATIVE_RESET=1`) does it in
  one statement but gives the tables new identities, and later lineage
  diffs across that edge report updated rows as inserted on both sides,
  which broke the rebase of reset batches in S6. A commit inherited from
  the parent is restored row by row from the snapshot's time-travel view.
- `revert()` (simulated): `DATA BRANCH DIFF` between the commit's snapshot
  and its predecessor, inverse applied with SQL.
- `delete()` (native): `DATA BRANCH DELETE DATABASE` (`DROP DATABASE` after
  a restore, which leaves the database without branch metadata).

MatrixOne's diff is reliable for two snapshots of one table, for a clone
(or chain of clones) against its ancestor's head, and for a clone against
the exact snapshot it was cloned from; it drops the ancestor's updates and
deletes when a clone is compared with an *older* snapshot of the ancestor,
which is why `rebase()` replays the branch's delta with `PICK` instead of
merging the branch into the clone. `DATA BRANCH DIFF ... OUTPUT AS` fails
on a table altered since the base (v4.2.5), so diff rows are fetched to the
client. A native-merge conflict between an update and a delete keeps the
update (the row is resurrected). Snapshot names are silently truncated to
64 characters; the backend keeps its own below that. `CREATE INDEX IF NOT
EXISTS` is not accepted (S2 falls back to `CREATE INDEX`).

A commit ref used with `exec()` is materialised as a temporary branch
(`<db>__tmp_*`) created from the snapshot and dropped when the connection
closes. `drop_database()` drops every branch database and every snapshot of
the run (`bb_<db>_*`). Async mode uses an aiomysql pool, like `dolt_mysql`.

---

## Neon backend — `neon` (Lakebase Postgres on Neon)

Runs against Neon's API (`console.neon.tech/api/v2`) with `NEON_API_KEY_ORG`
from `.env`. Each run creates a project of its own (`project_<db_name>`,
region `NEON_REGION`, default `aws-us-east-1`, Postgres `NEON_PG_VERSION`
17, a 2-day history window `NEON_HISTORY_RETENTION_SEC`) and deletes it
afterwards; stale benchmark projects left by interrupted runs are deleted
before a run starts. Every compute is a fixed `NEON_COMPUTE_CU` (2 CU) and never
auto-suspends (`NEON_BRANCH_SUSPEND_TIMEOUT_SEC=-1`): Neon's scale-to-zero
counts only running statements as activity, so with the plan default (5
min on Launch) a compute that a worker holds but has not queried for 5
minutes, or one a merge reads over `postgres_fdw` while busy elsewhere, is
suspended under its connections ("SSL connection has been closed
unexpectedly"). Computes are suspended explicitly instead, by the
active-compute budget below and on delete.

Branches are Neon branches with one read-write compute each. Neon versions
storage but has no commits, diff or merge, so the adapter builds them from
point-in-time branching (`parent_lsn`), branch restore and `postgres_fdw`:

- `branch()` (native): `POST /branches` with `parent_id`, `parent_lsn` for a
  commit ref, and a compute; the verb waits for the operations to finish
  and writes a fork row on the new branch (its own LSN and the parent point
  it was cloned from, the base of later merges).
- `commit()` (composed): a row in the branch's `_bb_commits` table plus
  `pg_current_wal_flush_lsn()` as the commit's point in time; `log()` reads
  the table. A commit ref is read through a temporary branch at that LSN,
  cached between uses, deleted when the connection closes.
- `diff()`: per table a row hash on each compute; the differing tables are
  pulled over `postgres_fdw` into temp tables and compared locally.
- `merge()`: SQL three-way merge on the target's compute. The base is a
  temporary branch at the source's fork LSN (its creation is inside the
  verb's latency), theirs is the source's compute, both pulled over
  `postgres_fdw`; rows are matched by primary key, conflicts resolved per
  `on_conflict` with the same callable row shape as the other backends;
  deletes go in reverse foreign-key order and upserts in forward order
  (the role is not a superuser, so constraints stay on).
- `rebase()`: the same three-way merge of the upstream into the branch,
  then a rebase row moves the branch's base to the upstream's LSN so the
  merge back only sees newer changes.
- `revert()`: temporary branches at the commit's LSN and its predecessor,
  inverse delta applied locally.
- `reset()` (native): `POST /branches/{id}/restore` to the commit's LSN.
  Neon requires `preserve_under_name` and keeps the pre-restore state in a
  backup branch (`<branch>_bk_*`) that cannot be deleted before the project.
- `delete()` (native): `DELETE /branches/{id}`; a branch that still has
  children is deferred and retried after later deletes and at close.
- Multi-branch queries (`exec(mode="multi")`) are not provided: Neon has no
  multi-branch query semantics, and the scenarios' cross-branch reads run
  once per branch instead (recorded as the `multi_ref_exec: unsupported`
  implementation entry).

Quotas are capacity, not errors. Neon limits *active computes* per
project (20 on the Launch plan, the default branch exempt), not branches,
so the process keeps at most `NEON_ACTIVE_BUDGET` (18) computes active:
when a compute is needed above that, the least recently used one that no
connection holds is suspended first (`NEON_SUSPEND_ON_SWITCH=1` instead
suspends a branch's compute whenever the connection moves off it); a
suspended compute resumes on the next connect, and both land in `CONNECT`.
A connect refused for the compute limit waits and retries. All API calls share one process-wide token bucket under the
documented 700 requests/minute (`NEON_API_RATE_PER_MIN`, `NEON_API_BURST`);
bucket waits are part of the verb's cost and are summed in the e2e stats'
`backend_observations`, while reactive waits (429/423/503 backoff, compute
limit) are recorded as `API_RETRY_WAIT` rows. Connections use the unpooled
endpoint: the pooler runs PgBouncer in transaction mode, where session
state (temp tables, the `search_path` postgres_fdw sets on its remote
sessions) leaks between clients. Storage is the sum of the branches'
`logical_size` from the API; there is no GC hook.

---

## Environment Variables

Create a `.env` file for backend-specific configuration:

```bash
# Neon API key for programmatic access
NEON_API_KEY_ORG=your_key_here

# Database connection strings (if needed)
DOLT_CONNECTION_STRING=postgresql://user:pass@localhost:5432/dbname
NEON_CONNECTION_STRING=postgresql://user:pass@host.neon.tech/dbname
```

---

## Additional Resources

- **Scenario configurations**: See `macrobench/configs/` and `macrobench/task.proto`
- **Microbenchmark configs**: See `microbench/configs/` for example configurations
- **Database schemas**: See `db_setup/` for SQL dump files

