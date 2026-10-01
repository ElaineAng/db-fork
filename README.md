# Database Benchmarking Framework

A parametrized and extensible benchmarking framework for testing PostgreSQL-compatible branchable database backends (Dolt, Neon, etc.) with support for branching, schema, and data related operations. Includes both macrobenchmark and microbenchmark workloads.

## Quick Start

```bash
# 1. Setup environment (installs Python 3.13 and all dependencies into .venv)
#    Requires uv (https://docs.astral.sh/uv/) and protoc (brew install protobuf)
uv sync

# 2. Run a macrobenchmark 
# Mini config, always start with this
./scripts/run_macrobench.sh --mini --outdir run_stats software_dev dolt 1 db_setup/ch-w1.sql

# Full config with 2hr timeout
./scripts/run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 software_dev dolt 5 db_setup/ch-w5.sql

# 3. Generate comparison plots
uv run python scripts/plotting/macro_comparison.py --dolt-dir run_stats_final/macro/dolt_full --neon-dir run_stats_final/macro/neon_full --outdir figures/

# 4. Run a microbenchmark (latency)
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# 5. Run a microbenchmark (throughput)
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional
```

All commands are run from the repository root.

### Repository Layout

```
dblib/              # Backend tool suites (Dolt, Neon, Xata, ...) and result collection
microbench/         # Microbenchmark runners and operations
macrobench/         # Macrobenchmark workflows and runner
util/               # Shared helpers (SQL loading, DB utilities)
agent/              # LLM agent workloads (install with `uv sync --extra agent`)
db_setup/           # SQL dumps/schemas and database setup scripts
scripts/            # Benchmark entry points (run_*.sh) and bench_lib.sh
scripts/plotting/   # Plotting and analysis scripts
```

---

## Table of Contents

- [Macrobenchmarks](#macrobenchmarks)
- [Microbenchmarks](#microbenchmarks)
  - [Latency Benchmarks](#latency-benchmarks)
  - [Throughput Benchmarks](#throughput-benchmarks)
- [Plotting Results](#plotting-results)
- [Output Files](#output-files)

---

## Macrobenchmarks

Macrobenchmarks simulate real-world workflows with multiple concurrent workers performing sequences of database operations.

### Running Macrobenchmarks

Use the `scripts/run_macrobench.sh` script (run it from the repository root):

```bash
./scripts/run_macrobench.sh [OPTIONS] <workflow> <backend> <db_scale> <sql_path>
```

#### Arguments

| Argument | Description | Options |
|----------|-------------|---------|
| `workflow` | Workflow type | `software_dev`, `failure_repro`, `data_cleaning`, `mcts`, `simulation` |
| `backend` | Database backend | `dolt`, `dolt_mysql`, `neon`, `kpg`, `xata`, `file_copy`, `txn` |
| `db_scale` | Database scale (number of warehouses) | Integer (e.g., `1`, `5`, `10`) |
| `sql_path` | Path to SQL schema dump | e.g., `db_setup/ch-w1.sql`, `db_setup/ch-w5.sql` |

#### Options

| Option | Description |
|--------|-------------|
| `--mini` | Use mini config (fewer workers/steps, suitable for testing) |
| `--outdir DIR` | Output directory (default: `run_stats/`) |
| `--max-runtime-sec N` | Cap total runtime in seconds (0 = no limit) |
| `--measure-storage` | Enable Neon storage measurement (15-min sleep before/after) |

#### Examples

```bash
# Run software development workflow on Dolt with 5 warehouses
./scripts/run_macrobench.sh software_dev dolt 5 db_setup/ch-w5.sql

# Run MCTS workflow on Neon with mini config
./scripts/run_macrobench.sh --mini mcts neon 1 db_setup/ch-w1.sql

# Run simulation workflow on Neon with custom output directory
./scripts/run_macrobench.sh --outdir run_stats/neon_mini simulation neon 1 db_setup/ch-w1.sql

# Run with runtime limit (10 minutes)
./scripts/run_macrobench.sh --max-runtime-sec 600 data_cleaning dolt 5 db_setup/ch-w5.sql

# Run with storage measurement for Neon
./scripts/run_macrobench.sh --measure-storage mcts neon 5 db_setup/ch-w5.sql
```

#### Output Files

Macrobenchmark results are saved to the output directory (default: `run_stats/`):

```
run_stats/
├── macro_<workflow>_<backend>_<scale>.parquet           # Operation-level latency data
└── macro_<workflow>_<backend>_<scale>_e2e_stats.json    # End-to-end statistics
```

---

## Microbenchmarks

Microbenchmarks measure latency and throughput for specific database operations under controlled conditions.

### Latency Benchmarks

Measure operation latency with varying numbers of branches and threads.

#### Single-Threaded Latency

Use `scripts/run_single_thread_bench.sh` to measure single-threaded operation latency:

```bash
./scripts/run_single_thread_bench.sh <backend> <sql_dump_path> <num_branches> [OPTIONS]
```

##### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `neon`, `kpg`, `xata`, `file_copy`, `txn`, `tiger` |
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

#### Multi-Threaded Latency

Use `scripts/run_multithread_bench.sh` to measure multi-threaded operation latency:

```bash
./scripts/run_multithread_bench.sh <backend> <sql_dump_path> [OPTIONS]
```

##### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `neon`, `kpg`, `xata`, `file_copy`, `txn`, `tiger` |
| `sql_dump_path` | Path to SQL dump file |

##### Options

| Option | Description |
|--------|-------------|
| `--seed <seed>` | Random seed for reproducibility |
| `--max-branches <max>` | Maximum number of branches (default: 1024) |
| `--shape <shape>` | Branch tree shape: `spine`, `bushy`, or `fan_out` (default: `spine`) |
| `--num-ops <n>` | Number of operations to perform |
| `--operations <ops>` | Comma-separated list (e.g., `READ,UPDATE`) |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

##### Examples

```bash
# Sweep from 2 to 1024 branches (threads = branches at each configuration)
./scripts/run_multithread_bench.sh dolt db_setup/tpcc_schema.sql

# Test up to 128 branches
./scripts/run_multithread_bench.sh dolt db_setup/tpcc_schema.sql --max-branches 128

# Run only READ and UPDATE operations with 100 ops per test
./scripts/run_multithread_bench.sh neon db_setup/tpcc_schema.sql --operations READ,UPDATE --num-ops 100
```

**Note:** In multi-threaded latency benchmarks, the number of threads always equals the number of branches. For independent thread/branch control, use throughput benchmarks.

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
└── multithread/
    └── (similar structure for multi-threaded runs)
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
checked out on the thread's branch. Async mode supports `dolt`, `dolt_mysql`
and `neon`; use `--mode sync` for the other backends (one op at a time per
thread). Every point of a sweep uses the same mode, including concurrency 1.

Each thread works on one branch, assigned round-robin. With fewer threads
than branches, the extra branches exist but get no load; with more threads
than branches, threads share branches.

#### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `dolt_mysql`, `neon`, `kpg`, `xata`, `txn`, `file_copy`, `tiger` |
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

### Macrobenchmark Comparison Plots

Compare macrobenchmark results between Dolt and Neon:

```bash
uv run python scripts/plotting/macro_comparison.py \
    --dolt-dir <dolt_results_dir> \
    --neon-dir <neon_results_dir> \
    --outdir <output_figures_dir>
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `--dolt-dir` | Directory with Dolt parquet files |
| `--neon-dir` | Directory with Neon parquet files |
| `--outdir` | Directory to save figures (default: `macro-analysis/figures_comparison`) |
| `--label-position` | Position for step labels as `x,y` in axes coordinates (default: `0.98,0.05`) |
| `--label-fontsize` | Font size for step labels (default: 16) |

#### Generated Plots

The script generates the following figures in the output directory:

- `latency_boxplot_comparison.png` - Box plots of latency by operation type
- `time_breakdown_comparison.png` - Stacked bar chart of time breakdown by operation
- `heatmap_comparison.png` - Heatmap showing latency comparison with ratios
- `elapsed_time_comparison.png` - Elapsed time comparison by workflow
- `steps_over_time.png` - Steps completion over time

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
run_stats_final/macro/
├── dolt_full/          # Full-scale Dolt runs
│   ├── macro_software_dev_dolt_5.parquet
│   ├── macro_software_dev_dolt_5_e2e_stats.json
│   ├── macro_failure_repro_dolt_5.parquet
│   ├── macro_data_cleaning_dolt_5.parquet
│   └── macro_mcts_dolt_5.parquet
├── dolt_mini/          # Mini-scale Dolt runs (for testing)
├── neon_full/          # Full-scale Neon runs
└── neon_mini/          # Mini-scale Neon runs
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

### Parquet Schema (TODO)

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

Install with `uv sync`, or `uv sync --extra agent` to also get the LLM agent
dependencies used by `agent/`. The project is installed in editable mode, and
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
of `concurrent_requests` connections, all checked out on the thread's branch.

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

- **Workflow configurations**: See `macrobench/configs/` for workflow definitions
- **Microbenchmark configs**: See `microbench/configs/` for example configurations
- **Database schemas**: See `db_setup/` for SQL dump files

