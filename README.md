# Database Benchmarking Framework

A benchmarking framework for testing PostgreSQL-compatible branchable database backends (Dolt, Neon, etc.) with support for branching, schema, and data related operations. Includes both macrobenchmark and microbenchmark workloads.

## Quick Start

```bash
# 1. Setup environment
python3 -m venv venv
source venv/bin/activate
pip3 install .

# 2. Run a macrobenchmark 
```
# Mini config, always start with this
./run_macrobench.sh --mini --outdir run_stats software_dev dolt 1 db_setup/ch-w1.sql

# Full config with 2hr timeout
./run_macrobench.sh --outdir run_stats --max-runtime-sec 7200 software_dev dolt 5 db_setup/ch-w5.sql
```
# 3. Run a microbenchmark (latency)
./run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# 4. Run a microbenchmark (throughput)
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional

# 5. Generate comparison plots
python scripts/macro_comparison.py --dolt-dir run_stats_final/macro/dolt_full --neon-dir run_stats_final/macro/neon_full --outdir figures/
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

Use the `run_macrobench.sh` script in the root directory:

```bash
./run_macrobench.sh [OPTIONS] <workflow> <backend> <db_scale> <sql_path>
```

#### Arguments

| Argument | Description | Options |
|----------|-------------|---------|
| `workflow` | Workflow type | `software_dev`, `failure_repro`, `data_cleaning`, `mcts`, `simulation` |
| `backend` | Database backend | `dolt`, `neon`, `kpg`, `xata`, `file_copy`, `txn` |
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
./run_macrobench.sh software_dev dolt 5 db_setup/ch-w5.sql

# Run MCTS workflow on Neon with mini config
./run_macrobench.sh --mini mcts neon 1 db_setup/ch-w1.sql

# Run simulation workflow on Neon with custom output directory
./run_macrobench.sh --outdir run_stats/neon_mini simulation neon 1 db_setup/ch-w1.sql

# Run with runtime limit (10 minutes)
./run_macrobench.sh --max-runtime-sec 600 data_cleaning dolt 5 db_setup/ch-w5.sql

# Run with storage measurement for Neon
./run_macrobench.sh --measure-storage mcts neon 5 db_setup/ch-w5.sql
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

Use `run_single_thread_bench.sh` to measure single-threaded operation latency:

```bash
./run_single_thread_bench.sh <backend> <sql_dump_path> <num_branches> [OPTIONS]
```

##### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `neon`, `kpg`, `xata`, `file_copy`, `txn`, `tiger` |
| `sql_dump_path` | Path to SQL dump file (e.g., `db_setup/tpcc_schema.sql`) |
| `num_branches` | Number of branches to create for testing |

##### Options

| Option | Description |
|--------|-------------|
| `--seed <seed>` | Random seed for reproducibility |
| `--shape <shape>` | Branch tree shape: `spine`, `bushy`, or `fan_out` (default: `spine`) |
| `--measure-storage` | Measure disk size before/after each update |
| `--operations <ops>` | Comma-separated list (e.g., `UPDATE,RANGE_UPDATE`) |
| `--range-size <n>` | Range size for RANGE_UPDATE operation (default: 200) |
| `--num-ops <n>` | Number of operations to perform |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

##### Examples

```bash
# Run with 16 branches
./run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16

# Run with custom seed and bushy branch shape
./run_single_thread_bench.sh neon db_setup/tpcc_schema.sql 32 --seed 12345 --shape bushy

# Run only UPDATE operations with storage measurement
./run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 8 --operations UPDATE --measure-storage

# Run with custom range size
./run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16 --operations RANGE_UPDATE --range-size 500
```

#### Multi-Threaded Latency

Use `run_multithread_bench.sh` to measure multi-threaded operation latency:

```bash
./run_multithread_bench.sh <backend> <sql_dump_path> [OPTIONS]
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
./run_multithread_bench.sh dolt db_setup/tpcc_schema.sql

# Test up to 128 branches
./run_multithread_bench.sh dolt db_setup/tpcc_schema.sql --max-branches 128

# Run only READ and UPDATE operations with 100 ops per test
./run_multithread_bench.sh neon db_setup/tpcc_schema.sql --operations READ,UPDATE --num-ops 100
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

Measure throughput (operations per second) with independent control over threads and branches.

#### Running Throughput Benchmarks

Use `run_throughput_bench.sh` with one of three sweep modes:

```bash
# Sweep threads (fix branches, vary threads)
./run_throughput_bench.sh <backend> <sql_dump_path> --sweep-threads --branches <N> [OPTIONS]

# Sweep branches (fix threads, vary branches)
./run_throughput_bench.sh <backend> <sql_dump_path> --sweep-branches --threads <N> [OPTIONS]

# Sweep proportionally (vary both threads and branches together)
./run_throughput_bench.sh <backend> <sql_dump_path> --sweep-proportional [OPTIONS]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `backend` | Database backend: `dolt`, `neon`, `kpg`, `xata`, `txn`, `file_copy`, `tiger` |
| `sql_dump_path` | Path to SQL dump file |
| `--sweep-threads` | Fix branches, vary threads (requires `--branches`) |
| `--sweep-branches` | Fix threads, vary branches (requires `--threads`) |
| `--sweep-proportional` | Vary both threads and branches proportionally |

#### Options

| Option | Description |
|--------|-------------|
| `--threads <N>` | Fixed thread count (for `--sweep-branches` mode) |
| `--branches <N>` | Fixed branch count (for `--sweep-threads` mode) |
| `--threads-per-branch <N>` | Threads per branch ratio for `--sweep-proportional` (default: 4) |
| `--thread-list <list>` | Comma-separated thread counts (e.g., `1,2,4,8,16`) |
| `--branch-list <list>` | Comma-separated branch counts (e.g., `1,2,4,8,16`) |
| `--seed <seed>` | Random seed for reproducibility |
| `--num-ops <n>` | Number of operations per test |
| `--operations <ops>` | Comma-separated list (e.g., `READ,RANGE_READ`) |
| `--output-dir <dir>` | Output directory (default: `/tmp/run_stats`) |

#### Examples

```bash
# Fix branches at 1, vary threads: 1,2,4,8,16,32,64,128
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-threads --branches 1

# Fix threads at 128, vary branches: 1,2,4,8,16,32
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-branches --threads 128

# Vary both proportionally (default: 4 threads per branch)
./run_throughput_bench.sh neon db_setup/ch-w1.sql --sweep-proportional

# Custom thread/branch lists
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-threads --branches 16 --thread-list "1,2,4,8,16,32"
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-branches --threads 128 --branch-list "1,2,4,8,16"

# Proportional with custom ratio (8 threads per branch)
./run_throughput_bench.sh neon db_setup/ch-w1.sql --sweep-proportional --threads-per-branch 8

# Run only specific operations
./run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional --operations READ,RANGE_READ
```

#### Output Files

Throughput benchmark results are saved to the output directory:

```
<output_dir>/
├── <backend>_<dataset>_tp_t<threads>_b<branches>.parquet
├── <backend>_<dataset>_tp_t<threads>_b<branches>_<operation>_threads<threads>_summary.json
└── <backend>_<dataset>_tp_t<threads>_b<branches>_setup.parquet
```

---

## Plotting Results

After running benchmarks, use the plotting scripts in the `scripts/` directory to generate visualizations.

### Macrobenchmark Comparison Plots

Compare macrobenchmark results between Dolt and Neon:

```bash
python scripts/macro_comparison.py \
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

#### Examples

```bash
# Compare full-scale macrobenchmarks
python scripts/macro_comparison.py \
    --dolt-dir run_stats_final/macro/dolt_full \
    --neon-dir run_stats_final/macro/neon_full \
    --outdir figures/macro_comparison

# Compare mini macrobenchmarks
python scripts/macro_comparison.py \
    --dolt-dir run_stats_final/macro/dolt_mini \
    --neon-dir run_stats_final/macro/neon_mini \
    --outdir figures/macro_mini_comparison
```

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
python scripts/plot_branch_latency_micro.py \
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
python scripts/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/branch \
    --outdir figures/ \
    --operation branch

# Plot branch connection latency only
python scripts/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation connect

# Plot both branch and connect (separate plots)
python scripts/plot_branch_latency_micro.py \
    --data-dir run_stats_final/micro/single_thread/branch \
    --outdir figures/ \
    --operation both

# Plot combined (branch and connect on single plot)
python scripts/plot_branch_latency_micro.py \
    --branch-dir run_stats_final/micro/single_thread/branch \
    --connect-dir run_stats_final/micro/single_thread/connect \
    --outdir figures/ \
    --operation combined
```

### Microbenchmark Throughput Plots

Plot throughput vs threads/branches:

```bash
python scripts/plot_throughput_micro.py \
    --data-dir <data_directory> \
    --output-dir <output_figures_dir> \
    [--operation <operation_type>]
```

#### Arguments

| Argument | Description |
|----------|-------------|
| `--data-dir` | Directory with throughput summary JSON files |
| `--output-dir` | Directory to save figures |
| `--operation` | (Optional) Specific operation type (e.g., `READ`, `RANGE_READ`, `UPDATE`) |

#### Examples

```bash
# Plot all operations from proportional sweep
python scripts/plot_throughput_micro.py \
    --data-dir run_stats_final/micro/tp_proportional \
    --output-dir figures/throughput

# Plot specific operation
python scripts/plot_throughput_micro.py \
    --data-dir run_stats_final/micro/tp_proportional \
    --operation READ \
    --output-dir figures/throughput

# Plot fixed-branch sweep results
python scripts/plot_throughput_micro.py \
    --data-dir run_stats_final/micro/tp_fix_branch \
    --output-dir figures/throughput_fixed_branch
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

### Parquet Schema

#### Macrobenchmark Parquet

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Benchmark run identifier |
| `iteration_number` | int | Sequential operation number |
| `op_type` | int | Operation type (see below) |
| `latency` | float | Operation latency in seconds |
| `sql_query` | string | Actual SQL executed |
| `thread_id` | int | Worker thread ID |
| `step_id` | int | Workflow step ID |
| `branch_count` | int | Current number of branches |

#### Microbenchmark Parquet

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Benchmark run identifier |
| `iteration_number` | int | Sequential operation number |
| `op_type` | int | Operation type (see below) |
| `latency` | float | Operation latency in seconds |
| `num_keys_touched` | int | Number of rows affected |
| `table_name` | string | Target table name |
| `sql_query` | string | Actual SQL executed |
| `random_seed` | int | Random seed used |

### Operation Types

| Code | Operation | Description |
|------|-----------|-------------|
| `0` | UNSPECIFIED | Unspecified operation |
| `1` | BRANCH_CREATE | Create a new branch |
| `2` | BRANCH_CONNECT | Connect to a branch |
| `3` | READ | Single-row read |
| `4` | INSERT | Insert operation |
| `5` | UPDATE | Update operation |
| `6` | COMMIT | Commit transaction |
| `7` | DDL | DDL operation (schema change) |
| `8` | BRANCH_DELETE | Delete a branch |
| `9` | API_RETRY_WAIT | API retry wait (overhead) |

---

## Prerequisites

1. **Python 3.11+** with virtual environment
2. **PostgreSQL-compatible backend**:
   - **Dolt**: Follow setup at https://github.com/dolthub/doltgresql
   - **Neon**: Configure via Neon console
3. **psql** client for database setup
4. **Required Python packages**: Install with `pip install .`

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
- **Library functions**: See `bench_lib.sh` for reusable benchmark utilities

---

## Troubleshooting

### Common Issues

1. **Database connection errors**: Check that your backend is running and accessible
2. **Permission errors**: Ensure your database user has CREATE/DROP permissions
3. **Missing dependencies**: Run `pip install .` to install all required packages
4. **Out of memory**: Reduce `--max-branches` or `--num-ops` for smaller tests
5. **Neon rate limits**: Add delays between runs or use `--measure-storage` cautiously

### Getting Help

For issues or questions:
- Check the documentation in each script's header comments
- Review the example configurations in `macrobench/configs/` and `microbench/configs/`
- Examine the benchmark library functions in `bench_lib.sh`
