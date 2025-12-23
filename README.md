# Database Microbenchmark Framework

A benchmarking framework for testing PostgreSQL-compatible database backends (Dolt, Neon) with support for branching, reads, inserts, updates, and range operations. Supports both synthetic microbenchmarks and benchmarking real-world workloads.

## Quick Start

```bash
# 1. Setup environment
python3 -m venv venv
source venv/bin/activate
pip3 install .

# 2. Define your benchmark config
emacs microbench/test_config.textproto

# 3. Run the benchmark
python3 microbench/runner.py --config microbench/test_config.textproto

# 4. View results
python3 plot_latency.py /tmp/run_stats/<run_id>.parquet --output="figures/<run_id>.png"
```

---

## Benchmark Workflow

### Step 1: Define the Configuration

Edit `microbench/test_config.textproto` to configure your benchmark:

```protobuf
# Unique identifier for this benchmark run
run_id: "my_benchmark_001"

# Database backend. Supported backends are under dblib/
backend: NEON

# Operations to benchmark (can specify multiple)
operations: READ
operations: INSERT
operations: UPDATE
operations: RANGE_UPDATE
operations: BRANCH

# Total number of operations to perform
num_ops: 1000

# Target table (empty = random table from schema)
table_name: "customer"

# Starting branch (empty = main/default)
starting_branch: ""

# Database setup
database_setup {
  db_name: "microbench"
  cleanup: true  # Delete database after completion
  
  # Create from SQL dump
  sql_dump {
    sql_dump_path: "db_setup/microbench.sql"
  }
}

# Range update configuration
range_update_config {
  range_size: 100  # Number of rows per range operation
}

# Whether to autocommit transactions
autocommit: true
```

---

### Step 2: Run the Benchmark

```bash
python3 microbench/runner.py --config microbench/test_config.textproto
```

The benchmark will:
1. Create the database from the SQL dump (if configured)
2. Execute the specified operations randomly, with a seed for reproducibility
3. Record latency metrics for each operation
4. Output results to `/tmp/run_stats/<run_id>.parquet`
5. Clean up the database (if `cleanup: true`)

#### Optional Arguments

```bash
python3 microbench/runner.py --config <path> --seed <int>
```

| Argument | Description |
|----------|-------------|
| `--config` | Path to the textproto configuration file |
| `--seed` | Random seed for reproducibility |

---

### Step 3: View Results

Results are saved as Parquet files in `/tmp/run_stats/`.

#### Option A: Plot with Python

Generate a latency visualization with 95% confidence intervals:

```bash
# Display plot
python3 plot_latency.py /tmp/run_stats/<run_id>.parquet

# Save to file
python3 plot_latency.py /tmp/run_stats/<run_id>.parquet -o latency_chart.png

# Show detailed statistics
python3 plot_latency.py /tmp/run_stats/<run_id>.parquet --show-stats
```

#### Option B: Query with DuckDB

For advanced analysis, query the Parquet file directly with DuckDB:

```bash
duckdb
```

```sql
-- Load the benchmark results
SELECT * FROM '/tmp/run_stats/<run_id>.parquet' LIMIT 10;

-- Average latency by operation type
SELECT 
    op_type,
    COUNT(*) as count,
    AVG(latency) as avg_latency_sec,
    AVG(latency) * 1000 as avg_latency_ms,
    STDDEV(latency) * 1000 as stddev_ms
FROM '/tmp/run_stats/<run_id>.parquet'
GROUP BY op_type
ORDER BY avg_latency_sec DESC;

-- Per-key latency for range operations
SELECT 
    op_type,
    AVG(latency / GREATEST(num_keys_touched, 1)) * 1000 as per_key_latency_ms
FROM '/tmp/run_stats/<run_id>.parquet'
GROUP BY op_type;

-- Latency distribution percentiles
SELECT 
    op_type,
    QUANTILE_CONT(latency, 0.50) * 1000 as p50_ms,
    QUANTILE_CONT(latency, 0.95) * 1000 as p95_ms,
    QUANTILE_CONT(latency, 0.99) * 1000 as p99_ms
FROM '/tmp/run_stats/<run_id>.parquet'
GROUP BY op_type;
```

---

## Output Schema

The Parquet output file contains:

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Benchmark run identifier |
| `iteration_number` | int | Sequential operation number |
| `op_type` | int | Operation type enum (see below) |
| `latency` | float | Operation latency in seconds |
| `num_keys_touched` | int | Number of rows affected |
| `table_name` | string | Target table name |
| `table_schema` | string | Table DDL |
| `initial_db_size` | int | Database size at start |
| `sql_query` | string | Actual SQL executed |
| `random_seed` | int | Random seed used |

**Operation Types:**
- `0`: UNSPECIFIED
- `1`: BRANCH_CREATE
- `2`: BRANCH_CONNECT
- `3`: READ
- `4`: INSERT
- `5`: UPDATE
- `6`: COMMIT

---

## Prerequisites

1. **Python 3.11+** with virtual environment
2. **PostgreSQL-compatible backend**:
   - **Dolt**: Follow setup in their official docs: https://github.com/dolthub/doltgresql
   - **Neon**: Configure via Neon console
3. **psql** client for database setup

---

## Environment Variables

Create a `.env` file for optional configuration:

```bash
# Neon key for their API access
NEON_API_KEY_ORG=your_key_here
```
