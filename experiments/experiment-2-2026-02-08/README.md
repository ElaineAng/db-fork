# Experiment 2: Update Storage Overhead (Per-Operation Deltas)

**Date**: 2026-02-08
**Status**: Planned

## 1. Problem Statement

- Experiments 0 and 1 measure storage overhead at the granularity of branch
  creation, but not at the granularity of individual data operations.
- The storage cost that matters most for day-to-day use is **write
  amplification** — how much extra storage each UPDATE or RANGE_UPDATE
  consumes as a function of the number of existing branches.
- Backends using copy-on-write or structural sharing may incur per-write
  overhead that grows with the number of branches sharing the same pages.
- This overhead is not captured by branch creation measurements alone.

## 2. Objective

Quantify the **per-operation storage overhead of UPDATE and RANGE_UPDATE** for
each backend as the number of branches grows and across different branch tree
topologies. Specifically:

- Measure `disk_size_after - disk_size_before` tightly around each SQL
  statement execution.
- Determine whether per-update storage cost scales with branch count.
- Compare point updates (single key) vs. range updates (multiple keys) to
  assess whether per-key overhead is consistent.

## 3. Methodology

### 3.1 Independent Variables

| Variable | Values |
|----------|--------|
| Backend | Dolt, PostgreSQL CoW (file_copy), ~~Neon~~, ~~Xata~~ |
| Branch topology | SPINE, BUSHY, FAN_OUT |
| Number of branches | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |
| Operation | UPDATE, RANGE_UPDATE |
| Range size (RANGE_UPDATE only) | 1, 10, 20 (default in 2a), 50, 100 |

Each combination is an independent run with a fresh database instance.
Runs use `--measure-storage` (single-threaded only).

### 3.2 Fixed Parameters

| Parameter | Value |
|-----------|-------|
| Table schema | TPC-C `orders` |
| Inserts per branch (setup) | 100 |
| Updates per branch (setup) | 20 |
| Deletes per branch (setup) | 10 |
| Autocommit | true |
| `measure_storage` | true |
| `num_ops` | 50 (UPDATE), 20 (RANGE_UPDATE) |

### 3.3 Procedure

Each run follows the same four-phase structure. Phase 2 is identical to
experiment 1. The key contribution is Phase 3, where each data operation is
individually instrumented with storage measurement.

#### Phase 1 — Database Initialization

Same as experiment 0.

#### Phase 2 — Branch Setup (`*_setup.parquet`)

Identical to experiment 1: creates N branches using the topology's parent
selection rule. Branch creation is timed with storage measurement; data
operations are untimed.

> Setup parquet from this experiment can also serve experiment 1 analysis.

#### Phase 3 — Operation Measurement (`*.parquet`)

On the last created branch, execute the target operation with **per-operation
storage measurement** enabled.

##### Point UPDATE (num_ops = 50)

| Step | Action | Timed? | Storage? | Output Row? |
|------|--------|--------|----------|-------------|
| 1 | Record `num_keys_touched` = 1 | — | — | — |
| 2 | `disk_size_before` = `get_total_storage_bytes()` | — | **Yes** | — |
| 3 | **START TIMER** | — | — | — |
| 4 | `execute_sql(UPDATE ..., timed=False)` | — | — | — |
| 5 | (skip commit — autocommit = true) | — | — | — |
| 6 | **STOP TIMER** | — | — | — |
| 7 | `disk_size_after` = `get_total_storage_bytes()` | — | **Yes** | — |
| 8 | Record `sql_query` | — | — | — |
| 9 | Flush | — | — | **Row i** |
| 10 | Repeat × 50 | | | |

Each row records:

| Column | Description |
|--------|-------------|
| `disk_size_before` | Total storage (all branches) before the operation |
| `disk_size_after` | Total storage (all branches) after the operation |
| `latency` | Execute + commit wall-clock time (combined) |
| `op_type` | `UPDATE` |
| `num_keys_touched` | 1 |
| `sql_query` | The UPDATE statement with arguments |

> **Key difference from experiment 0 Phase 3**: storage IS measured here. The
> `execute_sql` and `commit_changes` calls use `timed=False` internally — the
> outer timer captures both together.

##### RANGE_UPDATE (num_ops = 20)

Identical to point UPDATE except:

| Column | Value |
|--------|-------|
| `num_keys_touched` | Actual count of keys in the selected range |
| `op_type` | `RANGE_UPDATE` |
| `sql_query` | `UPDATE ... WHERE (pk) >= ... AND (pk) <= ...` |

#### Phase 4 — Cleanup

Same as experiment 0.

## 4. Analysis

### Figure 2a: Point UPDATE Storage Overhead vs. Branch Count

- **X-axis**: number of branches (1, 2, 4, 8, ..., 1024)
- **Y-axis**: per-update storage delta = `disk_size_after - disk_size_before`
  (bytes), reported as **p50** and **p99** across the 50 operations per run
- **Series**: backend × topology

**Goal**: determine whether per-update write amplification grows with the
number of branches, and whether this varies by backend or topology.

### Figure 2b: RANGE_UPDATE Storage Overhead vs. Branch Count

- **X-axis**: number of branches (1, 2, 4, 8, ..., 1024)
- **Y-axis**: per-update storage delta (bytes), p50 and p99
- **Series**: backend × topology × range_size

**Goal**: assess how range size interacts with branch count in determining
storage overhead.

### Figure 2c: Per-Key Overhead Normalization

- **X-axis**: range_size (1, 10, 20, 50, 100)
- **Y-axis**: per-key storage delta =
  `(disk_size_after - disk_size_before) / num_keys_touched` (bytes)
- **Series**: backend × topology

**Goal**: determine whether per-key overhead is constant across range sizes.
If so, the per-key cost from a point update (range_size = 1) accurately
predicts the cost of range updates, simplifying the storage cost model.

### Research Questions

1. Does per-operation storage overhead grow with the number of branches?
2. Is the growth rate backend-dependent or topology-dependent?
3. Is per-key storage overhead constant across range sizes, or do range
   updates exhibit amortization or amplification effects?

## 5. Execution Plan

### 5.1 Prerequisites

- Dolt server running on port 5433 (`DOLT_PORT=5433` in `.env`)
- PostgreSQL available (started by `setup_pg_volume.sh`)

### 5.2 Data Directories

| Backend | Data directory | Storage measurement method |
|---------|---------------|---------------------------|
| **Dolt** | `${DOLT_DATA_DIR:-/tmp/doltgres_data/databases}/<db_name>/` | `st_blocks * 512` on data dir (content-addressed, CoW-aware) |
| **file_copy** | macOS: `/Volumes/PGBench/pgdata/` (isolated APFS volume) | `shutil.disk_usage()` on volume (CoW-aware via isolation) |

### 5.3 Output Directory

All parquet files written to: `/tmp/run_stats/`

| File pattern | Contents |
|-------------|----------|
| `<backend>_tpcc_<N>_<shape>_<op>[_r<range>]_setup.parquet` | Branch creation timing + storage (reusable for Exp 1 analysis) |
| `<backend>_tpcc_<N>_<shape>_<op>[_r<range>].parquet` | Per-operation UPDATE/RANGE_UPDATE timing + storage |

### 5.4 Steps

```bash
./experiments/experiment-2-2026-02-08/run.sh
```

The script sources `bench_lib.sh` and runs two sub-experiments:

**Exp 2a**: UPDATE + RANGE_UPDATE with fixed range_size=20, all topologies

- 3 shapes (spine, bushy, fan_out) x 2 backends (dolt, file_copy) = 6 branch sweeps
- Each sweep runs 11 branch counts (1, 2, 4, ..., 1024) x 2 operations (UPDATE, RANGE_UPDATE)
- Total: 6 x 11 x 2 = 132 benchmark runs
- num_ops per run: 50 (UPDATE), 20 (RANGE_UPDATE)
- Neon and Xata are commented out for now

**Exp 2b**: RANGE_UPDATE with varying range_size, spine only

- 4 range sizes (1, 10, 50, 100) x 2 backends (dolt, file_copy) = 8 branch sweeps
- Each sweep runs 11 branch counts x 1 operation (RANGE_UPDATE)
- Total: 8 x 11 = 88 benchmark runs
- num_ops per run: 20
- range_size=20 is already covered by Exp 2a and not repeated here

### 5.5 Expected Output

**Exp 2a**: 264 parquet files (2 backends x 3 shapes x 11 branch counts x 2 operations x 2 file types)
- Setup: `dolt_tpcc_1_spine_update_setup.parquet` ... `file_copy_tpcc_1024_fan_out_range_update_r20_setup.parquet`
- Measurement: `dolt_tpcc_1_spine_update.parquet` ... `file_copy_tpcc_1024_fan_out_range_update_r20.parquet`

**Exp 2b**: 176 parquet files (2 backends x 4 range sizes x 11 branch counts x 2 file types)
- Setup: `dolt_tpcc_1_spine_range_update_r1_setup.parquet` ... `file_copy_tpcc_1024_spine_range_update_r100_setup.parquet`
- Measurement: `dolt_tpcc_1_spine_range_update_r1.parquet` ... `file_copy_tpcc_1024_spine_range_update_r100.parquet`
