# Experiment 1: Branch Creation Storage Overhead (Varying Topology)

**Date**: 2026-02-08
**Status**: Planned

## 1. Problem Statement

- Experiment 0 measured branch creation storage under a single topology
  (spine — linear chain).
- In practice, branch trees take many forms: deep linear chains, wide
  fan-outs from a single root, or balanced bushy trees with random parentage.
- Different topologies may exercise different code paths in each backend's
  branching implementation, leading to different storage characteristics.
- It is unknown whether the marginal cost of the nth branch depends on the
  tree shape.

## 2. Objective

Determine whether **branch tree topology** affects the marginal storage cost of
branch creation. Specifically:

- Measure per-branch-creation storage delta across three topologies (spine,
  bushy, fan-out) for each backend.
- Compare the scaling curves to identify topology-sensitive backends.
- Reuse spine data from experiment 0 where applicable.

## 3. Methodology

### 3.1 Independent Variables

| Variable | Values |
|----------|--------|
| Backend | Dolt, PostgreSQL CoW (file_copy), ~~Neon~~, ~~Xata~~ |
| Branch topology | SPINE, BUSHY, FAN_OUT |
| Number of branches | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |

Each combination is an independent run with a fresh database instance.

### 3.2 Fixed Parameters

| Parameter | Value |
|-----------|-------|
| Table schema | TPC-C `orders` |
| Inserts per branch | 100 |
| Updates per branch | 20 |
| Deletes per branch | 10 |
| Autocommit | true |

### 3.3 Topology Definitions

| Topology | Parent Selection Rule |
|----------|---------------------|
| SPINE | Always branch from the most recently created branch (linear chain) |
| FAN_OUT | Always branch from root (`main`) |
| BUSHY | Branch from a uniformly random existing branch |

### 3.4 Procedure

Each run follows the same four-phase structure as experiment 0. The only
difference is the parent selection rule during Phase 2.

#### Phase 1 — Database Initialization

Same as experiment 0.

#### Phase 2 — Branch Setup (`*_setup.parquet`)

Creates N branches using the topology's parent selection rule. Each branch
creation is **timed with storage measurement**; data operations are **untimed**.

Step-by-step example for N = 8, BUSHY topology:

| Step | Action | Timed? | Storage? | Output Row? |
|------|--------|--------|----------|-------------|
| 1 | 100 INSERTs + 20 UPDATEs + 10 DELETEs on main | No | No | No |
| 2 | COMMIT | No | No | No |
| 3 | `disk_size_before` = `get_total_storage_bytes()` | — | **Yes** | — |
| 4 | `CREATE BRANCH setup_branch_1` from main | **Yes** | — | — |
| 5 | `disk_size_after` = `get_total_storage_bytes()` | — | **Yes** | — |
| 6 | Flush | — | — | **Row 1** |
| 7 | Connect to setup_branch_1 | No | No | No |
| 8 | 100 INSERTs + 20 UPDATEs + 10 DELETEs on setup_branch_1 | No | No | No |
| 9 | COMMIT | No | No | No |
| 10 | `disk_size_before` = `get_total_storage_bytes()` | — | **Yes** | — |
| 11 | `CREATE BRANCH setup_branch_2` from *random existing branch* | **Yes** | — | — |
| 12 | `disk_size_after` = `get_total_storage_bytes()` | — | **Yes** | — |
| 13 | Flush | — | — | **Row 2** |
| 14 | Connect to setup_branch_2 | No | No | No |
| 15 | 100 INSERTs + 20 UPDATEs + 10 DELETEs on setup_branch_2 | No | No | No |
| ... | Repeat for branches 3–8 | ... | ... | ... |

**Output**: N rows, one per branch creation. Schema identical to experiment 0
setup parquet:

| Column | Description |
|--------|-------------|
| `disk_size_before` | Total storage immediately before branch creation |
| `disk_size_after` | Total storage immediately after branch creation |
| `latency` | Branch creation wall-clock time |
| `op_type` | `BRANCH_CREATE` |

#### Phase 3 — Operation Measurement

Runs but **data is not used** for this experiment's analysis.

#### Phase 4 — Cleanup

Same as experiment 0.

## 4. Analysis

### Figure 1: Marginal Branch Creation Storage by Topology

- **X-axis**: branch index (nth branch created: 1, 2, 3, ..., N)
- **Y-axis**: marginal storage delta = `disk_size_after - disk_size_before` (bytes)
- **Series**: backend × topology
- **Layout**: one facet per backend, or one combined figure

### Research Questions

1. Does the marginal storage cost of the nth branch differ across topologies
   for the same backend?
2. Do any backends exhibit constant marginal cost regardless of topology?
3. Does fan-out (shallow, wide) produce lower or higher overhead than spine
   (deep, narrow)?

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
| `<backend>_tpcc_<N>_<shape>_setup.parquet` | Branch creation timing + storage (N rows per file) |
| `<backend>_tpcc_<N>_<shape>.parquet` | Phase 3 operation measurement (not used for this experiment) |

### 5.4 Steps

```bash
./experiments/experiment-1-2026-02-08/run.sh
```

The script sources `bench_lib.sh` and runs:

- 3 shapes (spine, bushy, fan_out) x 2 backends (dolt, file_copy) = 6 branch sweeps
- Each sweep runs 11 branch counts (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024) x 1 operation (BRANCH)
- Total: 6 x 11 = 66 benchmark runs
- num_ops per run: 1 (single branch creation, timed with storage measurement)
- Neon and Xata are commented out for now

### 5.5 Expected Output

66 parquet files in `/tmp/run_stats/` (2 backends x 3 shapes x 11 branch counts):
- `dolt_tpcc_1_spine_setup.parquet` through `dolt_tpcc_1024_fan_out_setup.parquet`
- `file_copy_tpcc_1_spine_setup.parquet` through `file_copy_tpcc_1024_fan_out_setup.parquet`

Plus 66 corresponding measurement parquets (not used for analysis).
