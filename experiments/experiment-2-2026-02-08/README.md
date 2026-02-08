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
| Backend | Dolt, PostgreSQL CoW, Neon, Xata |
| Branch topology | SPINE, BUSHY, FAN_OUT |
| Number of branches | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |
| Operation | UPDATE, RANGE_UPDATE |
| Range size (RANGE_UPDATE only) | 1, 10, 20, 50, 100 |

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
