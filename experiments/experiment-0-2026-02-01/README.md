# Experiment 0: Branch Creation Storage Scaling (Spine Topology)

**Date**: 2026-02-01
**Status**: Done
**Report**: [results/report.md](results/report.md)

## 1. Problem Statement

- Database branching implementations differ fundamentally in how they represent
  branch metadata and data copies — some use copy-on-write (CoW), others
  duplicate data eagerly.
- The marginal storage cost of creating the nth branch is unknown and likely
  backend-dependent.
- It is unclear whether that cost grows, shrinks, or stays constant as the
  number of branches increases.

## 2. Objective

Quantify the **marginal storage overhead of branch creation** for each backend
as the total number of branches increases, under a fixed linear (spine)
topology. Specifically:

- Measure `disk_size_after - disk_size_before` for each branch creation event.
- Determine whether marginal cost is constant, linear, or super-linear in the
  number of existing branches.
- Compare storage efficiency across backends.

## 3. Methodology

### 3.1 Independent Variables

| Variable | Values |
|----------|--------|
| Backend | Dolt, PostgreSQL CoW, Neon, Xata |
| Number of branches | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |
| Target operation | BRANCH, CONNECT, READ, UPDATE, RANGE_READ, RANGE_UPDATE |

Each combination is an **independent run** with a fresh database instance.

### 3.2 Fixed Parameters

| Parameter | Value |
|-----------|-------|
| Table schema | TPC-C `orders` |
| Branch topology | SPINE (linear chain) |
| Inserts per branch | 100 |
| Updates per branch | 20 |
| Deletes per branch | 10 |
| Autocommit | true |
| Measurement ops | 1000 (UPDATE), 1 (BRANCH), 200 (RANGE_UPDATE) |

### 3.3 Procedure

Each run consists of four phases.

#### Phase 1 — Database Initialization

1. `CREATE DATABASE microbench`
2. Load TPC-C `orders` schema and seed data from SQL dump
3. (Dolt only) `CALL dolt_commit_all()`

#### Phase 2 — Branch Setup (`*_setup.parquet`)

Creates N branches in a linear chain. Each branch creation is **timed with
storage measurement**; intervening data operations are **untimed** and produce
no output rows.

Step-by-step example for N = 8:

| Step | Action | Timed? | Storage? | Output Row? |
|------|--------|--------|----------|-------------|
| 1 | 100 INSERTs on main | No | No | No |
| 2 | 20 UPDATEs + 10 DELETEs on main | No | No | No |
| 3 | COMMIT | No | No | No |
| 4 | `disk_size_before` = `get_total_storage_bytes()` | — | **Yes** | — |
| 5 | `CREATE BRANCH setup_branch_1` from main | **Yes** | — | — |
| 6 | `disk_size_after` = `get_total_storage_bytes()` | — | **Yes** | — |
| 7 | Flush | — | — | **Row 1** |
| 8 | Connect to setup_branch_1 | No | No | No |
| 9 | 100 INSERTs on setup_branch_1 | No | No | No |
| 10 | 20 UPDATEs + 10 DELETEs on setup_branch_1 | No | No | No |
| 11 | COMMIT | No | No | No |
| 12 | `disk_size_before` = `get_total_storage_bytes()` | — | **Yes** | — |
| 13 | `CREATE BRANCH setup_branch_2` from setup_branch_1 | **Yes** | — | — |
| 14 | `disk_size_after` = `get_total_storage_bytes()` | — | **Yes** | — |
| 15 | Flush | — | — | **Row 2** |
| ... | Repeat for branches 3–8 | ... | ... | ... |

**Output**: N rows, one per branch creation. Each row records:

| Column | Description |
|--------|-------------|
| `disk_size_before` | Total storage immediately before branch creation |
| `disk_size_after` | Total storage immediately after branch creation |
| `latency` | Branch creation wall-clock time |
| `op_type` | `BRANCH_CREATE` |

> **Note**: Data operations (steps 9–11) occur *between* consecutive
> measurement windows. They increase absolute storage but do **not** affect
> any row's delta (`disk_size_after - disk_size_before`), because each
> measurement window wraps only the `CREATE BRANCH` call.

#### Phase 3 — Operation Measurement (`*.parquet`)

On the last branch (`setup_branch_N`), execute the target operation repeatedly:

| Step | Action | Timed? | Storage? | Output Row? |
|------|--------|--------|----------|-------------|
| 1 | Execute operation (e.g. `UPDATE` one row) | **Yes** | No | **Yes** |
| 2 | Repeat × `num_ops` | **Yes** | No | **Yes** |

**Output**: `num_ops` rows. Each row records `latency`, `op_type`,
`sql_query`, and `num_keys_touched`. Storage columns are zero (not measured
in this phase).

#### Phase 4 — Cleanup

Drop database and delete backend project (Neon/Xata).

### 3.4 Data Sources for Analysis

| Analysis | Source | Phase |
|----------|--------|-------|
| Storage scaling | `*_setup.parquet` | Phase 2 |
| Operation latency | `*.parquet` | Phase 3 |

## 4. Analysis

### Storage Scaling (from Phase 2)

- **X-axis**: nth branch created (1, 2, 3, ..., N)
- **Y-axis**: marginal storage delta = `disk_size_after - disk_size_before` (bytes)
- **Series**: one per backend
- **Goal**: characterize how marginal branch creation cost evolves as the
  branch count grows under spine topology

### Operation Latency (from Phase 3)

- **X-axis**: number of branches (1, 2, 4, ..., 1024)
- **Y-axis**: operation latency (p50, p99)
- **Series**: one per backend × operation type
- **Goal**: determine whether operation latency degrades as the branch count
  increases
