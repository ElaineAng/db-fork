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
| Backend | Dolt, PostgreSQL CoW, Neon, Xata |
| Branch topology | SPINE, BUSHY, FAN_OUT |
| Number of branches | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 |

Each combination is an independent run with a fresh database instance.
**Spine data already exists** from experiment 0 and does not need to be re-collected.

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
