# Experiment 3: Operation Throughput Under Branching

**Status**: Planning

## Research Questions

**RQ1: Does branch creation throughput degrade with concurrent threads?**

Multiple threads creating branches simultaneously may contend on metadata
(catalog locks, WAL, chunk store). Even if 1 thread can create 10 branches,
10 threads may not sustain 100 branches due to metadata contention.

**RQ2: Does the number of branches degrade per-branch operation throughput?**

In CoW systems, branches share data. When N branches are active concurrently,
do they contend for shared resources (buffer pool, chunk store, pageserver)
and degrade each other's CRUD throughput?

**RQ3: Does topology affect throughput distribution?**

Spine topology accumulates data in later branches (larger DBs, deeper clone
chains). Fan-out branches from a constant-size root. Does this asymmetry
cause uneven throughput across branches?

## Metrics

### Exp 3a: Branch Creation Throughput

$$\text{branch\_throughput}(T) = \frac{\sum_{t=1}^{T} \text{branches\_created}(t)}{\Delta t}$$

where $T$ = number of concurrent threads, $\text{branches\_created}(t)$ =
successful branch creations by thread $t$, $\Delta t$ = 30s.

### Exp 3b: Per-Branch CRUD Goodput

$$\text{goodput}(i) = \frac{\text{successful\_ops}(i)}{\Delta t}$$

where $i$ = branch index (= thread index), $\text{successful\_ops}(i)$ =
total CRUD operations completed by thread $i$.

### Exp 3b: Aggregate CRUD Goodput

$$\text{aggregate\_goodput}(N) = \sum_{i=1}^{N} \text{goodput}(i)$$

where $N$ = total branches (= threads). Linear scaling means
$\text{aggregate\_goodput}(N) \approx N \times \text{goodput}(1)$.

## Design

### Exp 3a: Branch Creation Throughput

| Parameter | Value |
|-----------|-------|
| Backends | Dolt, file_copy, Neon |
| Topologies | spine, bushy, fan_out |
| Threads (T) | 1, 2, 4, 8, 16, 32, 64, 128 (Neon: 1–8) |
| Duration | 30s per run |
| Workload | Each thread creates branches in a while-true loop |
| Metric | Total branches created / 30s |

T threads create branches concurrently — total branches created >> T.
Neon capped at 8 threads (max 8 branches per project).

### Exp 3b: CRUD Throughput Under Branching

| Parameter | Value |
|-----------|-------|
| Backends | Dolt, file_copy, Neon |
| Topologies | spine, bushy, fan_out |
| Branches = Threads (N) | 1, 2, 4, 8, 16, 32, 64, 128 (Neon: 1–8) |
| Workload per thread | While-true loop of random CRUD ops (READ, UPDATE, RANGE_READ, RANGE_UPDATE) |
| Duration | 30s per run |
| Assignment | Thread_i owns branch_i exclusively |
| Metric | Successful ops / 30s per thread |

N branches created upfront; N threads each own one branch.
Neon capped at 8 (max 8 branches per project, 1 branch = 1 thread).

## Procedure

### Exp 3a: Branch Creation

1. Initialize fresh database
2. Spawn T threads
3. Each thread creates branches in a while-true loop for 30s using topology's
   parent rule
4. Count total successful branch creations / 30s

### Exp 3b: CRUD Operations

1. Create N branches using topology
2. Spawn N threads, each connected to its own branch
3. Each thread executes random CRUD ops in a while-true loop for 30s
4. Collect per-thread successful ops / 30s

## What We Expect

**RQ1 — Branch creation:**
- **Dolt**: Should scale well (branch = pointer write, minimal contention)
- **file_copy**: Likely bottlenecked by catalog locks and checkpoints — each
  `CREATE DATABASE` forces two checkpoints server-wide. Spine worse than
  fan-out (growing parent directory)
- **Neon**: Measures **control plane throughput** (HTTP API + compute endpoint
  provisioning), not database performance. Branch creation is ~2–10s per call.
  API rate limit (700 req/min) unlikely to be hit at T≤8 due to provisioning
  latency. Not directly comparable to Dolt/file_copy (local operations).
  HTTP 429 retry logic required.

**RQ2 — CRUD throughput vs N:**
- **Dolt**: Aggregate scales ~linearly (branches share chunk store but don't
  contend on reads/writes to separate tree roots)
- **file_copy**: Linear at low N (each branch is an independent PG database).
  At high N: degradation from shared buffer pool pressure, checkpoint
  interference, OS-level I/O contention
- **Neon**: CRUD ops use SQL connections (psycopg2), not the API — no rate
  limit concern. Depends on pageserver contention and page reconstruction cost

**RQ3 — Topology effect on distribution:**
- **Spine**: skewed — later branches have larger databases → slower scans,
  heavier indexes. Performance degradation expected
- **Fan-out**: tight — all branches same size, evenly distributed throughput
- **Bushy**: intermediate — random parent, average depth O(log N)

## Key Plots

1. **Branch creation throughput vs threads** (Exp 3a) — one line per topology
   per backend
2. **Aggregate CRUD goodput vs N** (Exp 3b) — does it scale?
3. **Per-branch throughput distribution at max N** (Exp 3b) — box plot per
   topology (is it fair?)
4. **Per-branch throughput vs branch index** (Exp 3b) — scatter for spine
   (does later = slower?)

## Context

- Exp 1 (done): branch creation **storage** overhead → file_copy spine O(N²)
- Exp 2 (done): per-operation **storage** overhead → near-zero for all backends
- Exp 3 (this): **throughput** → does branching affect runtime performance?

Exp 1 & 2 measured storage cost. This experiment measures time/rate cost.
