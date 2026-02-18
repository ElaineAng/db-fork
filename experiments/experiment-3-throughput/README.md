# Experiment 3: Operation Throughput Under Branching

**Date**: 2026-02-18
**Status**: Planning

## 1. Problem Statement

- Experiments 1 and 2 measured **storage** overhead of branching. They did not
  measure whether branching degrades **runtime performance**.
- In CoW systems, branches share data structures (chunk stores, buffer pools,
  page caches). Concurrent operations on multiple branches may contend for
  these shared resources.
- Branch creation itself may be a bottleneck: metadata contention (catalog
  locks, WAL, checkpoints) could prevent linear scaling of concurrent branch
  creation — even if single-threaded creation is fast.
- It is unknown whether topology affects throughput distribution across branches.

## 2. Objective

Quantify **operation throughput** as a function of branch count, concurrency,
and topology. Three research questions:

1. **Branch creation throughput**: How many branches can be created per second
   under concurrent threads? Does metadata contention limit scaling?
2. **CRUD throughput vs branch count**: Does per-branch CRUD throughput degrade
   as N grows?
3. **Topology effect on distribution**: Is throughput evenly distributed across
   branches, or does topology (spine vs fan-out) create unfairness?

## 3. Metrics

### Branch Creation Throughput (Exp 3a)

$$\text{branch\_throughput}(T) = \frac{\sum_{t=1}^{T} \text{branches\_created}(t)}{\Delta t}$$

where $T$ = number of concurrent threads, $\text{branches\_created}(t)$ =
successful branch creations by thread $t$, $\Delta t$ = 30s.

### Per-Branch CRUD Goodput (Exp 3b)

$$\text{goodput}(i) = \frac{\text{successful\_ops}(i)}{\Delta t}$$

where $i$ = branch index (= thread index), $\text{successful\_ops}(i)$ =
total CRUD operations completed by thread $i$.

### Aggregate CRUD Goodput (Exp 3b)

$$\text{aggregate\_goodput}(N) = \sum_{i=1}^{N} \text{goodput}(i)$$

where $N$ = total branches (= threads). Linear scaling means
$\text{aggregate\_goodput}(N) \approx N \times \text{goodput}(1)$.

## 4. Methodology

### 4.1 Independent Variables

| Variable | Exp 3a | Exp 3b |
|----------|--------|--------|
| Backend | Dolt, file_copy, Neon | Dolt, file_copy, Neon |
| Topology | SPINE, BUSHY, FAN_OUT | SPINE, BUSHY, FAN_OUT |
| Threads (T) | 1, 2, 4, 8, 16, 32, 64, 128 (Neon: 1–8) | — |
| Branches = Threads (N) | — | 1, 2, 4, 8, 16, 32, 64, 128 (Neon: 1–8) |

In Exp 3a, T threads create branches concurrently — total branches >> T.
In Exp 3b, N branches are created upfront; N threads each own one branch.
Neon capped at 8 in both because 1 branch requires 1 thread and max 8
branches per project.

Each combination is an independent run with a fresh database instance.

### 4.2 Fixed Parameters

| Parameter | Value |
|-----------|-------|
| Table schema | TPC-C `orders` |
| Duration | 30 seconds per run |
| Assignment | Thread_i owns branch_i exclusively (Exp 3b) |
| CRUD mix | Random ops: READ, UPDATE, RANGE_READ, RANGE_UPDATE |
| Autocommit | true |

### 4.3 Backend Configuration

| Backend | Branch method | Data directory | Isolation |
|---------|--------------|----------------|-----------|
| **Dolt** | `dolt_checkout('-b', branch_name)` — pointer write in content-addressed DAG | `${DOLT_DATA_DIR}/<db_name>/` | Single chunk store, all branches share |
| **file_copy** | `CREATE DATABASE ... STRATEGY = FILE_COPY` with `file_copy_method = 'clone'` — APFS `clonefile()` CoW | `/Volumes/PGBench/pgdata/` (isolated APFS volume) | Each branch = independent PG database on dedicated volume |
| **Neon** | API-managed timeline fork | Cloud (per-project isolation) | Max 8 branches; API rate-limited to ~11 req/sec |

**file_copy isolation**: Same setup as Experiments 1 & 2. PostgreSQL data
directory lives on a dedicated APFS sparse disk image (`~/pgbench.sparseimage`
mounted at `/Volumes/PGBench/`), created by `db_setup/setup_pg_volume.sh`.
This ensures `clonefile()` CoW semantics and isolates storage measurement
from OS activity. Each branch is a separate PostgreSQL database created via
`FILE_COPY` strategy, which iterates the parent's data directory calling
`clonefile()` per file.

### 4.4 Procedure

#### Exp 3a — Branch Creation Throughput

| Step | Action |
|------|--------|
| 1 | Initialize fresh database with TPC-C schema |
| 2 | Spawn T threads |
| 3 | Each thread creates branches in a while-true loop for 30s, using topology's parent rule |
| 4 | Record per-thread: `thread_id`, `successful_branch_count`, `elapsed_time` |
| 5 | Compute: `branch_creation_throughput = total_branches / 30s` |
| 6 | Cleanup |

**Concurrency model**: All threads share the same backend instance and a
thread-safe `SharedBranchManager`. Topology parent selection under concurrency:
- **Spine**: parent = globally last created branch (serialized via lock).
  Threads interleave but produce a single linear chain.
- **Fan-out**: parent = root (no contention on parent selection)
- **Bushy**: parent = random existing branch (lock-protected read)

Branch creation contends on:
- **Dolt**: chunk store writes (content-addressed, likely low contention)
- **file_copy**: PostgreSQL catalog locks, WAL writes, forced checkpoints
  (two per `CREATE DATABASE`), `clonefile()` syscalls on shared volume
- **Neon**: each `branch_create` is an **HTTP POST** that also provisions a
  compute endpoint (~2–10s per call). The API rate limit (700 req/min, burst
  40/sec) is unlikely to be hit at T≤8 due to provisioning latency, but
  retry logic for HTTP 429 is required. Note that Neon Exp 3a measures
  **control plane throughput** (API + endpoint provisioning), which is
  fundamentally different from Dolt/file_copy where branch creation is a
  local operation.

#### Exp 3b — CRUD Throughput Under Branching

| Step | Action |
|------|--------|
| 1 | Initialize fresh database with TPC-C schema |
| 2 | Create N branches upfront using topology (reuse `setup_nth_op_branches()`) |
| 3 | Populate each branch with workload (100 INSERTs + 20 UPDATEs + 10 DELETEs) |
| 4 | Spawn N threads, thread_i connects to branch_i |
| 5 | Each thread runs random CRUD ops in a while-true loop for 30s |
| 6 | Record per-thread: `thread_id`, `branch_index`, `successful_ops`, `elapsed_time` |
| 7 | Compute per-branch goodput = `successful_ops / elapsed_time` |
| 8 | Cleanup |

**Neon API note**: Branch creation (step 2) and initial connection (step 4)
use the Neon HTTP API, but are sequential during setup. The 30s measurement
phase (step 5) uses `psycopg2` SQL connections only — no API calls, no rate
limit concern.

**Per-thread output row**:

| Column | Description |
|--------|-------------|
| `thread_id` | Thread index (0..N-1) |
| `branch_index` | Which branch this thread operated on |
| `op_type` | Per-op: READ, UPDATE, etc. |
| `latency` | Per-operation wall-clock time |

Each individual operation is recorded in the parquet. Goodput
(`successful_ops / elapsed_time`) is computed in post-processing by
counting rows per `thread_id` and dividing by the 30s window.

## 5. Analysis

### Figure 3a: Branch Creation Throughput vs Threads

- **X-axis**: number of threads (1, 2, 4, ..., 128)
- **Y-axis**: branches created per second (aggregate)
- **Series**: backend × topology

**Goal**: identify metadata contention bottlenecks. Linear scaling = no
contention. Sub-linear = shared resource bottleneck.

### Figure 3b: Aggregate CRUD Goodput vs N

- **X-axis**: N (branches = threads: 1, 2, 4, ..., 128)
- **Y-axis**: aggregate goodput (sum of all per-branch ops/sec)
- **Series**: backend × topology

**Goal**: does aggregate throughput scale with N?

### Figure 3c: Per-Branch Throughput Distribution at Max N

- **Layout**: box plot or violin per topology, faceted by backend
- **Y-axis**: per-branch goodput (ops/sec)

**Goal**: is throughput fairly distributed? Spine expected to be skewed
(later branches = larger DBs = slower), fan-out expected to be tight.

### Figure 3d: Per-Branch Throughput vs Branch Index (Spine)

- **X-axis**: branch index (1, 2, ..., N)
- **Y-axis**: per-branch goodput (ops/sec)
- **Series**: backend

**Goal**: for spine topology, does later = slower? Confirms whether
accumulated data in spine parents causes measurable performance degradation.

## 6. Implementation Plan

### 6.1 What Exists (Reusable)

| Component | File | Status |
|-----------|------|--------|
| Multi-threading | `runner.py` (`ThreadPoolExecutor`, `SharedBranchManager`) | Ready |
| Branch topology | `runner.py` (`setup_nth_op_branches()`, SPINE/FAN_OUT/BUSHY) | Ready |
| Backend abstraction | `dblib/dolt.py`, `dblib/file_copy.py`, `dblib/neon.py` | Ready |
| Thread-safe result collection | `dblib/result_collector.py` (thread-local state, parquet) | Ready |
| APFS volume isolation | `db_setup/setup_pg_volume.sh` | Ready |
| Config system | Textproto-based, `bench_lib.sh` generates configs | Ready |

### 6.2 What Must Be Built

#### 1. Duration-based operation loop in `runner.py`

Current `nth_op_benchmark` runs a fixed number of operations. Need a new
mode that runs for a fixed wall-clock duration (30s):

```python
def throughput_benchmark(backend, config, result_collector, duration_sec=30):
    start = time.time()
    ops_count = 0
    while time.time() - start < duration_sec:
        execute_random_op(...)
        ops_count += 1
    return ops_count, time.time() - start
```

This applies to both Exp 3a (branch creation loop) and Exp 3b (CRUD loop).

#### 2. HTTP 429 retry in Neon branch creation

`neon.py:_create_branch_impl` currently has no retry on rate limit errors.
Add exponential backoff for HTTP 429 responses in `_request()` to handle
burst scenarios during concurrent Exp 3a runs:

```python
if r.status_code == 429:
    retry_after = float(r.headers.get("Retry-After", 1))
    time.sleep(retry_after)
    continue
```

#### 3. Goodput metric in result collection

Option A: Compute in post-processing from raw per-op parquet data
(count rows per thread_id / max(latency cumsum)).

Option B: Add summary row to parquet with `goodput_ops_per_sec` field.

**Recommendation**: Option A — no schema changes needed, just analysis.

#### 4. New shell script: `run_throughput_bench.sh`

Loop over thread counts instead of branch counts:

```bash
for SHAPE in spine bushy fan_out; do
    # Dolt, file_copy: up to 128 threads
    for BACKEND in dolt file_copy; do
        for T in 1 2 4 8 16 32 64 128; do
            run_throughput_bench $BACKEND $SHAPE $T 30 BRANCH
            run_throughput_bench $BACKEND $SHAPE $T 30 CRUD_MIX
        done
    done
    # Neon: capped at 8 threads (max 8 branches per project)
    for T in 1 2 4 8; do
        run_throughput_bench neon $SHAPE $T 30 BRANCH
        run_throughput_bench neon $SHAPE $T 30 CRUD_MIX
    done
done
```

#### 5. Config extensions

Add to textproto config:

| Field | Type | Description |
|-------|------|-------------|
| `duration_seconds` | int32 | Wall-clock time limit per run (replaces `num_ops`) |
| `num_threads` | int32 | Already exists in `runner.py`, expose in bench_lib |

#### 6. Analysis scripts

| Script | Purpose |
|--------|---------|
| `01_analyze.py` | Load parquets, compute per-thread goodput, aggregate by config |
| `02_plot.py` | Generate figures 3a–3d |

### 6.3 Prerequisites

- Dolt server running on port 5433 (`DOLT_PORT=5433`)
- PostgreSQL on isolated APFS volume (`db_setup/setup_pg_volume.sh`)
- Neon API key configured (max 8 branches)

### 6.4 Output Directory

All parquet files written to experiment-specific output dir:

| File pattern | Contents |
|-------------|----------|
| `{backend}_{shape}_{threads}_branch_throughput.parquet` | Exp 3a: per-op branch creation timing |
| `{backend}_{shape}_{threads}_crud_throughput.parquet` | Exp 3b: per-op CRUD timing |

### 6.5 Run Counts

**Exp 3a**: 8 thread counts × 3 topologies × 2 backends = **48 runs**
(+ Neon: 4 thread counts {1,2,4,8} × 3 topologies = 12 runs)

**Exp 3b**: 8 thread counts × 3 topologies × 2 backends = **48 runs**
(+ Neon: 4 thread counts {1,2,4,8} × 3 topologies = 12 runs)

**Total**: 120 runs × 30s = ~60 minutes (sequential)
