# Experiment 1: Branch Creation Storage Overhead

**Date**: 2026-02-09 (Dolt, file_copy), 2026-02-11 (Neon), 2026-02-25~26 (Xata)

## 0. Summary

| Property            | Dolt                    | file_copy                   | Neon                 | Xata                                  |
| ------------------- | ----------------------- | --------------------------- | -------------------- | ------------------------------------- |
| Measurement type    | Physical                | Physical                    | Logical              | Logical (metrics API)                 |
| Topology-sensitive? | No                      | **Yes** (spine 17x fan-out) | No                   | No at N=8 (sparse data at N≤4)       |
| Cost at max N       | ~685 B                  | 165 KB–2.74 MB             | ~7.3 MB              | 14.2–16.0 MB at N=16 (spine missing) |
| Branch mechanism    | Pointer in commit graph | Per-file CoW clone          | API-managed timeline | API-managed branching                 |

## 1. Experiment Procedure

One repetition (one full run) for a given backend, N, and topology:

1. Create fresh database from scratch
2. Add data to root branch (100 INSERTs + 20 UPDATEs + 10 DELETEs on TPC-C orders table)
3. For i = 1 to N:
   - Measure total storage → `disk_size_before`
   - Create branch_i from parent (depends on topology)
   - Measure total storage → `disk_size_after`
   - Record `latency`, `disk_size_before`, `disk_size_after` → one parquet row
   - Add data to branch_i (100 INSERTs + 20 UPDATEs + 10 DELETEs, not timed)

Each configuration (backend × topology × N) was run **3 repetitions** (Dolt, file_copy, Neon) or **2–6 repetitions** (Xata, variable due to retry/resume appending). Each repetition starts from a completely fresh database. All repetitions are appended to the same parquet file.

### Configurations


| Parameter         | Value                                                               |
| ----------------- | ------------------------------------------------------------------- |
| Backends          | Dolt, file_copy (PostgreSQL CoW), Neon, Xata                        |
| Branch counts (N) | 1–1024 (Dolt, file_copy); 1–8 (Neon); 1–16 (Xata, spine up to 8) |
| Data              | 92 setup parquet files, 37,245 setup rows                           |

### Storage Measurement


| Backend       | Method                                                     | Type                 | CoW-aware? |
| ------------- | ---------------------------------------------------------- | -------------------- | ---------- |
| **Dolt**      | `st_blocks * 512` on shared data directory                 | Physical             | Yes        |
| **file_copy** | `shutil.disk_usage()` on isolated APFS volume              | Physical             | Yes        |
| **Neon**      | `pg_database_size()` per branch, summed                    | Logical              | No         |
| **Xata**      | Branch Metrics API (`metric=disk`, max over 5 min), summed | Logical per instance | No         |

## 2. Metrics

**Marginal storage delta** is the change in total observed storage caused by creating one additional branch:

```
storage_delta = disk_size_after - disk_size_before
```

where `disk_size` is measured immediately before and after each branch creation call. Values reported in tables below are means across repetitions at each N.

## 3. Research Questions

### 3.1 RQ-to-Evidence Mapping

| RQ | Primary evidence | Role |
| -- | ---------------- | ---- |
| **RQ1**: Does the marginal storage cost of the nth branch differ across topologies for the same backend? | Table 4.1 + Figure 1 | Primary answer |
| **RQ2**: Do any backends exhibit constant marginal cost regardless of topology? | Table 4.1 + Figure 1 | Primary answer (qualitative) |
| **RQ3**: Does fan-out (shallow, wide) produce lower or higher overhead than spine (deep, narrow)? | Table 4.1 ratios at comparable N + Figure 1 | Primary answer |
| file_copy volatility context | Figure 2 | Supporting diagnostic (not standalone RQ evidence) |

### 3.2 RQ1 — Topology difference within backend

**RQ1: Does the marginal storage cost of the nth branch differ across topologies for the same backend?**

| Backend | Comparison point used | Observation | Answer |
| ------- | --------------------- | ----------- | ------ |
| **Dolt** | N=1024 | Spine 685 B, bushy 343 B, fan-out 11.33 KB | Minimal topology effect in absolute terms (near-zero baseline). |
| **file_copy** | N=1024 | Spine 2.74 MB, bushy 212 KB, fan-out 165 KB | **Yes**: strong topology sensitivity. |
| **Neon** | N=8 | Spine 7.35 MB, bushy 7.31 MB, fan-out 7.29 MB | No meaningful topology separation in this range. |
| **Xata** | **Common comparable N=8** | Spine 25.06 MB, bushy 24.89 MB, fan-out 22.64 MB | Similar at comparable N. Caveat: N=16 lacks spine coverage. |

### 3.3 RQ2 — Constant marginal cost across topology

**RQ2: Do any backends exhibit constant marginal cost regardless of topology?**

| Backend | Evidence at comparable N | Qualitative constancy judgment |
| ------- | ------------------------ | ------------------------------ |
| **Dolt** | N=1024: 343 B–11.33 KB across topologies | Approximately constant near zero (all topologies remain tiny). |
| **file_copy** | N=1024: 165 KB–2.74 MB across topologies | Not constant; spine diverges strongly from bushy/fan-out. |
| **Neon** | N=8: 7.29–7.35 MB across topologies | Approximately constant across topologies (logical metric). |
| **Xata** | **Common comparable N=8**: 22.64–25.06 MB | Roughly constant at comparable N; noisy and partially covered at N=16. |

### 3.4 RQ3 — Fan-out vs spine directionality

**RQ3: Does fan-out (shallow, wide) produce lower or higher overhead than spine (deep, narrow)?**

| Backend | Comparison N | Spine/Fan-out ratio | Direction |
| ------- | ------------ | ------------------- | --------- |
| **Dolt** | N=1024 | 0.06x | Spine is lower than fan-out at this point. |
| **file_copy** | N=1024 | 17.00x | Spine is much higher than fan-out. |
| **Neon** | N=8 | 1.01x | Spine is slightly higher than fan-out. |
| **Xata** | **Common comparable N=8** | 1.11x | Spine is higher than fan-out at N=8; N=16 lacks spine. |

Dolt is near-zero for both topologies; the 0.06x mean ratio is an outlier artifact in a quantized sparse-signal regime.
Therefore, we cannot conclude that for Dolt, Spine is cheaper than Fan-out.

## 4. Results

### 4.1 Marginal Storage Delta Tables

**Dolt** (physical, content-addressed):


| N    | Spine    | Bushy   | Fan-out  |
| ---- | -------- | ------- | -------- |
| 1    | 0 B      | 1.33 KB | 0 B      |
| 2    | 1.33 KB  | 1.33 KB | 683 B    |
| 4    | 0 B      | 1.00 KB | 0 B      |
| 8    | 171 B    | 341 B   | 683 B    |
| 16   | 85 B     | 1.42 KB | 0 B      |
| 32   | 10.71 KB | 128 B   | 10.71 KB |
| 64   | 5.67 KB  | 5.35 KB | 5.42 KB  |
| 128  | 42.83 KB | 21 B    | 45.34 KB |
| 256  | 4.00 KB  | 5 B     | 1.51 KB  |
| 512  | 11.33 KB | 683 B   | 22.00 KB |
| 1024 | 685 B    | 343 B   | 11.33 KB |

**file_copy** (physical, filesystem CoW):


| N    | Spine       | Bushy     | Fan-out   |
| ---- | ----------- | --------- | --------- |
| 1    | 152.00 KB   | 154.67 KB | 86.67 KB  |
| 2    | 152.00 KB   | 136.67 KB | 143.33 KB |
| 4    | 141.33 KB   | 128.33 KB | 126.00 KB |
| 8    | 171.17 KB   | 155.83 KB | 147.00 KB |
| 16   | 154.42 KB   | 157.92 KB | 130.25 KB |
| 32   | 157.33 KB   | 148.62 KB | 124.21 KB |
| 64   | 190.17 KB   | 145.54 KB | 122.12 KB |
| 128  | 264.98 KB   | 157.78 KB | 120.79 KB |
| 256  | 167.42 KB   | 162.17 KB | 120.44 KB |
| 512  | 623.78 KB   | 181.08 KB | 137.12 KB |
| 1024 | **2.74 MB** | 212.14 KB | 165.02 KB |

**Neon** (logical, `pg_database_size()`):


| N | Spine   | Bushy   | Fan-out |
| - | ------- | ------- | ------- |
| 1 | 7.29 MB | 7.29 MB | 7.29 MB |
| 2 | 7.30 MB | 7.29 MB | 7.29 MB |
| 4 | 7.32 MB | 7.30 MB | 7.29 MB |
| 8 | 7.35 MB | 7.31 MB | 7.29 MB |

**Xata** (logical, branch metrics API):


| N  | Spine    | Bushy    | Fan-out  |
| -- | -------- | -------- | -------- |
| 2  | 10.44 MB | 23.52 MB | 32.00 KB |
| 4  | 25.91 MB | 27.02 MB | 39.23 MB |
| 8  | 25.06 MB | 24.89 MB | 22.64 MB |
| 16 | —       | 15.95 MB | 14.20 MB |

At N=1024, file_copy spine is **17x** fan-out and **13x** bushy.

### 4.2 Per-Branch Delta Trajectory (Figure 1)

![Marginal Storage Delta per Branch](figures/fig1_marginal_storage_by_topology.png)
*Figure 1: Per-branch storage delta trajectory with one panel per backend
(Dolt/file_copy at N=1024, Neon at N=8, Xata at N=8 where all three topologies
are present). Each line is the mean across repetitions at each branch index;
shaded band = ±1 std.*

Figure 1 has one panel per backend. The y-axis is storage delta and the x-axis is branch index. Lines represent spine, bushy, and fan-out where data exists for that panel.

- **Dolt**: all three topologies cluster near zero across the full 1024-branch range. No visible separation between topologies.
- **file_copy**: bushy and fan-out stay flat near 120–165 KB. Spine diverges upward with large spikes, particularly in the second half of the range.
- **Neon**: all three lines overlap at ~7.3 MB with negligible separation.
- **Xata**: noisy with wide std bands; no clear topology separation at available N values.

### 4.3 file_copy Delta Volatility (Figure 2)

![file_copy Delta Volatility by Topology](figures/fig5_file_copy_delta_volatility.png)
*Figure 2: Individual storage deltas for file_copy at N=1024. Each branch
index has 3 data points (one per repetition), shown as scatter dots. The
solid line is a rolling mean (window=50), and the shaded band is ±1 rolling
standard deviation. All three repetitions are overlaid on the same axes.*

Figure 2 zooms into file_copy at N=1024, with one panel per topology. Unlike Figure 1 which shows means, this figure plots every individual measurement as a scatter dot to reveal per-branch variance.
It supports the RQ1/RQ3 interpretation for file_copy by showing how spine variance and spikes differ from bushy/fan-out.

- **Spine**: scatter spans -1.3 MB to +77 MB. The rolling mean trends upward but individual points are extremely spread. Negative deltas (storage decreasing after branch creation) appear throughout.
- **Bushy**: scatter is tightly clustered around the rolling mean (~150–200 KB). Very consistent across repetitions.
- **Fan-out**: tightest clustering of the three, with scatter staying in a narrow band around ~120 KB.

## 5. Notable Observations

- **Growth-with-N diagnostic (non-RQ)**: file_copy spine topology shows non-monotonic but upward-trending aggregate cost with large variance at high N (e.g., 152 KB at N=1, 265 KB at N=128, 167 KB at N=256, 624 KB at N=512, 2.74 MB at N=1024). Bushy and fan-out remain comparatively stable.
- **Neon logical measurement**: `pg_database_size()` reports logical size, not physical. It does not reflect CoW page sharing between branches. The constant ~7.3 MB per branch likely overstates physical cost.
- **Xata data filtering**: 58 of 261 Xata rows (~22%) were excluded because the metrics API returned zero for `disk_size_before` or `disk_size_after` (metrics lag at run start). This removes all N=1 data points.
- **Xata partial coverage**: Spine topology data only goes up to N=8, while bushy and fan-out reach N=16.
- **macOS only**: Storage measurements use APFS CoW semantics. Results may differ on Linux ext4/XFS.
- **No Dolt GC**: Unreferenced chunks were not garbage-collected, which may inflate Dolt measurements.

## 6. Hypotheses for Analysis

### 6.1 Section 4.2 — Per-Branch Delta Trajectory

#### 6.1.1 Dolt: Quantized non-zero deltas at N=1024

Across the entire Dolt dataset (18,426 rows, all N values and topologies), only
**6 distinct delta values** ever appear, and every non-zero value is an exact
power of 2:

| Delta | Hex | Occurrences | % of all rows |
|-------|-----|-------------|---------------|
| 0 B | — | 18,366 | 99.67% |
| 4 KB | 0x1000 | 28 | 0.15% |
| 16 KB | 0x4000 | 2 | 0.01% |
| 64 KB | 0x10000 | 5 | 0.03% |
| 1 MB | 0x100000 | 18 | 0.10% |
| 16 MB | 0x1000000 | 7 | 0.04% |

The means reported in Table 4.1 (e.g., 685 B for spine at N=1024) are artifacts
of averaging over ~99.9% zeros and a handful of power-of-2 jumps.

**Observed patterns in the raw data**:

- **4 KB jumps occur exclusively at iteration 0** (the first branch creation in
  a run). All 28 occurrences are at iter=0. This suggests a one-time metadata
  file allocation when the first branch is created.

- **1 MB and 16 MB jumps correlate with total database size**, not branch count.
  Examples: 1 MB jumps appear when `disk_size_before` is ~10 MB, ~20 MB, ~27 MB,
  ~60 MB, etc. The 16 MB jumps appear at ~67 MB, ~84 MB, ~199 MB, ~249 MB,
  ~364 MB, ~891 MB.

**Interpretation**:

1. **Branch creation is a pointer operation** (documented): Creating a branch
   adds a small commit object and branch ref to Dolt's content-addressed chunk
   store. The actual metadata is tens of bytes. No data chunks are duplicated.

2. **99.67% zero-cost because data operations drive file growth**: Between branch
   creations, the benchmark writes 100 INSERTs + 20 UPDATEs + 10 DELETEs. These
   data operations create new content-addressed chunks that grow the on-disk
   storage. The branch creation itself adds only a tiny pointer chunk that almost
   never triggers a storage change — hence 0 B delta in 99.67% of cases.

3. **The 4 KB iter=0 pattern**: The first branch creation in a fresh database
   consistently triggers a one-time 4 KB allocation (28/28 occurrences at
   iter=0). This likely reflects creation of a new metadata structure (e.g., a
   branch manifest or ref log) that requires one 4 KB filesystem block.

**Hypothesis — power-of-2 pre-allocation in the chunk store**:

The power-of-2 quantization (4 KB, 16 KB, 64 KB, 1 MB, 16 MB) and the
correlation between jump size and total database size are consistent with an
exponential pre-allocation strategy in Dolt's storage layer (e.g., table files
doubling their allocation granularity as they grow). However, this is inferred
from the delta pattern alone — the internal NBS pre-allocation logic has not been
verified in Dolt's source code. Alternative explanations (e.g., APFS sparse file
behavior, chunk journal compaction) could also produce power-of-2 jumps.

#### 6.1.2 Neon: Topology-dependent deltas despite logical measurement

At N=8, the mean marginal delta shows a small but consistent topology gradient:
spine 7.35 MB > bushy 7.31 MB > fan-out 7.29 MB (~60 KB spread).

**Observed in the raw data**:

The deltas are quantized to PostgreSQL's 8 KB page size with low variance
(most branch indices produce identical values across 3 repetitions; some show
±1 page jitter, e.g., spine branch 5–6 and fan-out branch 3–8):

| Branch | Spine (pages) | Fan-out (pages) |
|--------|---------------|-----------------|
| 1 | 933 | 933 |
| 2 | 937 | 933 |
| 3 | 938 | 934 |
| 4 | 939 | 934 |
| 5 | 942–943 | 934 |
| 6 | 943–944 | 934 |
| 7 | 944 | 934 |
| 8 | 945 | 934 |

Spine grows from 933 to 945 pages (+12 pages = +96 KB over 7 branches); fan-out
is nearly constant at 933–934 pages. Bushy falls between them (933–939 pages).

**Interpretation**:

The measurement sums `pg_database_size(current_database())` across all Neon
branches. When a new branch is created from a parent, the new branch's
`pg_database_size()` equals the parent's logical size at branching time. The
marginal delta is therefore approximately equal to the parent's logical size.

- **Fan-out** — Every branch is created from `main`. After initial data loading,
  `main` receives no further writes, so its `pg_database_size()` is constant
  (933–934 pages). Every branch creation adds the same delta ≈ 7.29 MB.

- **Spine** — Branch_i is created from branch_{i-1}. Before branching,
  branch_{i-1} has had 100 INSERTs + 20 UPDATEs + 10 DELETEs performed on it.
  These operations cause cumulative logical growth of the parent database:
  +90 net rows from INSERTs/DELETEs expand heap pages, and +30 dead tuples from
  UPDATEs/DELETEs occupy space until vacuumed. Each successive spine parent is
  slightly larger. By branch_8, the parent has grown by 12 pages (~96 KB) above
  the baseline.

- **Bushy** — The parent is randomly selected, so its logical size depends on how
  many operations it and its ancestors accumulated.

The topology gradient is real and near-deterministic — it reflects progressive
logical growth of parent databases through cumulative data operations along the
branching chain — but it does **not** indicate any difference in physical storage cost,
since Neon uses CoW page sharing and `pg_database_size()` cannot observe it.

### 6.2 Section 4.3 — file_copy Delta Volatility

#### 6.2.1 Bushy and fan-out: Low, stable storage deltas (~120–200 KB)

Fan-out shows the tightest clustering (mode 112 KB, median 154 KB, mean 165 KB);
bushy is higher and wider (mode 240 KB, median 264 KB, mean 212 KB).

**Hypothesis — fixed `clonefile()` metadata cost, modulated by parent
divergence**:

1. **`clonefile()` metadata is the dominant cost**: When PostgreSQL executes
   `CREATE DATABASE ... STRATEGY = FILE_COPY` with `file_copy_method = 'clone'`,
   it calls `clonefile()` for each file in the template database's
   `$PGDATA/base/<oid>/` directory. Each `clonefile()` creates a new APFS inode
   and directory entry with extent references pointing to shared data extents —
   no data blocks are physically copied. The total metadata cost per clone
   depends on the number of files in the database directory and the complexity
   of each file's extent tree. The consistent ~112–120 KB fan-out baseline
   suggests this metadata overhead is roughly fixed for a given database.

2. **Fan-out stability — identical source every time**: Every fan-out branch
   clones `main`, which is never written to after initial data loading. `main`'s
   files have a fixed extent layout. Each `clonefile()` produces identical
   metadata structures, resulting in the tightest clustering (most common delta:
   112 KB at 10.3% of rows).

3. **Bushy is slightly higher — plausibly from parent divergence**: In bushy
   topology, the parent is randomly selected from existing branches. Each parent
   has had 100 INSERTs + 20 UPDATEs + 10 DELETEs performed on it, which may
   cause CoW divergence at the APFS block level. A parent with more modified
   pages may have a more complex extent tree, slightly increasing the per-clone
   metadata cost. This is consistent with bushy's higher center (median 264 KB, IQR 63–340 KB) and
   wider spread, but the causal link between extent complexity and clone metadata
   cost has not been directly measured.

4. **4 KB-aligned but not quantized to discrete levels**: Every file_copy delta
   across all topologies is exactly a multiple of 4 KB (confirmed: 0 non-aligned
   values in 9,216 rows across all three N=1024 files). This reflects APFS's 4 KB
   block allocation granularity — `shutil.disk_usage().used` can only change in
   4 KB steps. However, unlike Dolt (which produces only 6 distinct power-of-2
   values), file_copy deltas span a **continuous range** of 4 KB multiples: 206
   unique values for fan-out and 239 for bushy at N=1024. The most common fan-out
   delta is 112 KB (28 blocks, 10.3% of rows), with a smooth distribution around
   it. This continuous spread reflects varying `clonefile()` metadata costs as
   APFS allocates different numbers of metadata blocks per clone depending on
   concurrent filesystem state.

5. **Negative deltas appear across all topologies**: At N=1024, negative deltas
   occur in fan-out (7.8% of rows, min -408 KB), bushy (22.9%, min -396 KB),
   and spine (13.7%, min -1.3 MB). This is consistent with PostgreSQL background
   processes (autovacuum, WAL recycling, background writer) routinely freeing
   more storage than the ~120 KB `clonefile()` metadata cost, producing
   net-negative measurements.
   Bushy has the highest negative-delta rate, possibly because random parent
   selection causes more autovacuum activity across many databases. Spine has
   fewer negative deltas than bushy but much larger negative magnitudes (up to
   -1.3 MB), consistent with WAL segment recycling events.

#### 6.2.2 Spine: High variance and negative storage deltas

Spine at N=1024 shows individual deltas spanning -1.3 MB to +77 MB, with the
rolling mean trending upward from ~150 KB to several MB. Negative deltas (storage
*decreasing* after branch creation) appear throughout.

**Observed negative delta magnitudes** (spine N=1024):

| Percentile | Magnitude |
|------------|-----------|
| P25 | 247 KB |
| P50 (median) | 364 KB |
| P75 | 629 KB |
| P90 | 948 KB |
| P95 | 1,044 KB |
| max | 1,308 KB |

Most spine negative deltas (median 364 KB, P90 948 KB) are well below the 16 MB
WAL segment size. Only 27 of 420 negative deltas exceed 1 MB.

**Hypothesis — PostgreSQL background processes on the shared measurement volume
dominate the variance**:

The benchmark measures storage via `shutil.disk_usage().used` on the isolated
APFS volume where `PGDATA` resides. This captures **all** physical changes on the
volume between the `before` and `after` calls — not just the `clonefile()` cost,
but also any concurrent PostgreSQL background I/O. No PostgreSQL tuning was
applied (default `shared_buffers`, `checkpoint_timeout`, `wal_segment_size`,
`autovacuum`). Note: in all topologies, the benchmark writes data (100 I + 20 U +
10 D) to each branch immediately after creation, so dirty buffers accumulate
across all topologies, not just spine.

1. **Checkpoint-induced spikes** (plausible, not decomposed): PostgreSQL's
   `CREATE DATABASE` forces **two server-wide checkpoints** (one before file copy
   to ensure source consistency, one after to persist the new database). Each
   checkpoint flushes all dirty `shared_buffers` pages to disk across every
   database on the server. At high N, hundreds of databases exist, each with
   dirty pages from recent writes. The forced checkpoint flush is captured by the
   volume-level `disk_usage()` measurement, and its magnitude varies depending on
   how many pages are dirty — a plausible source of the high positive variance.

   However, the same forced checkpoints fire in bushy and fan-out too, since
   those topologies also write to branches after creation. The reason spine is
   more affected is not fully identified — possible contributors include the
   sequential write→branch→write pattern and growing cumulative WAL/dirty page
   volume along the chain.

2. **WAL segment events** (possible contributor, not primary): PostgreSQL WAL
   segments default to 16 MB. WAL files reside on the same APFS volume. WAL
   allocation (+16 MB) or recycling (-16 MB) between measurements could explain
   some of the largest positive spikes. However, this does not explain the
   majority of negative deltas, which are much smaller (median 364 KB, P90
   948 KB). Smaller negative deltas are more consistent with autovacuum
   truncation or other sub-MB background reclamation events.

3. **Negative deltas — across all topologies, largest in spine**: Negative deltas
   occur in all topologies (fan-out 7.8%, bushy 22.9%, spine 13.7%). A negative
   measurement means background processes freed more than the ~120 KB
   `clonefile()` metadata cost between the two measurements. Possible mechanisms
   include autovacuum truncating trailing empty pages, checkpoint completion
   allowing WAL segment removal, or background writer writeback changing volume
   accounting. The exact causal decomposition cannot be determined from the
   volume-level measurement alone.

   What distinguishes spine is the *magnitude*: spine negatives reach -1.3 MB
   while bushy and fan-out cap at ~-400 KB. Bushy's higher negative-delta
   *rate* (22.9% vs spine 13.7%) is not fully explained — one possibility is
   that random parent selection triggers more autovacuum across many databases,
   but this has not been verified.

4. **Upward trend of the rolling mean with branch index** (observed, causes
   plausible but not decomposed): As more databases accumulate during a spine
   run, the rolling mean trends upward. Plausible contributing factors:
   - **More dirty pages per forced checkpoint**: Each of N databases may
     contribute unflushed pages, and the total flush size grows with N.
   - **Higher WAL generation rate**: More databases → more total write volume →
     more WAL segments in flight.
   - **`shared_buffers` pressure**: With 128 MB default and hundreds of
     databases, buffer pool contention increases.
   - **Growing parent file complexity**: In spine, branch_N clones branch_{N-1},
     whose heap files have grown from INSERTs (new pages allocated via CoW).

   The relative contribution of each factor cannot be determined from the
   volume-level measurement. A controlled experiment isolating one variable at a
   time (e.g., disabling autovacuum, pre-checkpointing, or using per-database
   rather than volume-level measurement) would be needed.

5. **Why spine diverges from bushy/fan-out** (directionally supported, not fully
   explained): Spine is the only topology where every branch creation immediately
   follows writes on the immediately preceding branch, creating a tight
   `write → checkpoint → clone → write → checkpoint → clone` cycle. In fan-out,
   `main` is unchanged after initial setup, so the forced checkpoints encounter
   fewer recently-dirtied pages from the clone source. In bushy, writes are
   spread across many branches. However, the data shows that even fan-out and
   bushy accumulate dirty buffers (since all topologies write to branches after
   creation), so the spine divergence may be partly explained by other factors
   such as cumulative WAL growth along the chain or increasing `CREATE DATABASE`
   duration at high N allowing more background I/O to occur within the
   measurement window.
