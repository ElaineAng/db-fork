# Experiments

Benchmark experiments for database branching storage and latency across
Dolt, PostgreSQL CoW, Neon, and Xata.

| ID | Date | Description | Status | Details |
|----|------|-------------|--------|---------|
| 0 | 2026-02-01 | Branch creation storage scaling (spine topology) | Done | [README](experiment-0-2026-02-01/README.md), [report](experiment-0-2026-02-01/results/report.md) |
| 1 | 2026-02-08 | Branch creation storage overhead (varying topology) | Done | [README](experiment-1-2026-02-08/README.md) |
| 2 | 2026-02-08 | Update storage overhead (per-op deltas) | Done | [README](experiment-2-2026-02-08/README.md) |
| 3 | 2026-02-18 | Operation throughput under branching | Planning | [README](experiment-3-throughput/README.md), [plan](experiment-3-throughput/results/docs/PLAN.md) |

## Backend Branching Mechanisms

All backends use copy-on-write (CoW) or structural sharing for branching.
This is confirmed by official documentation for each backend (see sources
below). This is important context for interpreting storage measurements.

### Dolt — Content-addressed structural sharing

Dolt's storage engine uses content-addressed **Prolly trees** where every chunk
is identified by a hash of its contents. Branching creates a lightweight pointer
in the commit graph — no data is duplicated.

> "When you create a new branch, you create a new pointer, a small amount of
> metadata. [...] There is no additional object storage required unless a change
> is made on a branch."
> — [How Dolt Scales to Millions of Versions, Branches, and Rows][dolt-scale]

> "Any subtree where the roots have the same content-address are only stored
> once."
> — [Storage Engine | Dolt Documentation][dolt-storage]

[dolt-scale]: https://www.dolthub.com/blog/2025-05-16-millions-of-versions/
[dolt-storage]: https://docs.dolthub.com/architecture/storage-engine
[dolt-prolly]: https://docs.dolthub.com/architecture/storage-engine/prolly-tree

### Neon — Page-level CoW via WAL timeline branching

Neon implements CoW at the page/WAL level through its custom Pageserver. A new
branch (internally called a "timeline") points to an existing LSN and starts
with zero layer files — all reads fall through to the parent.

> "A branch is a copy-on-write clone of your data."
> — [Branching | Neon Docs][neon-branching]

> "When you create a branch in Neon, the engine does not duplicate files or
> pages. Instead, the new branch points to an existing point in history and
> begins diverging from there using copy-on-write semantics."
> — [Architecture Overview | Neon Docs][neon-arch]

> "If you have a database with 1 GB logical size and you create a branch of it,
> both branches will have 1 GB logical size, even though the branch is
> copy-on-write and won't consume any extra physical disk space until you make
> changes to it."
> — [Neon Glossary (GitHub)][neon-glossary]

[neon-branching]: https://neon.tech/docs/conceptual-guides/branching
[neon-arch]: https://neon.tech/docs/storage-engine/architecture-overview/
[neon-glossary]: https://github.com/neondatabase/neon/blob/main/docs/glossary.md

### Xata — Block-level CoW via Simplyblock distributed storage

Xata implements CoW at the block storage layer beneath unmodified PostgreSQL,
using Simplyblock distributed NVMe-oF storage. Branch creation copies only the
metadata index; data blocks are shared.

> "When you create a new branch, it creates a new metadata index that initially
> points to all the same data blocks as the parent. No actual data is copied,
> so it's instantaneous."
> — [PostgreSQL Branching: Xata vs Neon vs Supabase — Part 1][xata-part1]

> "Branching is implemented at the block storage layer, underneath Postgres.
> This means we don't fork or modify PostgreSQL."
> — [A database branch for every pull request][xata-branching]

[xata-branching]: https://xata.io/postgres-branching
[xata-part1]: https://xata.io/blog/neon-vs-supabase-vs-xata-postgres-branching-part-1

### PostgreSQL (file_copy) — Filesystem-level CoW (optional)

PostgreSQL's `CREATE DATABASE ... STRATEGY = FILE_COPY` copies the template
database's files. Whether this is a full copy or a CoW clone depends on the
`file_copy_method` setting (PostgreSQL 18+).

| Setting | System call (Linux) | System call (macOS) | Behavior |
|---------|--------------------|--------------------|----------|
| `copy` (default) | `read()`/`write()` loop | `read()`/`write()` loop | Full byte-by-byte copy — no CoW |
| `clone` | `FICLONE` ioctl (reflink) | `clonefile()` | CoW clone — shared physical blocks |

> — [PostgreSQL 18 Documentation: file_copy_method][pg-fcm]

[pg-fcm]: https://postgresqlco.nf/doc/en/param/file_copy_method/

### Fair comparison requires `file_copy_method = 'clone'`

Since Dolt, Neon, and Xata all use CoW internally, the PostgreSQL file_copy
backend must also use CoW (`file_copy_method = 'clone'`) for a fair comparison
of branching costs. Using the default `copy` setting would compare CoW backends
against a full-copy baseline — a valid but different experiment.

### `st_blocks` overcounting on CoW filesystems

On any CoW filesystem (APFS, XFS with reflinks, Btrfs), `os.stat().st_blocks`
reports the **logical** block count per inode, not the physically unique blocks.
A cloned file reports the same `st_blocks` as the original, even though the
clone consumes near-zero additional physical storage. This means
`get_directory_size_bytes()` (which sums `st_blocks * 512`) will **overcount**
storage when CoW clones are in use.

| Filesystem | Reflink/clone support | `st_blocks` accurate for clones? |
|---|---|---|
| ext4 | No | N/A — no clones possible, always accurate |
| XFS (`reflink=1`) | Yes | No — includes shared extents |
| Btrfs | Yes | No — includes shared extents |
| macOS APFS | Yes (`clonefile()`) | No — includes shared extents |

**Workaround**: use volume-level `df` / `shutil.disk_usage()` deltas instead of
per-file `st_blocks` when measuring CoW storage. This requires an isolated
volume/partition so that only PostgreSQL activity is captured. Alternatively,
on Linux use `FIEMAP` with `FIEMAP_EXTENT_SHARED` to identify shared extents,
or on macOS use `getattrlist()` with `ATTR_CMNEXT_PRIVATESIZE`.

## Storage Measurement Validity Assessment

Each backend's `get_total_storage_bytes()` implementation is assessed below for
whether it reports **true physical storage** (accounting for CoW deduplication)
or **logical size** (which overcounts shared data when summed across branches).

### Dolt — VALID

All branches share a single content-addressed chunk store in a single data
directory. Measuring that directory with `st_blocks * 512` gives true physical
storage with deduplication already accounted for — identical chunks across
branches are stored exactly once on disk.

> "The best way to see how big your database is for now is to just look at
> disk."
> — [Dolt maintainer, GitHub Issue #6624][dolt-6624]

**Caveat**: unreferenced chunks (garbage) can inflate the measurement. Running
`CALL dolt_gc()` before measuring gives accurate results.

> "After you have accumulated a lot of data and are running Dolt in
> production you may want to reclaim space. [...] We recommend only running
> garbage collection in offline mode."
> — [Sizing Your Dolt Instance][dolt-sizing]

[dolt-6624]: https://github.com/dolthub/dolt/issues/6624
[dolt-sizing]: https://www.dolthub.com/blog/2023-12-06-sizing-your-dolt-instance/

### PostgreSQL (file_copy) — INVALID with `file_copy_method = 'clone'`

Sums `st_blocks * 512` across each branch's `$PGDATA/base/<oid>/` directory.

- With `file_copy_method = 'copy'` (full byte copies): **valid** — each branch
  owns its blocks exclusively.
- With `file_copy_method = 'clone'` (CoW reflink): **invalid** — every cloned
  file reports the same `st_blocks` as the original, overcounting shared blocks.
  See [`st_blocks` overcounting on CoW filesystems](#st_blocks-overcounting-on-cow-filesystems)
  above.

**Fix**: use volume-level `shutil.disk_usage()` delta on an isolated
partition, so that only PostgreSQL activity is captured.

### Neon — INVALID

Sums `pg_database_size()` across all branches. This reports **logical** size per
branch and does not account for CoW page sharing. Official docs explicitly
confirm this overcounts:

> "If you have a database with 1 GB logical size and you create a branch of
> it, both branches will have 1 GB logical size, even though the branch is
> copy-on-write and won't consume any extra physical disk space until you
> make changes to it."
> — [Neon Glossary][neon-glossary]

The only CoW-aware alternative is the project-level
**`synthetic_storage_size`** from the Neon API
(`GET /projects/{project_id}`):

> "The synthetic size is designed to: Take into account the copy-on-write
> nature of the storage. For example, if you create a branch, it doesn't
> immediately add anything to the synthetic size."
> — [Neon synthetic-size.md][neon-synthetic]

However, **`synthetic_storage_size` is not usable for fine-grained
benchmarking** due to two limitations:

1. **Update lag**: metrics refresh approximately every 15 minutes, and can
   take up to 1 hour before they are reportable via the API.

   > "Neon updates all metrics every 15 minutes but it could take up to
   > 1 hour before they are reportable."
   > — [Neon Consumption Limits][neon-limits]

   > "Neon's consumption data is updated approximately every 15 minutes,
   > so a minimum interval of 15 minutes between API calls is recommended."
   > — [Neon Consumption Metrics][neon-consumption]

2. **Project-level only**: the metric cannot be attributed to individual
   branches.

   > "The synthetic size is calculated for the whole project. It is not
   > straightforward to attribute size to individual branches."
   > — [Neon synthetic-size.md][neon-synthetic]

This means there is currently **no Neon metric suitable for measuring
per-operation or per-branch storage deltas in real time**. The current
`pg_database_size()` approach provides real-time readings but overcounts due
to CoW. `synthetic_storage_size` is CoW-aware but far too slow for
per-operation measurement. Neon storage measurements should be interpreted
as **logical size only** in analysis.

[neon-synthetic]: https://github.com/neondatabase/neon/blob/main/docs/synthetic-size.md
[neon-limits]: https://neon.com/docs/guides/consumption-limits
[neon-consumption]: https://neon.com/docs/guides/consumption-metrics

### Xata — LIKELY INVALID

Sums the per-branch `disk` metric from the Xata metrics API. Each PostgreSQL
instance runs on its own volume and reports its full logical footprint. Given
Xata's CoW architecture, summing across branches overcounts shared blocks.

Official documentation does not specify whether the per-branch `disk` metric
is logical or physical. However, the billing model is explicitly physical at the
workspace level:

> "Branches use copy-on-write to share storage with the parent so you only
> pay for changes, not full copies."
> — [A database branch for every pull request][xata-branching]

Per-branch storage attribution is not documented in the Xata API reference.

**Fix**: use workspace-level "Data storage" from the Usage dashboard if an API
is available, or measure `pg_database_size()` as a logical-only baseline and
note the overcounting in analysis.

### Summary

| Backend | Method | What it measures | Measurement CoW-aware? | Valid? | Fix |
|---|---|---|---|---|---|
| **Dolt** | `st_blocks * 512` on shared data dir | Physical (deduplicated) | Yes — single chunk store | **Yes** | Run `dolt_gc()` before measuring |
| **file_copy** | `st_blocks * 512` per branch OID dir | Logical per-branch | No — `st_blocks` overcounts clones | **No** | Volume-level `shutil.disk_usage()` delta |
| **Neon** | `pg_database_size()` per branch | Logical per-branch | No — reports full logical size | **No** | No real-time fix; `synthetic_storage_size` lags 15–60 min |
| **Xata** | Metrics API `disk` per branch | Likely logical | No — likely full logical footprint | **Likely no** | Workspace-level storage metric (if available) |
