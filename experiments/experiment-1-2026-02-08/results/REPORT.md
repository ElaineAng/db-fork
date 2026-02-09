# Experiment 1: Branch Creation Storage Overhead (Neon)

**Date**: 2026-02-09

## Objective

Measure how branch tree topology affects the marginal storage cost of branch
creation on Neon, with branch counts 1, 2, 4, and 8.

## Data

| Shape   | Branch counts | Iterations |
|---------|---------------|------------|
| spine   | 1, 2, 4, 8   | 2          |
| bushy   | 1, 2, 4, 8   | 3          |
| fan_out | 1, 2, 4, 8   | 3          |

Marginal cost = `disk_size_after - disk_size_before` per branch creation,
averaged across iterations.

## Results

| N | Spine | Bushy | Fan-out |
|---|-------|-------|---------|
| 1 | 7,643,136 | 7,643,136 | 7,643,136 |
| 2 | 7,659,520 | 7,643,136 | 7,643,136 |
| 4 | 7,673,856 | 7,648,597 | 7,643,136 |
| 8 | 7,701,504 | 7,662,251 | 7,643,136 |

All values in bytes.

![Marginal Storage Delta — Neon](figures/fig2_storage_delta_neon.png)

## Why These Results

Neon branches via copy-on-write page references. When a branch is created, Neon
records a new timeline that shares all existing pages with its parent. The
reported `pg_database_size()` reflects the logical size of the branch, not the
physical bytes added to shared storage.

- **Fan-out** (all branches from root): every branch forks from the same
  unmodified root. The root never accumulates mutations, so each branch sees
  the same logical size — the delta is constant at ~7.64 MB.

- **Spine** (each branch from the previous one): after creating branch _k_,
  the benchmark writes 100 INSERTs + 20 UPDATEs + 10 DELETEs into branch _k_
  before forking branch _k+1_ from it. Each successive parent is slightly
  larger because it carries the accumulated writes of all prior links in the
  chain. At N=8, the 8th branch forks from a parent that has absorbed 7 rounds
  of mutations, making its logical size ~58 KB larger than the root — hence
  the ~0.8% growth in marginal delta.

- **Bushy** (random parent): on average the selected parent has fewer
  accumulated mutations than spine's always-latest parent, so bushy falls
  between fan-out and spine but closer to fan-out.

The effect is small at N=8 because Neon's branch limit prevents testing at
higher counts. The file_copy backend (not shown) confirms the same pattern
extends to 1024 branches, where spine's marginal cost is 57% higher than
fan-out's.

## Limitations

- Capped at 8 branches (Neon platform limit).
- Spine data has only 2 iterations (vs 3 for bushy/fan_out).
- `pg_database_size()` reports logical size, not physical storage consumed
  on Neon's shared pageserver — the actual deduplication benefit of CoW is
  not captured by this metric.
