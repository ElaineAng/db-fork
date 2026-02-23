# Experiment 3 Results (2026-02-22 Extended Matrix)

## 1. Scope

This is the latest consolidated Exp3 result set after extending thread counts.

Primary sources:
- `/Users/garfield/PycharmProjects/db-fork/experiments/experiment-3-throughput/results/data/*_summary.json`
- `/Users/garfield/PycharmProjects/db-fork/experiments/experiment-3-throughput/results/data/*.parquet`
- `/Users/garfield/PycharmProjects/db-fork/experiments/experiment-3-throughput/results/run_manifest.csv` (regenerated from summary artifacts)

Run-integrity note:
- An intermediate Dolt high-thread pass initially used the wrong port due to `.env` override behavior. After fixing env precedence in `/Users/garfield/PycharmProjects/db-fork/bench_lib.sh`, Dolt high-thread points were rerun and the manifest was regenerated from data artifacts. The values below reflect only that corrected final state.

Matrix dimensions:
- Topology: `spine`, `bushy`, `fan_out`
- Mode: `branch`, `crud`
- Threads:
  - Dolt/file_copy: `1,2,4,8,16,32,64,128,256,512,1024`
  - Neon: `1,2,4,8,16`

## 2. Completeness and Status

Canonical manifest rows: `162`
- Dolt: `66`
- file_copy: `66`
- Neon: `30`

Coverage:
- Missing expected points: `0` for all backends.
- One catastrophic point failure was recorded as `FAILED`:
  - `exp3_file_copy_spine_1024t_crud_tpcc`
  - reason: `Runner timed out after 900s`
  - summary exists, main parquet is missing (expected for timeout termination).

Status counts:
- Dolt: `20 SUCCESS`, `46 PARTIAL`, `0 FAILED`
- file_copy: `17 SUCCESS`, `48 PARTIAL`, `1 FAILED`
- Neon: `2 SUCCESS`, `28 PARTIAL`, `0 FAILED`

## 3. Throughput Summary

Throughput shown as `successful_ops / 30s`.

### 3.1 Branch Throughput (Exp3a)

| backend | shape | T1 ops/s | Peak (T, ops/s) | Tmax ops/s |
|---|---|---:|---:|---:|
| dolt | spine | 116.13 | 4, 154.20 | 105.00 (T=1024) |
| dolt | bushy | 118.73 | 8, 159.10 | 104.40 (T=1024) |
| dolt | fan_out | 120.53 | 4, 156.93 | 106.20 (T=1024) |
| file_copy | spine | 48.80 | 1, 48.80 | 0.00 (T=1024) |
| file_copy | bushy | 49.83 | 1, 49.83 | 0.00 (T=1024) |
| file_copy | fan_out | 47.53 | 1, 47.53 | 0.00 (T=1024) |
| neon | spine | 0.00 | 1, 0.00 | 0.00 (T=16) |
| neon | bushy | 0.00 | 1, 0.00 | 0.00 (T=16) |
| neon | fan_out | 0.00 | 1, 0.00 | 0.00 (T=16) |

### 3.2 CRUD Aggregate Throughput (Exp3b)

| backend | shape | T1 ops/s | Peak (T, ops/s) | Tmax ops/s |
|---|---|---:|---:|---:|
| dolt | spine | 148.77 | 16, 498.10 | 122.77 (T=1024) |
| dolt | bushy | 152.57 | 16, 503.67 | 151.17 (T=1024) |
| dolt | fan_out | 147.13 | 8, 465.63 | 152.67 (T=1024) |
| file_copy | spine | 2650.40 | 4, 6745.10 | 0.00 (T=1024) |
| file_copy | bushy | 2717.30 | 4, 7021.33 | 73.37 (T=1024) |
| file_copy | fan_out | 2686.47 | 16, 6706.90 | 60.60 (T=1024) |
| neon | spine | 37.80 | 8, 292.83 | 0.00 (T=16) |
| neon | bushy | 34.93 | 8, 265.67 | 0.00 (T=16) |
| neon | fan_out | 39.60 | 8, 290.20 | 0.00 (T=16) |

## 4. Failure Summary (Why)

Global operation totals:
- Attempted: `4,835,760`
- Successful: `4,658,484`
- Failed (exception): `12,675`
- Failed (slow-threshold): `164,601`
- Global success rate: `96.33%`

Backend success rates:
- Dolt: `392,700 / 532,451 = 73.75%`
- file_copy: `4,217,724 / 4,252,980 = 99.17%`
- Neon: `48,060 / 50,329 = 95.49%`

Top failure categories by backend (point-level):
- Dolt: `FAILURE_TIMEOUT` (46 points)
- file_copy: `FAILURE_BACKEND_STATE_CONFLICT` (30), `FAILURE_TIMEOUT` (19)
- Neon: `FAILURE_BACKEND_STATE_CONFLICT` (18), `FAILURE_TIMEOUT` (10)

Most important concrete reasons observed:
- file_copy branch mode: repeated `CREATE DATABASE ... TEMPLATE ...` conflicts (`source database ... is being accessed by other users`).
- file_copy high-thread CRUD: severe long-tail/timeout behavior; one point (`spine_1024_crud`) required hard timeout (`900s`).
- Neon branch mode at `T=16`: branch-limit failures (`BRANCHES_LIMIT_EXCEEDED`) dominate branch attempts.

## 5. Takeaways

- The extended matrix is complete at point level (`162/162`) with one explicit timeout failure recorded.
- Concurrency degradation is clear at high thread counts:
  - Dolt and file_copy both show high-T instability in CRUD success throughput.
  - file_copy branch creation remains non-viable beyond single-thread in this setup.
  - Neon branch throughput is constrained by project branch limits, not local DB engine throughput.
- Captured summary artifacts now preserve both partial-failure and catastrophic-timeout reasoning for post-hoc analysis.
