# Generated Tables (Exp3)

Definitions used by this generator:
- Branch tables (RQ1): `successful BRANCH_CREATE rows / 30s`.
- CRUD tables (RQ2/RQ3): `successful CRUD rows / 30s`.
- `T1 throughput`: throughput at thread count `T=1`.
- `Max-thread throughput`: throughput at backend-specific maximum T.
  (From manifest/data: `dolt=T1024`, `file_copy=T1024`, `neon=T16`).

## Table 1. Matrix Coverage

| Backend | Expected points | Found points | Missing points |
|---------|-----------------|--------------|----------------|
| dolt | 66 | 66 | 0 |
| file_copy | 66 | 65 | 1 |
| neon | 30 | 30 | 0 |
| TOTAL | 162 | 161 | 1 |

## Table 2. Branch Throughput Summary by Backend

| Backend | T1 branch-create throughput (ops/s, min-max over topology) | Peak branch-create throughput (ops/s) | Max-thread branch-create throughput (ops/s, min-max over topology) | Max-thread definition |
|---------|------------------------------------------------------------|---------------------------------------|--------------------------------------------------------------------|-----------------------|
| dolt | 58.07 - 60.27 | 79.03 (bushy, T=4) | 2.47 - 3.50 | T=1024 |
| file_copy | 23.53 - 24.73 | 24.73 (bushy, T=1) | 0.00 - 0.00 | T=1024 |
| neon | 0.00 - 0.00 | 0.00 (spine, T=1) | 0.00 - 0.00 | T=16 |

## Table 3. Branch Throughput Detailed (Backend x Topology)

| Backend | Topology | T1 branch-create throughput (ops/s) | Peak branch-create throughput | Max-thread branch-create throughput | Max/T1 |
|---------|----------|-------------------------------------|-------------------------------|-------------------------------------|--------|
| dolt | spine | 58.07 | 77.10 (T=4) | 2.47 (T=1024) | 0.042 |
| dolt | bushy | 59.37 | 79.03 (T=4) | 3.50 (T=1024) | 0.059 |
| dolt | fan_out | 60.27 | 78.47 (T=4) | 2.73 (T=1024) | 0.045 |
| file_copy | spine | 24.23 | 24.23 (T=1) | 0.00 (T=1024) | 0.000 |
| file_copy | bushy | 24.73 | 24.73 (T=1) | 0.00 (T=1024) | 0.000 |
| file_copy | fan_out | 23.53 | 23.53 (T=1) | 0.00 (T=1024) | 0.000 |
| neon | spine | 0.00 | 0.00 (T=1) | 0.00 (T=16) | NA |
| neon | bushy | 0.00 | 0.00 (T=1) | 0.00 (T=16) | NA |
| neon | fan_out | 0.00 | 0.00 (T=1) | 0.00 (T=16) | NA |

## Table 4. CRUD Aggregate Throughput Detailed (Backend x Topology)

| Backend | Topology | T1 aggregate CRUD throughput (ops/s) | Peak aggregate throughput | Max-thread aggregate throughput | Max/T1 |
|---------|----------|--------------------------------------|---------------------------|---------------------------------|--------|
| dolt | spine | 148.77 | 498.10 (T=16) | 122.77 (T=1024) | 0.825 |
| dolt | bushy | 152.57 | 503.67 (T=16) | 151.17 (T=1024) | 0.991 |
| dolt | fan_out | 147.13 | 465.63 (T=8) | 152.67 (T=1024) | 1.038 |
| file_copy | spine | 2650.40 | 6745.10 (T=4) | NA (T=1024) | NA |
| file_copy | bushy | 2717.30 | 7021.33 (T=4) | 73.37 (T=1024) | 0.027 |
| file_copy | fan_out | 2686.47 | 6706.90 (T=16) | 60.60 (T=1024) | 0.023 |
| neon | spine | 37.80 | 292.83 (T=8) | 0.00 (T=16) | 0.000 |
| neon | bushy | 34.93 | 265.67 (T=8) | 0.00 (T=16) | 0.000 |
| neon | fan_out | 39.60 | 290.20 (T=8) | 0.00 (T=16) | 0.000 |

## Table 5. CRUD Per-thread Degradation (T1 -> Tmax)

| Backend | Topology | T1 per-thread goodput (ops/s/thread) | Max-thread per-thread goodput | Per-thread degradation T1->Tmax | Zero-throughput threads at Tmax |
|---------|----------|--------------------------------------|-------------------------------|---------------------------------|---------------------------------|
| dolt | spine | 148.77 | 0.12 | 99.92% | 155 |
| dolt | bushy | 152.57 | 0.15 | 99.90% | 93 |
| dolt | fan_out | 147.13 | 0.15 | 99.90% | 90 |
| file_copy | spine | 2650.40 | NA | NA | NA |
| file_copy | bushy | 2717.30 | 0.07 | 100.00% | 1004 |
| file_copy | fan_out | 2686.47 | 0.06 | 100.00% | 1002 |
| neon | spine | 37.80 | 0.00 | 100.00% | 16 |
| neon | bushy | 34.93 | 0.00 | 100.00% | 16 |
| neon | fan_out | 39.60 | 0.00 | 100.00% | 16 |

## Table 6. Fairness at Max Thread Count (CRUD)

| Backend | Topology | Tmax | Mean per-thread goodput (ops/s/thread) | CV at Tmax | Zero-throughput threads |
|---------|----------|------|----------------------------------------|------------|-------------------------|
| dolt | spine | 1024 | 0.120 | 1.255 | 155 |
| dolt | bushy | 1024 | 0.148 | 1.236 | 93 |
| dolt | fan_out | 1024 | 0.149 | 1.345 | 90 |
| file_copy | spine | 1024 | NA | NA | NA |
| file_copy | bushy | 1024 | 0.072 | 7.538 | 1004 |
| file_copy | fan_out | 1024 | 0.059 | 7.145 | 1002 |
| neon | spine | 16 | 0.000 | 0.000 | 16 |
| neon | bushy | 16 | 0.000 | 0.000 | 16 |
| neon | fan_out | 16 | 0.000 | 0.000 | 16 |

## Table 7. Failure Summary by Backend

| Backend | Attempted ops | Successful ops | Failed ops | Failed exception ops | Failed slow ops | Success rate | Top failure category |
|---------|---------------|----------------|------------|----------------------|-----------------|--------------|----------------------|
| dolt | 532,451 | 392,700 | 139,751 | 1,734 | 138,017 | 73.75% | FAILURE_TIMEOUT (138017) |
| file_copy | 4,252,980 | 4,217,724 | 35,256 | 8,960 | 26,296 | 99.17% | FAILURE_TIMEOUT (26296) |
| neon | 50,329 | 48,060 | 2,269 | 1,981 | 288 | 95.49% | FAILURE_BACKEND_STATE_CONFLICT (1981) |

