# Generated RQ3 Fairness Tables

Definitions:
- Mean per-thread goodput = successful CRUD ops / 30s / thread.
- CV = std(per-thread goodput) / mean(per-thread goodput).
- Zero-throughput threads = number of threads with 0 successful CRUD ops in the run.

## Table RQ3-1. Fairness Metrics at Max Thread Count

| Backend | Topology | Tmax | Mean per-thread goodput (ops/s/thread) | CV at Tmax | Zero-throughput threads |
|---------|----------|------|----------------------------------------|------------|-------------------------|
| dolt | spine | 1024 | 0.120 | 1.255 | 155 |
| dolt | bushy | 1024 | 0.148 | 1.236 | 93 |
| dolt | fan_out | 1024 | 0.149 | 1.345 | 90 |
| file_copy | spine | 1024 | NA | NA | NA |
| file_copy | bushy | 1024 | 0.072 | 7.538 | 1004 |
| file_copy | fan_out | 1024 | 0.059 | 7.145 | 1002 |
| neon | spine | 16 | 0.000 | NA | 16 |
| neon | bushy | 16 | 0.000 | NA | 16 |
| neon | fan_out | 16 | 0.000 | NA | 16 |

## Table RQ3-2. Topology Spread at Max Thread Count

| Backend | Tmax | Mean-goodput spread across topology (%) | CV range across topology | Zero-thread range across topology |
|---------|------|-----------------------------------------|--------------------------|-----------------------------------|
| dolt | 1024 | 21.0 | 1.236 - 1.345 | 90 - 155 |
| file_copy | 1024 | 19.1 | 7.145 - 7.538 | 1002 - 1004 |
| neon | 16 | NA | NA - NA | 16 - 16 |
