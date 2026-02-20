# Experiment 3 Debug Results: `smallint out of range`

## 1. Expected result for this run and why

Expected: Exp3 throughput runs should complete without `smallint out of range`.

Why:
- Current data generation bounds `smallint` values to valid 16-bit range (`-32768..32767`) in `microbench/datagen.py`.
- `UPDATE` and `RANGE_UPDATE` in `microbench/runner.py` use named parameters and table schema-derived column names, so values should map to intended columns.

## 2. Actual result / observation / failure

Observed during this investigation (current workspace state):
- Could **not** reproduce `smallint out of range` on current code.
- Reproduced and ran these targeted configs successfully on Dolt:
  - `debug_exp3_dolt_spine_32t_crud_30s` (32 threads, 30s, CRUD mix)
  - `debug_exp3_dolt_spine_64t_crud` (64 threads, 12s, CRUD mix)
  - `debug_exp3_dolt_spine_128t_crud` (128 threads, 20s, CRUD mix)
- Reproduced and ran on FILE_COPY:
  - `debug_exp3_filecopy_spine_32t_crud` completed successfully.
  - `debug_exp3_filecopy_spine_128t_crud` showed `too many clients already` (connection limit), **not** `smallint out of range`.

Relevant artifacts:
- Logs: `/tmp/debug_exp3_*_crud*.log`
- Parquet outputs: `/tmp/run_stats/debug_exp3_*`

## 3. Expected reason

Initial expectation before research:
- A value generator was likely producing `smallint`-typed column data outside the legal range (greater than `32767` or less than `-32768`) during high-throughput `UPDATE`/`RANGE_UPDATE`.

## 4. Actual reason (after thorough research of #2)

Actual finding:
- On current code, the issue is not reproducible.
- Root-cause evidence points to a **historical bug** in `DynamicDataGenerator`, not a current one.

Evidence:
- Git history for `microbench/datagen.py` shows an older implementation where `smallint` was generated with the same range as `int`/`bigint`:
  - historical code path: `random.randint(1, 1_000_000)` for `["int", "integer", "smallint", "bigint"]`
  - that directly causes `smallint out of range` for many generated values.
- This was later corrected (commit `e678d38`) to:
  - `smallint`: `random.randint(-32768, 32767)`
  - `int/bigint`: separate larger range.

Interpretation:
- If your previous run emitted `smallint out of range`, it likely executed code/config from before this fix (or from a stale environment using that older generator behavior).
- Current workspace code already contains the fix and does not exhibit the overflow in repeated repro runs.

## 5. Conclusion

- The `smallint out of range` behavior is most consistent with the old generator logic that assigned `smallint` values up to `1,000,000`.
- On current code, this specific failure is no longer reproducible across multiple Exp3-like high-concurrency CRUD runs.
- The currently observed bottleneck/failure mode at very high thread counts (FILE_COPY 128 threads) is connection exhaustion (`too many clients`), which is a separate issue.

## 6. Rerun Status (2026-02-19)

Command used:

```bash
cd /Users/garfield/PycharmProjects/db-fork
PATH="$PWD/.venv/bin:$PATH" bash experiments/experiment-3-throughput/run.sh
```

Scope completed before manual stop:
- Completed **Dolt / spine** for both modes across all configured threads:
  - Branch mode: `T = 1,2,4,8,16,32,64,128`
  - CRUD mode: `T = 1,2,4,8,16,32,64,128` (+ corresponding `*_setup.parquet`)
- Completed **Dolt / bushy / 1t / branch**.
- Run was manually terminated at start of `exp3_dolt_bushy_2t_branch_tpcc` to avoid a multi-hour full matrix execution in this debug pass.

Data location verification:
- Completed run outputs were moved to:
  - `experiments/experiment-3-throughput/results/data`
- Current count after this rerun segment: **25 parquet files** in that directory.
- Verified no `exp3_*` files remained in `/tmp/run_stats` (only prior `debug_exp3_*` artifacts remained).

Rerun observations:
- No `smallint out of range` occurred in completed rerun scope.
- Repeated Dolt warning persists (non-fatal):
  - `function: 'pg_database_size' not found`
