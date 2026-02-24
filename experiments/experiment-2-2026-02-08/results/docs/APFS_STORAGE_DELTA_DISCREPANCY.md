# Dolt macOS Storage Delta Discrepancy

**Date**: 2026-02-24

## 1. Problem

Experiment 2 measures per-UPDATE storage delta on Dolt. On macOS/APFS
the experiment reports near-zero deltas. On Linux/ext4 it reports
correct non-zero deltas.

## 2. How Experiment 2 Measures Storage Delta

```
storage_delta = disk_size_after - disk_size_before
```

For Dolt, `disk_size` is computed by `dblib/util.py:get_directory_size_bytes`,
which walks the Dolt data directory and sums `st_blocks * 512` per file
(deduplicated by inode):

```python
for dirpath, _, filenames in walk(path):
    for f in filenames:
        st = os.stat(join(dirpath, f))
        if st.st_ino not in seen_inodes:
            seen_inodes.add(st.st_ino)
            total += st.st_blocks * 512   # physical allocated bytes
```

The dominant file in the data directory is the Dolt journal
(`.dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv`), an append-only file
where all chunk writes go [1][2].

## 3. The Gap (diagnostic, not measured by experiment)

**gap** := `st_blocks * 512 - st_size` on the journal file

- `st_blocks * 512` = physical bytes allocated on disk
- `st_size` = logical bytes written (actual file size)
- `gap` = allocated-but-unused bytes beyond the logical end

On macOS/APFS with N>0, the journal file exhibits a large gap
(26 KB–1 MB). On Linux/ext4, the gap stays 5–7 KB regardless of N.
The gap correlates with the zero `storage_delta` observations, but
the mechanism by which it accumulates on APFS is unknown.

## 4. Empirical Results (`demo_apfs_storage_delta.py`)

The demo measures `storage_delta` exactly like Experiment 2:
`disk_size = get_directory_size_bytes(db_dir)` (sum of `st_blocks * 512`
over the entire Dolt data directory, deduplicated by inode) before and
after each UPDATE.

50 single-row UPDATEs per case. N = number of branches created in
Phase 2 before measurement.

### macOS/APFS

```
┌─────────┬───────────┬──────────────────┬─────────────────────┐
│ N       │ DB size   │ Non-zero deltas  │ Mean storage_delta  │
├─────────┼───────────┼──────────────────┼─────────────────────┤
│ 0       │   251 KB  │ 39/50 ( 78%)     │          3,195 B    │
│ 4       │  2677 KB  │  0/50 (  0%)     │              0 B    │
│ 16      │  9922 KB  │  1/50 (  2%)     │         20,972 B    │
│ 32      │ 23362 KB  │  1/50 (  2%)     │         20,972 B    │
│ 64      │ 50194 KB  │  0/50 (  0%)     │              0 B    │
└─────────┴───────────┴──────────────────┴─────────────────────┘
```

- **N=0**: `storage_delta` detects most writes (78%).
- **N>0**: `storage_delta = 0` for nearly all writes (0–2%).

### Linux/ext4

```
┌─────────┬───────────┬──────────────────┬─────────────────────┐
│ N       │ DB size   │ Non-zero deltas  │ Mean storage_delta  │
├─────────┼───────────┼──────────────────┼─────────────────────┤
│ 0       │   440 KB  │ 39/50 ( 78%)     │          3,195 B    │
│ 4       │  3596 KB  │ 49/50 ( 98%)     │          6,226 B    │
│ 16      │ 10988 KB  │ 50/50 (100%)     │          7,045 B    │
│ 32      │ 24748 KB  │ 50/50 (100%)     │          9,667 B    │
│ 64      │ 52576 KB  │ 50/50 (100%)     │          8,847 B    │
└─────────┴───────────┴──────────────────┴─────────────────────┘
```

`storage_delta` detects writes at any DB size on ext4.

## 5. Observations

1. **macOS-only**: the discrepancy is specific to DoltgreSQL on APFS.
   Pure filesystem tests (Python `pwrite` + `fsync` or `F_FULLFSYNC`
   with the same write sizes and patterns) show no discrepancy on APFS.

2. **Threshold effect**: any branching history (N>0) causes
   `storage_delta` to report zero for nearly all UPDATEs. N=0
   (no branches) detects 78% of writes.

3. **Linux unaffected**: on ext4, `storage_delta` detects 78–100%
   of writes at any DB size.

## 6. Conclusion

On macOS/APFS with branching history (N>0), Experiment 2's
`storage_delta` (`st_blocks * 512` before/after) reports zero for
nearly all UPDATEs. On Linux/ext4, `storage_delta` correctly detects
writes at any DB size. The effect is specific to how DoltgreSQL
interacts with APFS — pure filesystem tests with the same write
sizes and patterns show no discrepancy. The exact APFS-side mechanism
is unknown (APFS is closed-source), but the effect is reproducible.

## References

[1] DoltHub, "Prolly Trees." https://docs.dolthub.com/architecture/storage-engine/prolly-tree
[2] DoltHub, "Journaling Chunk Store," 2023. https://www.dolthub.com/blog/2023-03-08-dolt-chunk-journal/
[3] Apple, "Apple File System Reference," 2020. https://developer.apple.com/support/downloads/Apple-File-System-Reference.pdf
