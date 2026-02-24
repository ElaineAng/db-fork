#!/usr/bin/env python3
"""Verify st_blocks behavior for Dolt UPDATE on ext4 vs APFS.

PURPOSE: Demonstrate that each Dolt UPDATE (with autocommit) flushes
new Prolly tree chunks to the append-only journal file, causing
st_blocks to increase on every operation.

RUN ON BOTH PLATFORMS to compare:
  macOS:  DOLT_PORT=5433 python3 verify_st_blocks.py
  Linux:  DOLT_PORT=5433 python3 verify_st_blocks.py

EXPECTED RESULTS:
  ext4 (Linux): ~100% non-zero (journal file grows, st_blocks increases)
  APFS (macOS): Need to verify — the macOS Exp 2 report shows 0.7% non-zero

REQUIREMENTS:
  - DoltgreSQL running on DOLT_PORT (default 5433)
  - psycopg2 installed
  - DOLT_DATA_DIR pointing to DoltgreSQL's data directory
"""

import os
import time
import psycopg2

# ── Configuration ────────────────────────────────────────────────────

DOLT_PORT = int(os.environ.get("DOLT_PORT", 5433))
DOLT_DATA_DIR = os.environ.get(
    "DOLT_DATA_DIR", "/tmp/doltgres_data/databases"
)
DOLT_USER = os.environ.get("DOLT_USER", "postgres")
DOLT_PASSWORD = os.environ.get("DOLT_PASSWORD", "password")
NUM_UPDATES = 50  # Match experiment (50 UPDATEs per run)
DB_NAME = "st_blocks_verify"

# ── Helpers ──────────────────────────────────────────────────────────

def get_dir_st_blocks(path):
    """Sum st_blocks * 512 for all files (same as dblib/util.py)."""
    total = 0
    seen = set()
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                st = os.stat(fp)
                if st.st_ino not in seen:
                    seen.add(st.st_ino)
                    total += st.st_blocks * 512
            except OSError:
                pass
    return total


def snapshot_files(path):
    """Return {relative_path: (st_blocks_bytes, st_size)}."""
    snap = {}
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            rel = os.path.relpath(fp, path)
            try:
                st = os.stat(fp)
                snap[rel] = (st.st_blocks * 512, st.st_size)
            except OSError:
                pass
    return snap


def fmt(b):
    if abs(b) >= 1 << 20:
        return f"{b / (1 << 20):.2f} MB"
    if abs(b) >= 1 << 10:
        return f"{b / (1 << 10):.2f} KB"
    return f"{b} B"


def connect(dbname):
    return psycopg2.connect(
        host="localhost", port=DOLT_PORT,
        user=DOLT_USER, password=DOLT_PASSWORD, dbname=dbname,
    )


# ── Setup ────────────────────────────────────────────────────────────

uname = os.uname()
try:
    fs_line = os.popen("df -T /tmp 2>/dev/null | tail -1").read().split()
    fs_type = fs_line[1] if len(fs_line) > 1 else "unknown"
except Exception:
    fs_type = "unknown"

print("=" * 70)
print(f"Platform:    {uname.sysname} {uname.release}")
print(f"Filesystem:  {fs_type}")
print(f"Dolt port:   {DOLT_PORT}")
print(f"Data dir:    {DOLT_DATA_DIR}")
print(f"Scenario:    {NUM_UPDATES} UPDATEs with AUTOCOMMIT (matches Exp 2)")
print("=" * 70)

conn = connect("postgres")
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
if cur.fetchone():
    cur.execute(f"DROP DATABASE {DB_NAME}")
cur.execute(f"CREATE DATABASE {DB_NAME}")
conn.close()

conn = connect(DB_NAME)
conn.autocommit = True  # Matches experiment: autocommit = true
cur = conn.cursor()

# Create TPC-C orders table (matching experiment schema exactly)
cur.execute("""
    CREATE TABLE orders (
        o_id int4 NOT NULL,
        o_d_id int2 NOT NULL,
        o_w_id int2 NOT NULL,
        o_c_id int4,
        o_entry_d int8,
        o_carrier_id int2,
        o_ol_cnt int2,
        o_all_local int2,
        PRIMARY KEY (o_id, o_d_id, o_w_id)
    )
""")

# Seed 100 rows (matching experiment seed count)
import random
rng = random.Random(42)
for i in range(100):
    cur.execute(
        "INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (i, 1, 1, rng.randint(1, 999999), rng.randint(0, 999999),
         rng.randint(1, 99), rng.randint(1, 15), rng.randint(0, 1)),
    )
cur.execute("SELECT dolt_commit('-Am', 'seed data')")

db_dir = os.path.join(DOLT_DATA_DIR, DB_NAME)

# ── Measure: UPDATE with autocommit (no explicit dolt_commit) ────────

print(f"\nDolt data dir: {db_dir}")
print(f"\n{'Op':>4}  {'Before':>12}  {'After':>12}  {'Delta':>10}  Changed file")
print("-" * 75)

deltas = []
for i in range(NUM_UPDATES):
    snap_before = snapshot_files(db_dir)
    before = sum(v[0] for v in snap_before.values())

    # UPDATE a known row (guaranteed hit, matching experiment pattern)
    row_id = i % 100
    cur.execute(
        "UPDATE orders SET o_c_id = %s, o_entry_d = %s, o_carrier_id = %s "
        "WHERE o_id = %s AND o_d_id = 1 AND o_w_id = 1",
        (rng.randint(1, 999999), rng.randint(0, 999999), rng.randint(1, 99),
         row_id),
    )

    snap_after = snapshot_files(db_dir)
    after = sum(v[0] for v in snap_after.values())
    delta = after - before
    deltas.append(delta)

    # Identify which file changed
    changed = []
    for f in sorted(set(snap_before) | set(snap_after)):
        b = snap_before.get(f, (0, 0))
        a = snap_after.get(f, (0, 0))
        if a[0] != b[0]:
            changed.append(f"{os.path.basename(f)} (+{fmt(a[0]-b[0])})")
    changed_str = ", ".join(changed) if changed else "(none)"

    print(f"{i:>4}  {fmt(before):>12}  {fmt(after):>12}  {fmt(delta):>10}  {changed_str}")

# ── Summary ──────────────────────────────────────────────────────────

nz = sum(1 for d in deltas if d != 0)
total = len(deltas)
mean_d = sum(deltas) / total

print(f"\n{'=' * 70}")
print(f"RESULT")
print(f"{'=' * 70}")
print(f"  Non-zero deltas: {nz}/{total} ({nz/total*100:.1f}%)")
print(f"  Zero deltas:     {total-nz}/{total} ({(total-nz)/total*100:.1f}%)")
print(f"  Mean delta:      {fmt(mean_d)}")
print(f"  Unique deltas:   {sorted(set(deltas))}")
print()

if nz / total > 0.9:
    print("  INTERPRETATION: Almost every UPDATE flushes new Prolly tree chunks")
    print("  to the append-only journal file. st_blocks grows monotonically.")
    print("  This matches the Linux Exp 2 result (99.7% non-zero).")
elif nz / total < 0.1:
    print("  INTERPRETATION: UPDATEs are buffered in the working set. The chunk")
    print("  journal file rarely grows. This matches the macOS Exp 2 result (0.7%).")
else:
    print(f"  INTERPRETATION: Mixed — {nz/total*100:.0f}% of UPDATEs trigger disk flush.")
    print("  Partial buffering in working set, partial flush to journal.")

# ── Cleanup ──────────────────────────────────────────────────────────

cur.close()
conn.close()
conn = connect("postgres")
conn.autocommit = True
cur = conn.cursor()
cur.execute(f"DROP DATABASE {DB_NAME}")
cur.close()
conn.close()
print("\nCleaned up test database.")
