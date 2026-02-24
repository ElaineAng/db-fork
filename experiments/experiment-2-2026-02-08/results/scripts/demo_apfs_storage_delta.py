"""
APFS Allocation Gap Demo: Why macOS reports zero storage deltas for Dolt.

Measures storage_delta exactly like Experiment 2:
    disk_size      = sum(st_blocks * 512) over the Dolt data directory
    storage_delta  = disk_size_after - disk_size_before  (per UPDATE)

On macOS/APFS with any branching history (N>0), storage_delta is zero
for nearly all UPDATEs. On Linux/ext4, storage_delta is non-zero.

Requires: DoltgreSQL on localhost:5433, psycopg2
"""

import os
import platform
import psycopg2
import random

CFG = dict(host="localhost", port=5433, user="postgres", password="password")
DATA_DIR = "/tmp/doltgres_data/databases"
JOURNAL = ".dolt/noms/vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
N_OPS = 50


def get_directory_size_bytes(path):
    """Physical disk usage of a directory, in bytes.

    Matches Experiment 2's measurement (dblib/util.py:get_directory_size_bytes):
    st_blocks * 512 per file, deduplicated by inode.
    """
    if not os.path.exists(path):
        return 0
    total = 0
    seen_inodes = set()
    for root, _, files in os.walk(path):
        for f in files:
            st = os.stat(os.path.join(root, f))
            if st.st_ino not in seen_inodes:
                seen_inodes.add(st.st_ino)
                total += st.st_blocks * 512
    return total


def connect(db="postgres"):
    c = psycopg2.connect(**CFG, dbname=db)
    c.autocommit = True
    return c


def create_db(name, n_branches):
    """Create a Dolt database replicating Experiment 2's Phase 1 + 2.

    Phase 1: table t(id, v), seed 100 rows, commit.
    Phase 2: create n_branches in chain (main -> b1 -> b2 -> ...),
             each with 100 INSERTs + 20 UPDATEs + dolt_commit.
    """
    db_dir = os.path.join(DATA_DIR, name)
    print(f"    [create_db] start: n_branches={n_branches}, "
          f"db_dir {'does not exist yet' if not os.path.exists(db_dir) else f'{get_directory_size_bytes(db_dir)/1024:.0f} KB'}")

    conn = connect()
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {name}")
    cur.execute(f"CREATE DATABASE {name}")
    cur.close(); conn.close()

    conn = connect(name)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    cur.execute("SELECT dolt_commit('-Am', 'init')")

    rnd, pk, pks = random.Random(42), 0, []
    for _ in range(100):
        cur.execute("INSERT INTO t VALUES (%s,%s)", (pk, rnd.randint(1, 100)))
        pks.append(pk); pk += 1
    cur.execute("SELECT dolt_commit('-Am', 'seed')")

    for b in range(1, n_branches + 1):
        parent = f"b{b-1}" if b > 1 else "main"
        cur.execute(f"SELECT dolt_checkout('{parent}')")
        cur.execute(f"SELECT dolt_branch('b{b}')")
        cur.execute(f"SELECT dolt_checkout('b{b}')")
        for _ in range(100):
            cur.execute("INSERT INTO t VALUES (%s,%s)", (pk, rnd.randint(1, 100)))
            pks.append(pk); pk += 1
        for _ in range(20):
            cur.execute("UPDATE t SET v=%s WHERE id=%s",
                        (rnd.randint(1, 10), rnd.choice(pks)))
        cur.execute(f"SELECT dolt_commit('-Am', 'br{b}')")

    branch = f"b{n_branches}" if n_branches else "main"
    cur.close(); conn.close()
    print(f"    [create_db] done:  db_dir = {get_directory_size_bytes(db_dir)/1024:.0f} KB")
    return branch, pks


def cleanup(name):
    c = connect()
    c.cursor().execute(f"DROP DATABASE IF EXISTS {name}")
    c.close()


def run_updates(name, branch, pks):
    """Run N_OPS UPDATEs, measure storage_delta exactly like Experiment 2.

    For each UPDATE:
        disk_size_before = get_directory_size_bytes(db_dir)
        <execute UPDATE>
        disk_size_after  = get_directory_size_bytes(db_dir)
        storage_delta    = disk_size_after - disk_size_before

    Returns (db_size, nonzero_count).
    """
    db_dir = os.path.join(DATA_DIR, name)
    conn = connect(name)
    cur = conn.cursor()
    cur.execute(f"SELECT dolt_checkout('{branch}')")

    rnd = random.Random(999)
    nonzero_count = 0
    deltas = []

    for _ in range(N_OPS):
        disk_size_before = get_directory_size_bytes(db_dir)
        cur.execute("UPDATE t SET v=%s WHERE id=%s",
                    (rnd.randint(1, 100), rnd.choice(pks)))
        disk_size_after = get_directory_size_bytes(db_dir)

        storage_delta = disk_size_after - disk_size_before
        deltas.append(storage_delta)
        if storage_delta != 0:
            nonzero_count += 1

    db_size = get_directory_size_bytes(db_dir)
    mean_delta = sum(deltas) / len(deltas) if deltas else 0

    cur.close(); conn.close()
    cleanup(name)
    return db_size, nonzero_count, mean_delta


def run_case(label, name, n_branches):
    """Run one case: create DB, measure UPDATEs, print results."""
    print(f"{label}")
    br, pks = create_db(name, n_branches)
    db_size, nz_count, mean_delta = run_updates(name, br, pks)
    print(f"  DB size:               {db_size/1024:>8.0f} KB")
    print(f"  Mean storage_delta:    {mean_delta:>8,.0f} B")
    print(f"  Non-zero deltas:       {nz_count:>2}/{N_OPS} ({nz_count/N_OPS*100:.0f}%)")
    print()
    return db_size, nz_count, mean_delta


def _print_summary(title, results):
    """Print summary table."""
    print(f"{'='*68}")
    print(f"  {title}")
    print(f"{'='*68}\n")
    print("┌─────────┬───────────┬──────────────────┬─────────────────────┐")
    print("│ N       │ DB size   │ Non-zero deltas  │ Mean storage_delta  │")
    print("├─────────┼───────────┼──────────────────┼─────────────────────┤")
    for db_size, nz, mean_d, n_br in results:
        print(f"│ {n_br:<7d} │ {db_size/1024:>6.0f} KB │"
              f" {nz:>2}/{N_OPS} ({nz/N_OPS*100:>3.0f}%)      │"
              f" {mean_d:>14,.0f} B    │")
    print("└─────────┴───────────┴──────────────────┴─────────────────────┘")


# ── Case configurations ───────────────────────────────────────────────
#   (label, db_name, n_branches)

CASES_MAC = [
    ("Case 1: N=0  (0 branches)",   "apfs_demo_0",   0),
    ("Case 2: N=4  (4 branches)",   "apfs_demo_4",   4),
    ("Case 3: N=16 (16 branches)",  "apfs_demo_16",  16),
    ("Case 4: N=32 (32 branches)",  "apfs_demo_32",  32),
    ("Case 5: N=64 (64 branches)",  "apfs_demo_64",  64),
]

CASES_LINUX = [
    ("N=0  (0 branches)",   "ext4_demo_0",   0),
    ("N=4  (4 branches)",   "ext4_demo_4",   4),
    ("N=16 (16 branches)",  "ext4_demo_16",  16),
    ("N=32 (32 branches)",  "ext4_demo_32",  32),
    ("N=64 (64 branches)",  "ext4_demo_64",  64),
]


def main():
    is_mac = platform.system() == "Darwin"
    is_linux = platform.system() == "Linux"

    print(f"Platform: {platform.system()}")
    print(f"Measurement: {N_OPS} single-row UPDATEs per case")
    print(f"storage_delta = disk_size_after - disk_size_before")
    print(f"disk_size = sum(st_blocks * 512) over Dolt data directory\n")

    if is_mac:
        results = []
        for label, name, n_br in CASES_MAC:
            db_size, nz, mean_d = run_case(label, name, n_br)
            results.append((db_size, nz, mean_d, n_br))
        _print_summary(f"macOS/APFS: {N_OPS} UPDATEs per case", results)
        print()
        print("  N=0:  storage_delta detects most writes.")
        print("  N>0:  storage_delta = 0 for nearly all writes.")

    elif is_linux:
        results = []
        for label, name, n_br in CASES_LINUX:
            db_size, nz, mean_d = run_case(label, name, n_br)
            results.append((db_size, nz, mean_d, n_br))
        _print_summary(f"Linux/ext4: {N_OPS} UPDATEs per case", results)
        print()
        print("  storage_delta detects writes at any DB size.")

    else:
        print("Unsupported platform. Run on macOS or Linux.")


if __name__ == "__main__":
    main()
