"""Background interference measurement for macrobench.

Runs a fixed query mix (OLTP read, OLTP write, OLAP) on the root branch
continuously in background threads across three phases:
  1. WARMUP  — discard results (warm caches, establish connections)
  2. BASELINE — record latencies with no branch workers active
  3. MEASUREMENT — record latencies while branch workers are running

Comparing BASELINE vs MEASUREMENT latency reveals whether branching
activity (create/mutate/prune on child branches) degrades main-branch
query performance.
"""

import os
import random
import threading
import time

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import pyarrow as pa
import pyarrow.parquet as pq

from macrobench import task_pb2 as tp

# Phase constants
WARMUP = "warmup"
BASELINE = "baseline"
MEASUREMENT = "measurement"


def _make_connection_factory(config, backend_info):
    """Return a callable that creates a raw psycopg2 autocommit connection
    to the root branch.

    Args:
        config: MacroBenchConfig protobuf.
        backend_info: BackendInfo from create_backend_project().

    Returns:
        A zero-arg callable that returns a new psycopg2 connection.
    """
    backend = config.backend
    db_name = config.database_setup.db_name

    if backend == tp.Backend.NEON:
        from dblib.neon import NeonToolSuite

        def _connect():
            uri = NeonToolSuite._get_neon_connection_uri(
                backend_info.neon_project_id,
                backend_info.default_branch_id,
                db_name,
            )
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn

    elif backend == tp.Backend.DOLT:
        from dblib.dolt import DoltToolSuite

        def _connect():
            uri = DoltToolSuite.get_initial_connection_uri(db_name)
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn

    elif backend == tp.Backend.KPG:
        from dblib.kpg import KpgToolSuite

        def _connect():
            uri = KpgToolSuite.get_initial_connection_uri(db_name)
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn

    elif backend == tp.Backend.FILE_COPY:
        from dblib.file_copy import FileCopyToolSuite

        def _connect():
            uri = FileCopyToolSuite.get_initial_connection_uri(db_name)
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn

    elif backend == tp.Backend.TXN:
        from dblib.transaction import TxnToolSuite

        def _connect():
            uri = TxnToolSuite.get_initial_connection_uri(db_name)
            conn = psycopg2.connect(uri)
            # Don't use autocommit - keep transaction open to simulate
            # concurrent queries against a database with a long-running
            # transaction containing many savepoints
            return conn

    else:
        raise ValueError(
            f"Unsupported backend for interference monitor: {backend}"
        )

    return _connect


def _generate_oltp_read(num_warehouses, seq):
    """Generate an OLTP point-lookup SELECT on the customer table."""
    w_id = (seq % num_warehouses) + 1
    d_id = ((seq * 3 + 7) % 10) + 1
    c_id = (seq % 3000) + 1
    return (
        f"SELECT c_id, c_balance, c_ytd_payment FROM customer "
        f"WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
    )


def _generate_oltp_write(num_warehouses, rng, seq):
    """Generate an OLTP UPDATE on the customer table."""
    w_id = (seq % num_warehouses) + 1
    d_id = ((seq * 3 + 7) % 10) + 1
    c_id = (seq % 3000) + 1
    amount = round(rng.uniform(1, 100), 2)
    return (
        f"UPDATE customer SET c_balance = c_balance - {amount} "
        f"WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
    )


def _generate_olap_heavy(num_warehouses, seq):
    """Return an expensive OLAP query scanning the customer table.

    Rotates a ``WHERE c_w_id <= N`` bound so the SQL text (and scanned
    data) differs each iteration, defeating query caching.
    """
    w_bound = (seq % num_warehouses) + 1
    return (
        f"SELECT c_credit, COUNT(*), AVG(c_ytd_payment) "
        f"FROM customer WHERE c_w_id <= {w_bound} GROUP BY c_credit"
    )


def _generate_olap_light(num_warehouses, seq):
    """Return a cheap OLAP query scanning the warehouse table.

    The warehouse table has only ``num_warehouses`` rows (typically 1–10),
    so this is fast even at scale.
    """
    w_bound = (seq % num_warehouses) + 1
    return (
        f"SELECT COUNT(*), SUM(w_ytd), AVG(w_tax) "
        f"FROM warehouse WHERE w_id <= {w_bound}"
    )


def _interference_worker(
    thread_id,
    connection_factory,
    num_warehouses,
    query_types,
    interval_sec,
    phase_ref,
    stop_event,
    results,
):
    """Tight loop executing a round-robin query mix on the root branch.

    Args:
        thread_id: Unique thread identifier (1000, 1001, ...).
        connection_factory: Zero-arg callable returning a psycopg2 connection.
        num_warehouses: Number of warehouses for query parameter generation.
        query_types: List of query type strings to cycle through
                     (e.g. ["oltp_read", "olap_heavy", "olap_light"]).
        interval_sec: Sleep between queries (0 = no delay).
        phase_ref: Single-element list holding the current phase string.
        stop_event: threading.Event signalling shutdown.
        results: Shared list to append (timestamp, phase, thread_id,
                 query_type, latency) tuples.
    """
    rng = random.Random(42 + thread_id)
    seq = 0
    n_types = len(query_types)

    conn = None
    try:
        conn = connection_factory()

        while not stop_event.is_set():
            phase = phase_ref[0]
            qtype = query_types[seq % n_types]

            if qtype == "oltp_read":
                sql = _generate_oltp_read(num_warehouses, seq)
            elif qtype == "oltp_write":
                sql = _generate_oltp_write(num_warehouses, rng, seq)
            elif qtype == "olap_heavy":
                sql = _generate_olap_heavy(num_warehouses, seq)
            elif qtype == "olap_light":
                sql = _generate_olap_light(num_warehouses, seq)
            else:
                sql = _generate_olap_heavy(num_warehouses, seq)

            seq += 1

            try:
                t0 = time.perf_counter()
                with conn.cursor() as cur:
                    cur.execute(sql)
                    if cur.description is not None:
                        cur.fetchall()
                elapsed = time.perf_counter() - t0
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

            results.append((
                time.time(),       # wall-clock timestamp
                phase,             # "warmup", "baseline", or "measurement"
                thread_id,
                qtype,
                elapsed,
            ))

            if interval_sec > 0:
                time.sleep(interval_sec)

    except Exception:
        # Connection failed — nothing we can do, let the thread exit
        pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


class InterferenceMonitor:
    """Manages background interference-measurement threads and parquet output.

    Usage:
        monitor = InterferenceMonitor(...)
        monitor.start()                    # begins WARMUP
        time.sleep(warmup_sec)
        monitor.set_phase(BASELINE)
        time.sleep(baseline_sec)
        monitor.set_phase(MEASUREMENT)
        # ... run branch workers ...
        monitor.stop()
        monitor.write_parquet()
    """

    VALID_QUERY_TYPES = {"oltp_read", "oltp_write", "olap_heavy", "olap_light"}

    def __init__(
        self,
        num_threads,
        connection_factory,
        num_warehouses,
        run_id,
        output_dir,
        query_types=None,
        interval_sec=0.0,
    ):
        self._num_threads = num_threads
        self._connection_factory = connection_factory
        self._num_warehouses = num_warehouses
        self._run_id = run_id
        self._output_dir = output_dir
        self._interval_sec = interval_sec

        if query_types is None:
            query_types = ["oltp_read", "oltp_write", "olap_heavy", "olap_light"]
        unknown = set(query_types) - self.VALID_QUERY_TYPES
        if unknown:
            raise ValueError(
                f"Unknown query types: {unknown}. "
                f"Valid types: {sorted(self.VALID_QUERY_TYPES)}"
            )
        self._query_types = query_types

        # Shared mutable phase — single-element list for GIL-safe reads
        self._phase_ref = [WARMUP]
        self._stop_event = threading.Event()
        self._results = []  # shared list of tuples
        self._threads = []

    def start(self):
        """Spawn daemon threads and begin WARMUP phase."""
        for i in range(self._num_threads):
            tid = 1000 + i  # avoid collision with workers 0..T
            t = threading.Thread(
                target=_interference_worker,
                args=(
                    tid,
                    self._connection_factory,
                    self._num_warehouses,
                    self._query_types,
                    self._interval_sec,
                    self._phase_ref,
                    self._stop_event,
                    self._results,
                ),
                daemon=True,
            )
            t.start()
            self._threads.append(t)

    def set_phase(self, phase):
        """Transition to BASELINE or MEASUREMENT (thread-safe via GIL)."""
        self._phase_ref[0] = phase

    def stop(self):
        """Signal stop and join all threads (timeout 10s each)."""
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=10)

    def write_parquet(self):
        """Write collected samples to {run_id}_interference.parquet.

        Warmup samples are excluded from the output.
        """
        kept = [r for r in self._results if r[1] != WARMUP]
        if not kept:
            print("No interference samples to write.")
            return

        os.makedirs(self._output_dir, exist_ok=True)

        timestamps, phases, thread_ids, query_types, latencies = (
            zip(*kept)
        )

        table = pa.table({
            "timestamp": pa.array(timestamps, type=pa.float64()),
            "phase": pa.array(phases, type=pa.string()),
            "thread_id": pa.array(thread_ids, type=pa.int64()),
            "query_type": pa.array(query_types, type=pa.string()),
            "latency": pa.array(latencies, type=pa.float64()),
        })

        filepath = os.path.join(
            self._output_dir, f"{self._run_id}_interference.parquet"
        )
        pq.write_table(table, filepath)
        print(f"Wrote {len(kept)} interference samples to {filepath}")
