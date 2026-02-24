"""Isolation overhead benchmark.

Measures two dimensions of isolation overhead for branchable databases:

Phase A — Branch-count scaling (single-threaded, controlled):
    For each target branch count in [0, 10, 25, 50, 100, 200]:
        Create branches until reaching target_count (each with DML divergence).
        Run analytical queries K times on the ROOT branch.
        Record (branch_count, query_id, latency).
    Only variable: branch count.  No concurrent workload.  Clean signal.

Phase B — Cross-branch OLTP/OLAP interference (concurrent):
    For each configured branch count:
        Fork oltp_branch and olap_branch from root.
        Config 1: OLTP baseline (1 thread, oltp_branch alone).
        For each olap_thread_count in [1, 2, 4]:
            Config 2: OLAP baseline (olap_threads on olap_branch alone).
            Config 3: Concurrent (OLTP + OLAP together).
        Record latencies tagged by phase for comparison.

Usage:
    python -m macrobench.isolation_bench --config macrobench/configs/isolation.textproto
"""

import argparse
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.protobuf import text_format

from macrobench import task_pb2 as tp
from dblib import result_collector as rc

# Reuse infrastructure from microbench
from microbench.runner import (
    BackendInfo,
    create_backend_project,
    cleanup_backend,
    SharedProgress,
)

# Import backend tool suites for per-thread connections
from dblib.dolt import DoltToolSuite
from dblib.neon import NeonToolSuite
from dblib.kpg import KpgToolSuite
from dblib.xata import XataToolSuite
from dblib.file_copy import FileCopyToolSuite


# ─────────────────────────────────────────────────────────────────────
# OLAP query (Phase A + Phase B)
# ─────────────────────────────────────────────────────────────────────

def _get_olap_query(oltp_table, olap_contended):
    """Return a single OLAP query based on table and contention settings.

    When contended, the OLAP query hits the same table as OLTP updates.
    When uncontended, it hits a different table (order_line x warehouse).
    """
    if oltp_table == "customer":
        if olap_contended:
            return (
                "olap_customer_agg",
                "SELECT c_credit, COUNT(*), AVG(c_balance) FROM customer GROUP BY c_credit;",
            )
        else:
            return (
                "olap_orderline_warehouse",
                "SELECT SUM(ol_amount) FROM order_line ol JOIN warehouse w ON ol.ol_supply_w_id = w.w_id;",
            )
    else:  # district
        if olap_contended:
            return (
                "olap_district_agg",
                "SELECT d_w_id, SUM(d_ytd), AVG(d_next_o_id) FROM district GROUP BY d_w_id;",
            )
        else:
            return (
                "olap_orderline_warehouse",
                "SELECT SUM(ol_amount) FROM order_line ol JOIN warehouse w ON ol.ol_supply_w_id = w.w_id;",
            )


# ─────────────────────────────────────────────────────────────────────
# OLTP updates (Phase B)
# ─────────────────────────────────────────────────────────────────────

def _oltp_update(oltp_table, rng, seq):
    """Generate a deterministic OLTP UPDATE, picked round-robin via seq % 4.

    Args:
        oltp_table: "customer" or "district".
        rng: random.Random instance (for randomizing the update amount).
        seq: monotonically increasing sequence number.

    Returns (query_name, sql_string).
    """
    variant = seq % 4

    if oltp_table == "customer":
        w_id = (seq % 10) + 1
        d_id = ((seq * 3 + 7) % 10) + 1
        c_id = (seq % 3000) + 1
        where = f"WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
        amount = round(rng.uniform(1, 100), 2)

        if variant == 0:
            return ("oltp_update_balance",
                    f"UPDATE customer SET c_balance = c_balance - {amount} {where};")
        elif variant == 1:
            return ("oltp_update_ytd_payment",
                    f"UPDATE customer SET c_ytd_payment = c_ytd_payment + {amount} {where};")
        elif variant == 2:
            return ("oltp_update_payment_cnt",
                    f"UPDATE customer SET c_payment_cnt = c_payment_cnt + 1 {where};")
        else:
            return ("oltp_update_delivery_cnt",
                    f"UPDATE customer SET c_delivery_cnt = c_delivery_cnt + 1 {where};")
    else:  # district
        w_id = (seq % 10) + 1
        d_id = ((seq * 3 + 7) % 10) + 1
        where = f"WHERE d_w_id = {w_id} AND d_id = {d_id}"
        amount = round(rng.uniform(1, 100), 2)

        if variant == 0:
            return ("oltp_update_d_ytd_plus",
                    f"UPDATE district SET d_ytd = d_ytd + {amount} {where};")
        elif variant == 1:
            return ("oltp_update_d_next_oid_plus",
                    f"UPDATE district SET d_next_o_id = d_next_o_id + 1 {where};")
        elif variant == 2:
            return ("oltp_update_d_ytd_minus",
                    f"UPDATE district SET d_ytd = d_ytd - {amount} {where};")
        else:
            return ("oltp_update_d_next_oid_minus",
                    f"UPDATE district SET d_next_o_id = d_next_o_id - 1 {where};")


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _build_microbench_config(config):
    """Build a microbench-compatible TaskConfig for create_backend_project().

    Same pattern as macrobench/runner.py.
    """
    from microbench import task_pb2 as micro_tp

    micro_config = micro_tp.TaskConfig()
    micro_config.run_id = config.run_id
    micro_config.backend = config.backend
    micro_config.autocommit = config.autocommit

    micro_config.database_setup.db_name = config.database_setup.db_name
    micro_config.database_setup.cleanup = config.database_setup.cleanup

    source = config.database_setup.WhichOneof("source")
    if source == "sql_dump":
        micro_config.database_setup.sql_dump.sql_dump_path = (
            config.database_setup.sql_dump.sql_dump_path
        )
    elif source == "existing_db":
        micro_config.database_setup.existing_db.branch_id = (
            config.database_setup.existing_db.branch_id
        )
        micro_config.database_setup.existing_db.neon_project_id = (
            config.database_setup.existing_db.neon_project_id
        )

    return micro_config


def _create_db_tools(config, backend_info, result_collector):
    """Create a per-thread database tool suite connection.

    Mirrors macrobench/runner.py _create_db_tools().
    """
    backend = config.backend
    db_name = config.database_setup.db_name
    autocommit = config.autocommit

    if backend == tp.Backend.DOLT:
        return DoltToolSuite.init_for_bench(
            result_collector, db_name, autocommit,
            backend_info.default_branch_name,
        )
    elif backend == tp.Backend.KPG:
        return KpgToolSuite.init_for_bench(
            result_collector, db_name, autocommit,
        )
    elif backend == tp.Backend.NEON:
        return NeonToolSuite.init_for_bench(
            result_collector,
            backend_info.neon_project_id,
            backend_info.default_branch_id,
            backend_info.default_branch_name,
            db_name,
            autocommit,
        )
    elif backend == tp.Backend.XATA:
        return XataToolSuite.init_for_bench(
            result_collector,
            backend_info.xata_project_id,
            backend_info.default_branch_id,
            backend_info.default_branch_name,
            db_name,
            autocommit,
        )
    elif backend == tp.Backend.FILE_COPY:
        return FileCopyToolSuite.init_for_bench(
            result_collector,
            db_name,
            autocommit,
            backend_info.default_branch_name,
            backend_info.file_copy_info.branches,
        )
    else:
        raise ValueError(f"Unsupported backend: {backend}")


# ─────────────────────────────────────────────────────────────────────
# Phase A: Branch-count scaling
# ─────────────────────────────────────────────────────────────────────

def run_phase_a(config, backend_info, result_collector,
                oltp_table, olap_contended):
    """Phase A: measure analytical query latency as branch count grows.

    Single-threaded, no concurrent workload. Only variable is branch count.
    """
    branch_counts = sorted(config.branch_counts)
    k = config.queries_per_step or 3
    mutations_per_branch = config.mutations_per_branch or 5
    rng = random.Random(42)

    olap_name, olap_sql = _get_olap_query(oltp_table, olap_contended)

    print(f"\n{'='*60}")
    print("Phase A: Branch-count scaling")
    print(f"  Branch counts: {list(branch_counts)}")
    print(f"  Queries per step: {k}")
    print(f"  Mutations per branch: {mutations_per_branch}")
    print(f"  OLAP query: {olap_name}")
    print(f"{'='*60}")

    rc.set_current_thread_id(0)
    db_tools = _create_db_tools(config, backend_info, result_collector)

    result_collector.set_context(
        table_name="phase_a_scaling",
        table_schema="ch-benchmark",
        initial_db_size=0,
        seed=42,
    )

    root_name = backend_info.default_branch_name
    _, root_id = db_tools.get_current_branch()

    current_branch_count = 0
    branch_idx = 0

    try:
        for target_count in branch_counts:
            # Create branches until we reach target_count
            while current_branch_count < target_count:
                branch_name = f"iso_a_{branch_idx}"
                branch_idx += 1

                try:
                    db_tools.create_branch(
                        branch_name, root_id, timed=False
                    )
                    db_tools.connect_branch(branch_name, timed=False)

                    # Apply DML mutations for data divergence
                    for _ in range(mutations_per_branch):
                        w_id = rng.randint(1, 10)
                        d_id = rng.randint(1, 10)
                        stmt = (
                            f"UPDATE district SET d_ytd = d_ytd + "
                            f"{round(rng.uniform(1, 100), 2)} "
                            f"WHERE d_w_id = {w_id} AND d_id = {d_id};"
                        )
                        try:
                            db_tools.execute_sql(stmt, timed=False)
                            if not config.autocommit:
                                db_tools.commit_changes(
                                    timed=False, message="iso_diverge"
                                )
                        except Exception:
                            pass

                    current_branch_count += 1
                except Exception as e:
                    print(f"  Warning: branch creation failed: {e}")
                    break

            # Switch back to root for measurement
            db_tools.connect_branch(root_name, timed=False)

            print(
                f"  Measuring at branch_count={current_branch_count} "
                f"(1 query x {k} reps)..."
            )

            # Record branch count and run the single OLAP query
            result_collector.record_branch_count(current_branch_count)
            for rep in range(k):
                try:
                    result_collector.record_branch_count(
                        current_branch_count
                    )
                    db_tools.execute_sql(olap_sql, timed=True)
                except Exception as e:
                    print(
                        f"    Query {olap_name} rep {rep} failed: {e}"
                    )

    finally:
        db_tools.close_connection()

    print("Phase A complete.")


# ─────────────────────────────────────────────────────────────────────
# Phase B: Cross-branch OLTP/OLAP interference
# ─────────────────────────────────────────────────────────────────────

def _oltp_worker(
    config, backend_info, result_collector, branch_name,
    duration_sec, warmup_sec, stop_event, phase_label,
    branch_count, oltp_table,
):
    """OLTP worker thread: runs UPDATE queries on a branch.

    Args:
        config: IsolationBenchConfig.
        backend_info: BackendInfo.
        result_collector: Shared ResultCollector.
        branch_name: Branch to run OLTP on.
        duration_sec: Total measurement duration.
        warmup_sec: Warmup before recording.
        stop_event: Threading event signaling stop.
        phase_label: Tag for table_name (e.g. "phase_b_oltp_baseline").
        branch_count: Current branch count for metadata.
        oltp_table: "customer" or "district".
    """
    thread_id = 100  # Reserved ID for OLTP thread
    rc.set_current_thread_id(thread_id)
    # Unique seed per config so baseline doesn't warm cache for concurrent.
    rng = random.Random(hash(phase_label))

    db_tools = _create_db_tools(config, backend_info, result_collector)

    try:
        db_tools.connect_branch(branch_name, timed=False)

        start_time = time.monotonic()
        warmup_end = start_time + warmup_sec
        measure_end = start_time + warmup_sec + duration_sec
        seq = 0

        while not stop_event.is_set() and time.monotonic() < measure_end:
            now = time.monotonic()
            in_measurement = now >= warmup_end

            query_name, query_sql = _oltp_update(oltp_table, rng, seq)
            seq += 1

            if in_measurement:
                result_collector.set_context(
                    table_name=phase_label,
                    table_schema="ch-benchmark",
                    initial_db_size=0,
                    seed=42,
                )
                result_collector.record_branch_count(branch_count)
                try:
                    db_tools.execute_sql(query_sql, timed=True)
                except Exception:
                    pass
            else:
                # Warmup: execute but don't record
                try:
                    db_tools.execute_sql(query_sql, timed=False)
                except Exception:
                    pass
    finally:
        db_tools.close_connection()


def _olap_worker(
    config, backend_info, result_collector, branch_name,
    duration_sec, warmup_sec, stop_event, phase_label,
    branch_count, thread_id, oltp_table, olap_contended,
):
    """OLAP worker thread: runs a single analytical query in a loop.

    Args:
        config: IsolationBenchConfig.
        backend_info: BackendInfo.
        result_collector: Shared ResultCollector.
        branch_name: Branch to run OLAP on.
        duration_sec: Total measurement duration.
        warmup_sec: Warmup before recording.
        stop_event: Threading event signaling stop.
        phase_label: Tag for table_name.
        branch_count: Current branch count for metadata.
        thread_id: Unique thread ID for this OLAP worker.
        oltp_table: "customer" or "district".
        olap_contended: Whether OLAP hits the same table as OLTP.
    """
    rc.set_current_thread_id(thread_id)

    _, olap_sql = _get_olap_query(oltp_table, olap_contended)
    db_tools = _create_db_tools(config, backend_info, result_collector)

    try:
        db_tools.connect_branch(branch_name, timed=False)

        start_time = time.monotonic()
        warmup_end = start_time + warmup_sec
        measure_end = start_time + warmup_sec + duration_sec

        while not stop_event.is_set() and time.monotonic() < measure_end:
            now = time.monotonic()
            in_measurement = now >= warmup_end

            if in_measurement:
                result_collector.set_context(
                    table_name=phase_label,
                    table_schema="ch-benchmark",
                    initial_db_size=0,
                    seed=42,
                )
                result_collector.record_branch_count(branch_count)
                try:
                    db_tools.execute_sql(olap_sql, timed=True)
                except Exception:
                    pass
            else:
                try:
                    db_tools.execute_sql(olap_sql, timed=False)
                except Exception:
                    pass
    finally:
        db_tools.close_connection()


def _setup_branches_for_phase_b(
    config, backend_info, result_collector, target_count, rng,
):
    """Create branches with divergent DML for Phase B measurement.

    Returns list of created branch names.
    """
    mutations_per_branch = config.mutations_per_branch or 5
    db_tools = _create_db_tools(config, backend_info, result_collector)
    _, root_id = db_tools.get_current_branch()
    created = []

    try:
        for i in range(target_count):
            branch_name = f"iso_b_bg_{i}"
            try:
                db_tools.create_branch(branch_name, root_id, timed=False)
                db_tools.connect_branch(branch_name, timed=False)
                for _ in range(mutations_per_branch):
                    w_id = rng.randint(1, 10)
                    d_id = rng.randint(1, 10)
                    stmt = (
                        f"UPDATE district SET d_ytd = d_ytd + "
                        f"{round(rng.uniform(1, 100), 2)} "
                        f"WHERE d_w_id = {w_id} AND d_id = {d_id};"
                    )
                    try:
                        db_tools.execute_sql(stmt, timed=False)
                        if not config.autocommit:
                            db_tools.commit_changes(
                                timed=False, message="iso_b_diverge"
                            )
                    except Exception:
                        pass
                created.append(branch_name)
            except Exception as e:
                print(f"  Warning: Phase B branch creation failed: {e}")
                break

        # Create the OLTP and OLAP branches (delete stale ones first)
        db_tools.connect_branch(
            backend_info.default_branch_name, timed=False
        )
        for name in ["oltp_branch", "olap_branch"]:
            try:
                db_tools.delete_branch(name, timed=False)
            except Exception:
                pass
            db_tools.create_branch(name, root_id, timed=False)
    finally:
        db_tools.close_connection()

    return created


def _cleanup_phase_b_branches(
    config, backend_info, result_collector, bg_branches,
):
    """Delete branches created during Phase B."""
    db_tools = _create_db_tools(config, backend_info, result_collector)
    try:
        db_tools.connect_branch(
            backend_info.default_branch_name, timed=False
        )
        for name in ["oltp_branch", "olap_branch"] + bg_branches:
            try:
                db_tools.delete_branch(name, timed=False)
            except Exception as e:
                print(f"    Warning: failed to delete branch {name}: {e}")
    except Exception as e:
        print(f"    Warning: cleanup connection failed: {e}")
    finally:
        db_tools.close_connection()


def _run_timed_config(
    config, backend_info, result_collector, branch_count,
    duration_sec, warmup_sec,
    run_oltp, run_olap, olap_threads, phase_label_prefix,
    oltp_table, olap_contended,
    olap_branches=None,
):
    """Run a timed configuration (baseline or concurrent).

    Args:
        run_oltp: Whether to run OLTP thread.
        run_olap: Whether to run OLAP threads.
        olap_threads: Number of OLAP threads (only if run_olap).
        phase_label_prefix: Base label (e.g. "phase_b_concurrent_2t").
        oltp_table: "customer" or "district".
        olap_contended: Whether OLAP hits the same table as OLTP.
        olap_branches: List of branch names for OLAP threads. Each thread
            is assigned a different branch (round-robin). Defaults to
            ["olap_branch"] if not provided.
    """
    if not olap_branches:
        olap_branches = ["olap_branch"]

    stop_event = threading.Event()
    futures = []

    with ThreadPoolExecutor(
        max_workers=(1 if run_oltp else 0) + (olap_threads if run_olap else 0)
    ) as executor:
        if run_oltp:
            futures.append(
                executor.submit(
                    _oltp_worker,
                    config, backend_info, result_collector,
                    "oltp_branch", duration_sec, warmup_sec,
                    stop_event, f"{phase_label_prefix}_oltp",
                    branch_count, oltp_table,
                )
            )

        if run_olap:
            for t in range(olap_threads):
                branch = olap_branches[t % len(olap_branches)]
                futures.append(
                    executor.submit(
                        _olap_worker,
                        config, backend_info, result_collector,
                        branch, duration_sec, warmup_sec,
                        stop_event, f"{phase_label_prefix}_olap",
                        branch_count,
                        200 + t,  # thread IDs: 200, 201, 202, ...
                        oltp_table, olap_contended,
                    )
                )

        # Wait for all threads to finish
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  Thread failed: {e}")


def run_phase_b(config, backend_info, result_collector,
                oltp_table, olap_contended):
    """Phase B: measure OLTP/OLAP interference across branches.

    For each configured branch count:
      1. Create background branches + oltp_branch + olap_branch
      2. Config 1: OLTP baseline
      3. For each olap_thread_count:
           Config 2: OLAP baseline
           Config 3: OLTP + OLAP concurrent
      4. Clean up branches
    """
    branch_counts = sorted(config.branch_counts)
    olap_thread_counts = list(config.olap_thread_counts) or [1, 2, 4]
    duration_sec = config.interference_duration_sec or 60
    warmup_sec = config.warmup_sec or 5
    rng = random.Random(99)

    olap_name, _ = _get_olap_query(oltp_table, olap_contended)

    print(f"\n{'='*60}")
    print("Phase B: OLTP/OLAP interference")
    print(f"  Branch counts: {list(branch_counts)}")
    print(f"  OLAP thread counts: {olap_thread_counts}")
    print(f"  Duration: {duration_sec}s + {warmup_sec}s warmup")
    print(f"  OLTP table: {oltp_table}")
    print(f"  OLAP contended: {olap_contended} ({olap_name})")
    print(f"{'='*60}")

    for target_count in branch_counts:
        print(f"\n  --- Branch count: {target_count} ---")

        # Setup: create background branches + measurement branches
        print(f"  Creating {target_count} background branches...")
        bg_branches = _setup_branches_for_phase_b(
            config, backend_info, result_collector, target_count, rng,
        )
        actual_count = len(bg_branches) + 2  # +oltp_branch +olap_branch

        try:
            # Use background branches for OLAP threads so each runs on a
            # separate branch, testing true cross-branch interference.
            # Fall back to olap_branch when no background branches exist.
            olap_branches = bg_branches if bg_branches else ["olap_branch"]

            # Config 1: OLTP baseline
            print(f"  Running OLTP baseline ({duration_sec}s)...")
            _run_timed_config(
                config, backend_info, result_collector,
                branch_count=actual_count,
                duration_sec=duration_sec,
                warmup_sec=warmup_sec,
                run_oltp=True,
                run_olap=False,
                olap_threads=0,
                phase_label_prefix="phase_b_oltp_baseline",
                oltp_table=oltp_table,
                olap_contended=olap_contended,
            )

            for n_olap in olap_thread_counts:
                # Config 2: OLAP baseline
                print(
                    f"  Running OLAP baseline "
                    f"({n_olap} threads on {min(n_olap, len(olap_branches))} "
                    f"branches, {duration_sec}s)..."
                )
                _run_timed_config(
                    config, backend_info, result_collector,
                    branch_count=actual_count,
                    duration_sec=duration_sec,
                    warmup_sec=warmup_sec,
                    run_oltp=False,
                    run_olap=True,
                    olap_threads=n_olap,
                    phase_label_prefix=f"phase_b_olap_baseline_{n_olap}t",
                    oltp_table=oltp_table,
                    olap_contended=olap_contended,
                    olap_branches=olap_branches,
                )

                # Config 3: Concurrent OLTP + OLAP
                print(
                    f"  Running concurrent OLTP + OLAP "
                    f"({n_olap} OLAP threads on {min(n_olap, len(olap_branches))} "
                    f"branches, {duration_sec}s)..."
                )
                _run_timed_config(
                    config, backend_info, result_collector,
                    branch_count=actual_count,
                    duration_sec=duration_sec,
                    warmup_sec=warmup_sec,
                    run_oltp=True,
                    run_olap=True,
                    olap_threads=n_olap,
                    phase_label_prefix=f"phase_b_concurrent_{n_olap}t",
                    oltp_table=oltp_table,
                    olap_contended=olap_contended,
                    olap_branches=olap_branches,
                )

        finally:
            # Clean up Phase B branches
            print(f"  Cleaning up {len(bg_branches) + 2} branches...")
            _cleanup_phase_b_branches(
                config, backend_info, result_collector, bg_branches,
            )


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run isolation overhead benchmark."
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to IsolationBenchConfig textproto file.",
    )
    parser.add_argument(
        "--phase",
        type=str,
        default="both",
        choices=["a", "b", "both"],
        help="Which phase to run (default: both).",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="run_stats/",
        help="Directory for output parquet files (default: run_stats/).",
    )
    parser.add_argument(
        "--oltp-table",
        type=str,
        default="customer",
        choices=["customer", "district"],
        help="Table targeted by OLTP updates (default: customer).",
    )
    parser.add_argument(
        "--olap-contended",
        action="store_true",
        default=False,
        help="OLAP query hits the same table as OLTP (contention).",
    )

    args = parser.parse_args()

    # Load config
    try:
        config = tp.IsolationBenchConfig()
        with open(args.config, "r") as f:
            text_format.Parse(f.read(), config)
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config: {e}")
        sys.exit(1)

    print(f"Run ID: {config.run_id}")
    print(f"Backend: {tp.Backend.Name(config.backend)}")
    print(f"Branch counts: {list(config.branch_counts)}")
    print(f"OLAP thread counts: {list(config.olap_thread_counts)}")
    print(f"OLTP table: {args.oltp_table}")
    print(f"OLAP contended: {args.olap_contended}")

    # Set up backend and database
    micro_config = _build_microbench_config(config)
    backend_info = create_backend_project(micro_config)
    result_collector = rc.ResultCollector(
        run_id=config.run_id, output_dir=args.outdir,
    )

    start_time = time.time()

    try:
        if args.phase in ("a", "both"):
            run_phase_a(config, backend_info, result_collector,
                        args.oltp_table, args.olap_contended)

        if args.phase in ("b", "both"):
            run_phase_b(config, backend_info, result_collector,
                        args.oltp_table, args.olap_contended)

    finally:
        elapsed = time.time() - start_time
        print(f"\nCompleted in {elapsed:.1f}s")

        # Write results
        result_collector.write_to_parquet()

        # Cleanup
        cleanup_backend(micro_config, backend_info)


if __name__ == "__main__":
    main()
