"""Macrobenchmark runner implementing the round-robin execution model from
Section 3.3.

T worker threads each perform S steps over a shared branch tree.
Each step: Branch -> Mutate -> Evaluate -> (mark committed) -> Prune.
C cross-branch queries are spread evenly across the S steps.

Usage:
    python -m macrobench.runner --config macrobench/configs/software_dev.textproto
"""

import argparse
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.protobuf import text_format

from macrobench import task_pb2 as tp
from macrobench.branch_tree import BranchTree
from macrobench.workflows import get_workflow_ops, WorkflowOps

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


def _create_db_tools(config, backend_info, result_collector):
    """Create a per-thread database tool suite connection.

    Mirrors the BenchmarkSuite.__enter__ pattern from microbench/runner.py
    but returns just the db_tools object.

    Args:
        config: MacroBenchConfig protobuf.
        backend_info: BackendInfo from create_backend_project().
        result_collector: Shared ResultCollector instance.

    Returns:
        A DBToolSuite subclass instance connected to the database.
    """
    backend = config.backend
    db_name = config.database_setup.db_name
    autocommit = config.autocommit

    if backend == tp.Backend.DOLT:
        return DoltToolSuite.init_for_bench(
            result_collector,
            db_name,
            autocommit,
            backend_info.default_branch_name,
        )
    elif backend == tp.Backend.KPG:
        return KpgToolSuite.init_for_bench(
            result_collector, db_name, autocommit
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


def _flush_to_disk(db_tools):
    """Flush database and OS buffers so on-disk storage measurements are accurate.

    Tries CHECKPOINT (PostgreSQL) to flush shared_buffers, then os.sync()
    to flush OS page cache.  CHECKPOINT is silently skipped for backends
    that don't support it (e.g. Dolt).
    """
    try:
        db_tools.execute_sql("CHECKPOINT")
    except Exception:
        pass  # Dolt / non-PG backends
    os.sync()


def _do_delete_branch(db_tools, branch_node, storage=False):
    """Delete a branch via the DBToolSuite API.

    Dispatches to the backend-specific implementation:
      - Dolt:  dolt_branch('-D', name)
      - Neon:  neon.branch_delete() SDK call
      - Xata:  DELETE API call
      - KPG:   no-op (base class default)

    The caller must NOT be connected to the branch being deleted.

    Args:
        db_tools: The DBToolSuite instance.
        branch_node: The BranchNode to delete.
        storage: Whether to measure storage before/after.
    """
    db_tools.delete_branch(
        branch_name=branch_node.name,
        branch_id=branch_node.branch_id,
        timed=True,
        storage=storage,
    )


class CrossBranchSync:
    """Thread-safe synchronization for cross-branch queries.

    Ensures that when a cross-branch query fires, all worker threads have
    completed (and pre-committed) at least up to that step, so
    ``get_pre_committed_leaves()`` sees every thread's latest branch.

    At most ``budget`` cross-branch queries fire across all threads combined.
    """

    def __init__(self, total_steps: int, budget: int, num_workers: int):
        self._lock = threading.Lock()
        self._remaining = budget
        if budget <= 0 or total_steps <= 0:
            self._eligible: set[int] = set()
        elif budget >= total_steps:
            self._eligible = set(range(total_steps))
        else:
            interval = max(1, total_steps // budget)
            self._eligible = {
                s for s in range(total_steps) if (s + 1) % interval == 0
            }

        # Per-thread progress: step_id of the last completed (or skipped) step.
        self._progress = [-1] * num_workers
        self._progress_cond = threading.Condition()

    def report_progress(self, thread_id: int, step_id: int) -> None:
        """Record that *thread_id* has finished (or skipped) *step_id*."""
        with self._progress_cond:
            self._progress[thread_id] = step_id
            if step_id in self._eligible:
                self._progress_cond.notify_all()

    def try_claim_and_wait(self, step_id: int, timeout: float = 120.0) -> bool:
        """Claim this step for a cross-branch query if eligible.

        If claimed, blocks until every thread has reported progress >= step_id
        so the subsequent ``get_pre_committed_leaves()`` sees all branches.
        """
        if step_id not in self._eligible:
            return False
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
        # Wait for all threads to reach at least this step.
        with self._progress_cond:
            self._progress_cond.wait_for(
                lambda: all(p >= step_id for p in self._progress),
                timeout=timeout,
            )
        return True


def _run_cross_branch_queries(
    db_tools,
    branch_tree: BranchTree,
    workflow_ops: WorkflowOps,
    progress,
    thread_id: int,
    result_collector: rc.ResultCollector = None,
    measure_storage: bool = False,
):
    """Execute cross-branch compare queries on pre-committed leaf branches."""
    compare_queries = workflow_ops.compare()
    if not compare_queries:
        return

    leaves = branch_tree.get_pre_committed_leaves()
    for node in leaves:
        if not node.alive:
            continue
        try:
            connect_fn = lambda: db_tools.connect_branch(
                node.name,
                timed=True,
                storage=measure_storage,
            )
            if result_collector:
                _retry_on_429(connect_fn, result_collector)
            else:
                connect_fn()
            for query in compare_queries:
                try:
                    db_tools.execute_sql(
                        query, timed=True, storage=measure_storage
                    )
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] Compare query failed on "
                        f"{node.name}: {e}"
                    )
        except Exception as e:
            progress.write(
                f"[T{thread_id}] Connect failed for compare on {node.name}: {e}"
            )


def _retry_on_429(fn, result_collector, max_retries=10, base_delay=1.0):
    """Retry a callable with exponential backoff + jitter on rate-limit errors.

    Handles HTTP 429, Neon "too many running operations", and similar
    rate-limit responses.  Each retry wait is recorded as an
    API_RETRY_WAIT timing entry so the overhead is visible in results.
    """
    from dblib import result_pb2 as rslt

    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            is_rate_limit = (
                "429" in msg or "too many" in msg or "running operations" in msg
            )
            # NeonAPIError loses the HTTP status code; check the
            # underlying response object if available.
            if not is_rate_limit and hasattr(e, "response"):
                resp = getattr(e, "response", None)
                if resp is not None:
                    code = getattr(resp, "status_code", 0)
                    is_rate_limit = code == 429
            if is_rate_limit and attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                # Add jitter (0.5x–1.5x) to avoid thundering herd.
                delay *= 0.5 + random.random()
                # Record the retry wait (including sleep) as a timed event.
                with result_collector.maybe_measure_ops(
                    op_type=rslt.OpType.API_RETRY_WAIT, timed=True
                ):
                    time.sleep(delay)
                result_collector.record_num_keys_touched(0)
                result_collector.flush_record()
            else:
                raise


def worker_fn(
    thread_id: int,
    config,
    backend_info: BackendInfo,
    branch_tree: BranchTree,
    result_collector: rc.ResultCollector,
    workflow_ops: WorkflowOps,
    progress: SharedProgress,
    cb_sync: CrossBranchSync,
):
    """Worker thread function implementing the per-step automaton.

    Each thread independently performs S steps in round-robin fashion.
    Per-step cycle: Branch -> Mutate -> Evaluate -> mark pre-committed -> (optional) Prune -> mark committed.
    Cross-branch queries are interleaved at evenly spaced steps.

    Args:
        thread_id: Unique thread identifier.
        config: MacroBenchConfig.
        backend_info: Connection info.
        branch_tree: Shared branch tree.
        result_collector: Shared result collector.
        workflow_ops: SQL operations for the configured workflow.
        progress: Shared progress bar.
    """
    rc.set_current_thread_id(thread_id)
    rng = random.Random(42 + thread_id)
    measure_storage = config.measure_storage

    # Create per-thread DB connection
    db_tools = _create_db_tools(config, backend_info, result_collector)

    # Set up storage measurement if enabled
    if measure_storage:
        result_collector.set_storage_fn(db_tools.get_total_storage_bytes)

    # Set result context
    result_collector.set_context(
        table_name="macrobench",
        table_schema="ch-benchmark",
        initial_db_size=0,
        seed=42 + thread_id,
    )

    S = config.setup.total_steps

    try:
        step_id = 0
        while step_id < S:
            # --- Wait for branch slot (Neon has a 20 active branch limit, and burst of 40 request/s limit) ---
            if not branch_tree.wait_for_slot(timeout=60.0):
                progress.write(
                    f"[T{thread_id}] Timed out waiting for branch slot "
                    f"at step {step_id}, skipping."
                )
                cb_sync.report_progress(thread_id, step_id)
                progress.update(1)
                step_id += 1
                continue

            # --- Branch ---
            parent_node = branch_tree.assign_parent(rng)
            if parent_node is None:
                # Tree is full (no eligible parents). Skip this step.
                cb_sync.report_progress(thread_id, step_id)
                progress.update(1)
                step_id += 1
                continue

            branch_name = f"macro_t{thread_id}_s{step_id}"
            try:
                # Create child branch (retry on rate-limit)
                _retry_on_429(
                    lambda: db_tools.create_branch(
                        branch_name,
                        parent_node.branch_id,
                        timed=True,
                        storage=measure_storage,
                    ),
                    result_collector,
                )
                # Connect to the new branch
                _retry_on_429(
                    lambda: db_tools.connect_branch(
                        branch_name,
                        timed=True,
                        storage=measure_storage,
                    ),
                    result_collector,
                )
            except Exception as e:
                progress.write(
                    f"[T{thread_id}] Branch creation failed at step "
                    f"{step_id}: {e}"
                )
                cb_sync.report_progress(thread_id, step_id)
                progress.update(1)
                step_id += 1
                continue

            # Get the branch ID from the backend
            try:
                _, new_branch_id = db_tools.get_current_branch()
            except Exception:
                new_branch_id = branch_name

            child_node = branch_tree.add_child(
                parent_node, branch_name, new_branch_id
            )

            # --- Mutate (DDL: M_s schema changes) ---
            ddl_stmts = workflow_ops.mutate_ddl(step_id, thread_id=thread_id)
            for i, stmt in enumerate(ddl_stmts):
                if i >= config.step.schema_changes:
                    break
                try:
                    db_tools.execute_sql(
                        stmt, timed=True, storage=measure_storage
                    )
                    if not config.autocommit:
                        db_tools.commit_changes(timed=False, message="ddl")
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] DDL failed at step {step_id}: {e}"
                    )

            # --- Mutate (DML: M_d data mutations) ---
            dml_stmts = workflow_ops.mutate_dml(
                step_id, rng, thread_id=thread_id
            )
            for i, stmt in enumerate(dml_stmts):
                if i >= config.step.data_mutations:
                    break
                try:
                    db_tools.execute_sql(
                        stmt, timed=True, storage=measure_storage
                    )
                    if not config.autocommit:
                        db_tools.commit_changes(timed=False, message="dml")
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] DML failed at step {step_id}: {e}"
                    )

            # --- Evaluate (Q_v queries) ---
            eval_queries = workflow_ops.evaluate()
            for i, query in enumerate(eval_queries):
                if i >= config.step.eval_queries:
                    break
                try:
                    db_tools.execute_sql(
                        query, timed=True, storage=measure_storage
                    )
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] Eval failed at step {step_id}: {e}"
                    )

            # --- Mark pre-committed (eligible for cross-branch reads) ---
            branch_tree.mark_pre_committed(child_node)
            cb_sync.report_progress(thread_id, step_id)

            # --- Cross-branch query (after work, before potential deletion) ---
            if cb_sync.try_claim_and_wait(step_id):
                branch_tree.begin_cross_branch()
                try:
                    _run_cross_branch_queries(
                        db_tools,
                        branch_tree,
                        workflow_ops,
                        progress,
                        thread_id,
                        result_collector=result_collector,
                        measure_storage=measure_storage,
                    )
                finally:
                    branch_tree.end_cross_branch()

            # --- Prune (probabilistic gamma) ---
            should_prune = (
                config.step.prune_prob > 0
                and rng.random() < config.step.prune_prob
            )
            if should_prune:
                # Wait until no cross-branch queries are running.
                branch_tree.wait_prune_safe()
                branch_tree.mark_dead(child_node)
                try:
                    _retry_on_429(
                        lambda: db_tools.connect_branch(
                            branch_tree.root.name,
                            timed=True,
                            storage=measure_storage,
                        ),
                        result_collector,
                    )
                    # Delete branch (retry on rate-limit)
                    _retry_on_429(
                        lambda: _do_delete_branch(
                            db_tools,
                            child_node,
                            storage=measure_storage,
                        ),
                        result_collector,
                    )
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] Prune failed at step {step_id}: {e}"
                    )
            else:
                # Survived pruning — promote to committed (parent-eligible)
                branch_tree.mark_committed(child_node)

            progress.update(1)
            step_id += 1

    except Exception as e:
        progress.write(f"[T{thread_id}] Worker crashed: {e}")
    finally:
        db_tools.close_connection()


def _build_microbench_config(config):
    """Build a microbench-compatible TaskConfig for create_backend_project().

    The macrobench reuses microbench's backend setup infrastructure, which
    expects a microbench.task_pb2.TaskConfig. This helper creates a minimal
    one from the macrobench config.
    """
    from microbench import task_pb2 as micro_tp

    micro_config = micro_tp.TaskConfig()
    micro_config.run_id = config.run_id
    micro_config.backend = config.backend  # enum values match
    micro_config.autocommit = config.autocommit

    # Copy database setup
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


def main():
    parser = argparse.ArgumentParser(
        description="Run macrobenchmark from config file."
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the MacroBenchConfig textproto file.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bar.",
    )
    parser.add_argument(
        "--outdir",
        type=str,
        default="/tmp/run_stats",
        help="Directory to save parquet results (default: /tmp/run_stats).",
    )
    parser.add_argument(
        "--measure-storage",
        action="store_true",
        help="Measure disk_size_before/after around each timed operation.",
    )
    parser.add_argument(
        "--measure-interference",
        action="store_true",
        help="Enable background interference measurement (3-phase: warmup/baseline/measurement).",
    )
    parser.add_argument(
        "--warmup-sec",
        type=int,
        default=5,
        help="Seconds for interference warmup phase (default: 5).",
    )
    parser.add_argument(
        "--baseline-sec",
        type=int,
        default=30,
        help="Seconds for interference baseline phase (default: 15).",
    )
    parser.add_argument(
        "--monitor-threads",
        type=int,
        default=1,
        help="Number of background interference monitor threads (default: 2).",
    )
    parser.add_argument(
        "--monitor-queries",
        type=str,
        default="oltp_read,oltp_write,olap",
        help="Comma-separated query types for interference monitor "
        "(default: oltp_read,oltp_write,olap).",
    )
    parser.add_argument(
        "--monitor-interval-ms",
        type=int,
        default=100,
        help="Sleep between monitor queries in ms (default: 0, no delay).",
    )

    args = parser.parse_args()

    # Load config
    try:
        config = tp.MacroBenchConfig()
        with open(args.config, "r") as f:
            text_format.Parse(f.read(), config)
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config: {e}")
        sys.exit(1)

    # Apply CLI overrides
    if args.measure_storage:
        config.measure_storage = True

    print(f"Run ID: {config.run_id}")
    print(f"Backend: {tp.Backend.Name(config.backend)}")
    print(f"Workflow: {tp.WorkflowType.Name(config.workflow)}")
    print(
        f"Workers: {config.setup.workers}, "
        f"Steps/worker: {config.setup.total_steps}"
    )
    print(
        f"Tree: F_r={config.setup.root_fanout}, "
        f"F_i={config.setup.inner_fanout}, "
        f"D={config.setup.max_depth}"
    )
    print(
        f"Per-step: M_s={config.step.schema_changes}, "
        f"M_d={config.step.data_mutations}, "
        f"Q_v={config.step.eval_queries}, "
        f"gamma={config.step.prune_prob:.2f}"
    )
    print(f"Cross-branch queries: C={config.setup.cross_branch_queries}")
    if config.measure_storage:
        print("Storage measurement: enabled")
    if args.measure_interference:
        print(
            f"Interference measurement: enabled "
            f"(threads={args.monitor_threads}, "
            f"warmup={args.warmup_sec}s, baseline={args.baseline_sec}s)"
        )

    # Set up backend and database
    micro_config = _build_microbench_config(config)
    backend_info = create_backend_project(micro_config)

    # Initialize components
    workflow_ops = get_workflow_ops(
        config.workflow, scale=config.setup.db_scale
    )

    # Neon limits active branches to 20 (including the default branch).
    max_active = 20 if config.backend == tp.Backend.NEON else 0
    if max_active:
        print(f"Branch limit: {max_active} active branches (Neon)")

    branch_tree = BranchTree(
        root_name=backend_info.default_branch_name,
        root_id=(
            backend_info.default_branch_id or backend_info.default_branch_name
        ),
        root_fanout=config.setup.root_fanout,
        inner_fanout=config.setup.inner_fanout,
        max_depth=config.setup.max_depth,
        max_active_branches=max_active,
    )

    result_collector = rc.ResultCollector(
        run_id=config.run_id, output_dir=args.outdir
    )
    num_workers = max(1, config.setup.workers)
    total_work = num_workers * config.setup.total_steps
    progress = SharedProgress(
        total=total_work,
        desc=f"Macrobench ({num_workers} workers)",
        disable=args.no_progress,
    )

    cb_sync = CrossBranchSync(
        config.setup.total_steps, config.setup.cross_branch_queries, num_workers
    )

    # Measure total storage before the workflow
    storage_before = 0
    storage_db_tools = None
    if config.measure_storage:
        try:
            storage_db_tools = _create_db_tools(
                config, backend_info, result_collector
            )
            _flush_to_disk(storage_db_tools)
            storage_before = storage_db_tools.get_total_storage_bytes()
            print(f"Storage before workflow: {storage_before} bytes")
        except Exception as e:
            print(f"Warning: could not measure storage before workflow: {e}")

    # --- Interference monitor setup (3-phase: warmup → baseline → measurement) ---
    monitor = None
    if args.measure_interference:
        from macrobench.interference_monitor import (
            InterferenceMonitor,
            _make_connection_factory,
            BASELINE,
            MEASUREMENT,
        )

        conn_factory = _make_connection_factory(config, backend_info)
        # evaluate_queries = workflow_ops.evaluate()
        evaluate_queries = None  # Don't run workflow queries in monitor; use separate interference queries instead.
        query_types = [
            q.strip() for q in args.monitor_queries.split(",") if q.strip()
        ]
        monitor = InterferenceMonitor(
            num_threads=args.monitor_threads,
            connection_factory=conn_factory,
            num_warehouses=workflow_ops.num_warehouses,
            evaluate_queries=evaluate_queries,
            query_types=query_types,
            interval_sec=args.monitor_interval_ms / 1000.0,
            run_id=config.run_id,
            output_dir=args.outdir,
        )
        monitor.start()
        print(f"Interference monitor: WARMUP ({args.warmup_sec}s)...")
        time.sleep(args.warmup_sec)

        monitor.set_phase(BASELINE)
        print(f"Interference monitor: BASELINE ({args.baseline_sec}s)...")
        time.sleep(args.baseline_sec)

        monitor.set_phase(MEASUREMENT)
        print("Interference monitor: MEASUREMENT (workers starting)...")

    print(f"\nStarting macrobenchmark with {num_workers} worker(s)...")
    start_time = time.time()

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(
                    worker_fn,
                    thread_id=i,
                    config=config,
                    backend_info=backend_info,
                    branch_tree=branch_tree,
                    result_collector=result_collector,
                    workflow_ops=workflow_ops,
                    progress=progress,
                    cb_sync=cb_sync,
                )
                for i in range(num_workers)
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker failed: {e}")

        progress.close()

    finally:
        # Stop interference monitor before any cleanup
        if monitor:
            monitor.stop()

        elapsed = time.time() - start_time
        print(f"\nCompleted in {elapsed:.1f}s")
        print(
            f"Branch tree: {branch_tree.size()} total nodes, "
            f"{branch_tree.alive_count()} alive"
        )

        # Write interference results
        if monitor:
            monitor.write_parquet()

        # Measure total storage after the workflow
        storage_after = 0
        if config.measure_storage:
            try:
                if storage_db_tools:
                    _flush_to_disk(storage_db_tools)
                    storage_after = storage_db_tools.get_total_storage_bytes()
                    print(f"Storage after workflow: {storage_after} bytes")
                    print(
                        f"Storage delta: {storage_after - storage_before} bytes"
                    )
            except Exception as e:
                print(f"Warning: could not measure storage after workflow: {e}")
            finally:
                if storage_db_tools:
                    storage_db_tools.close_connection()

        # Write storage summary to a separate log file
        if config.measure_storage:
            storage_log = {
                "run_id": config.run_id,
                "backend": tp.Backend.Name(config.backend),
                "workflow": tp.WorkflowType.Name(config.workflow),
                "workers": num_workers,
                "total_steps": config.setup.total_steps,
                "elapsed_sec": round(elapsed, 2),
                "storage_before_bytes": storage_before,
                "storage_after_bytes": storage_after,
                "storage_delta_bytes": storage_after - storage_before,
            }
            storage_log_path = os.path.join(
                args.outdir, f"{config.run_id}_storage.json"
            )
            os.makedirs(args.outdir, exist_ok=True)
            with open(storage_log_path, "w") as f:
                json.dump(storage_log, f, indent=2)
            print(f"Storage log written to {storage_log_path}")

        # Write results
        result_collector.write_to_parquet()

        # Cleanup
        cleanup_backend(micro_config, backend_info)


if __name__ == "__main__":
    main()
