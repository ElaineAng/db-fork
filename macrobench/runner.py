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
from dblib import result_pb2 as rslt
from dblib.db_api import DBToolSuite, OpResult

# Reuse infrastructure from microbench
from microbench.runner2 import (
    BackendInfo,
    BackendManager,
    BackendSetup,
    SharedProgress,
    create_db_tools,
)

from dblib.neon import NeonToolSuite


def _create_db_tools(config, backend_info, result_collector) -> DBToolSuite:
    """Open a per-thread database tool suite on the default branch."""
    return create_db_tools(
        config.backend,
        backend_info,
        config.database_setup.db_name,
        result_collector,
        measure_storage=False,
    )


def _flush_to_disk(db_tools):
    """Flush database and OS buffers so on-disk storage measurements are accurate.

    Tries CHECKPOINT (PostgreSQL) to flush shared_buffers, then os.sync()
    to flush OS page cache.  CHECKPOINT is silently skipped for backends
    that don't support it (e.g. Dolt).
    """
    db_tools.exec(["CHECKPOINT"], timed=False)  # FAILED on Dolt, ignored
    os.sync()


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
    """Run the compare queries on every pre-committed leaf branch.

    Each leaf gets its own exec(), so the CONNECT row carries the switch
    cost and the EXEC row the query time on that branch.
    """
    leaves = branch_tree.get_pre_committed_leaves()
    for node in leaves:
        if not node.alive:
            continue
        compare_queries = workflow_ops.compare(
            step_id=node.step_id, thread_id=node.thread_id
        )
        if not compare_queries:
            continue
        res = _retry_on_rate_limit(
            lambda: db_tools.exec(
                compare_queries, refs=[node.name], storage=measure_storage,
                label="compare",
            )[0],
            result_collector,
            progress=progress,
            thread_id=thread_id,
        )
        if not res.ok:
            progress.write(
                f"[T{thread_id}] Compare on {node.name}: {res.status_name} "
                f"{res.error}"
            )


def _is_retryable_error(e):
    """Return True if the exception is a retryable Neon rate-limit or
    resource-limit error.

    Covers:
      - HTTP 429 (API rate limit)
      - "too many running operations" / "too many" (concurrent op limit)
      - "branches limit" / "endpoints limit" (active resource caps)
      - "limit reached" (generic Neon limit wording)
    """
    msg = str(e).lower()
    if any(
        pattern in msg
        for pattern in (
            "429",
            "too many",
            "running operations",
            "branches limit",
            "endpoints limit",
            "limit reached",
        )
    ):
        return True
    # NeonAPIError may lose the HTTP status code in the message;
    # check the underlying response object if available.
    resp = getattr(e, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", 0)
        if code in (429, 409):
            return True
    return False


def _retry_on_rate_limit(
    fn,
    result_collector,
    max_retries=10,
    base_delay=1.0,
    progress=None,
    thread_id=None,
    stop_event: threading.Event = None,
) -> OpResult:
    """Call ``fn`` (which returns an OpResult) again with exponential
    backoff and jitter while it comes back FAILED with a rate-limit or
    resource-limit error (HTTP 429, Neon "too many running operations",
    active branch/endpoint limits, ...).

    Each wait is recorded as an API_RETRY_WAIT row so the overhead is
    visible in results. Only the first retry is logged.
    """
    tag = f"[T{thread_id}] " if thread_id is not None else ""

    for attempt in range(max_retries):
        result = fn()
        if not result.failed or attempt == max_retries - 1:
            return result
        if not _is_retryable_error(RuntimeError(result.error)):
            return result
        delay = base_delay * (2**attempt)
        delay *= 0.5 + random.random()  # jitter against thundering herds
        if attempt == 0 and progress:
            progress.write(
                f"{tag}Rate limited, retrying "
                f"(up to {max_retries}x, {delay:.1f}s backoff)..."
            )
        stopped = False
        with result_collector.timed(rslt.OpType.API_RETRY_WAIT, label="retry"):
            if stop_event:
                stopped = stop_event.wait(delay)
            else:
                time.sleep(delay)
        if stopped:
            raise _WorkerStopped()
    return result


class _WorkerStopped(Exception):
    """Raised inside a worker when stop_event is set, to break out of nested loops."""

    pass


def worker_fn(
    thread_id: int,
    config,
    backend_info: BackendInfo,
    branch_tree: BranchTree,
    result_collector: rc.ResultCollector,
    workflow_ops: WorkflowOps,
    progress: SharedProgress,
    cb_sync: CrossBranchSync,
    stop_event: threading.Event = None,
    worker_conns: dict | None = None,
    completed_work: dict | None = None,
    max_runtime_sec: int = 0,
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
        cb_sync: Cross-branch query synchronization.
        stop_event: Event set by the main thread when deadline expires.
        worker_conns: Shared dict for main thread to cancel in-flight queries.
        completed_work: Shared dict to record {thread_id: {"steps": N, "ops": M}}.
        max_runtime_sec: Runtime cap in seconds (used for slot wait timeout).
    """
    rc.set_current_thread_id(thread_id)
    rng = random.Random(42 + thread_id)
    # Per-op storage is too expensive for Neon (pg_database_size on every
    # branch for every operation); keep the before/after in main() only.
    measure_storage = config.measure_storage and config.backend != tp.Backend.NEON
    verbose = thread_id == 0  # only log from thread 0 to reduce noise

    # Create per-thread DB connection
    db_tools = _create_db_tools(config, backend_info, result_collector)
    db_tools.measure_storage = measure_storage

    # Register connection so main thread can cancel in-flight queries
    if worker_conns is not None:
        worker_conns[thread_id] = db_tools.conn

    # Set result context
    result_collector.set_context(
        table_name="macrobench",
        table_schema="ch-benchmark",
        initial_db_size=0,
        seed=42 + thread_id,
    )

    S = config.setup.total_steps

    steps_finished = 0
    ops_finished = 0
    status = "completed"

    try:
        step_id = 0
        while step_id < S:
            # Record current step ID for all operations within this step
            result_collector.record_step_id(step_id)

            if stop_event and stop_event.is_set():
                status = "stopped"
                break

            # --- Wait for branch slot (Neon has a 20 active branch limit, and burst of 40 request/s limit) ---
            slot_timeout = 60.0
            if not branch_tree.wait_for_slot(timeout=slot_timeout):
                if verbose:
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
                res = _retry_on_rate_limit(
                    lambda: db_tools.branch(
                        branch_name, from_ref=parent_node.name, label="branch",
                    ),
                    result_collector,
                    progress=progress if verbose else None,
                    thread_id=thread_id,
                    stop_event=stop_event,
                )
                res.raise_for_status()
                ops_finished += 1
            except _WorkerStopped:
                raise
            except Exception as e:
                if stop_event and stop_event.is_set():
                    raise _WorkerStopped()
                if verbose:
                    progress.write(
                        f"[T{thread_id}] Branch create failed at step "
                        f"{step_id}: {type(e).__name__}; {e}"
                    )
                cb_sync.report_progress(thread_id, step_id)
                progress.update(1)
                step_id += 1
                continue

            child_node = branch_tree.add_child(
                parent_node,
                branch_name,
                branch_name,
                thread_id=thread_id,
                step_id=step_id,
            )

            # --- Mutate (DDL: M_s schema changes), then (DML: M_d data
            # mutations), then Evaluate (Q_v queries). Each phase is one
            # exec() on the child branch; the first one also records the
            # CONNECT row for switching to it. ---
            phases = [
                ("ddl", workflow_ops.mutate_ddl(step_id, thread_id=thread_id)[
                    : config.step.schema_changes]),
                ("dml", workflow_ops.mutate_dml(step_id, rng, thread_id=thread_id)[
                    : config.step.data_mutations]),
                ("eval", workflow_ops.evaluate(step_id=step_id, thread_id=thread_id)[
                    : config.step.eval_queries]),
            ]
            for phase, stmts in phases:
                if not stmts:
                    continue
                # Statements run one at a time so a failing statement does
                # not skip the rest of the phase.
                for stmt in stmts:
                    if stop_event and stop_event.is_set():
                        raise _WorkerStopped()
                    res = db_tools.exec(
                        [stmt], refs=[branch_name], label=phase,
                    )[0]
                    if res.ok:
                        ops_finished += 1
                    elif verbose:
                        progress.write(
                            f"[T{thread_id}] {phase.upper()} {res.status_name} at "
                            f"step {step_id}: {res.error}"
                        )
                if phase == "dml":
                    # Snapshot the mutated state (UNSUPPORTED on backends
                    # without commits; recorded as such).
                    res = db_tools.commit(
                        branch_name, message=f"step {step_id}", label="commit",
                    )
                    if res.ok:
                        ops_finished += 1

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
                # Delete the branch (the backend moves this connection off
                # it first if needed); retry on rate-limit.
                res = _retry_on_rate_limit(
                    lambda: db_tools.delete(child_node.name, label="prune"),
                    result_collector,
                    progress=progress if verbose else None,
                    thread_id=thread_id,
                    stop_event=stop_event,
                )
                if res.ok:
                    ops_finished += 1
                elif verbose:
                    progress.write(
                        f"[T{thread_id}] Prune {res.status_name} at step "
                        f"{step_id}: {res.error}"
                    )
                branch_tree.mark_dead(child_node)
            else:
                # Survived pruning — promote to committed (parent-eligible)
                branch_tree.mark_committed(child_node)

            progress.update(1)
            step_id += 1
            steps_finished += 1

    except _WorkerStopped:
        status = "interrupted"
    except Exception as e:
        status = f"crashed: {type(e).__name__}: {e}"
    finally:
        # Reset step_id to -1 after worker finishes
        result_collector.record_step_id(-1)

        if completed_work is not None:
            completed_work[thread_id] = {
                "steps": steps_finished,
                "ops": ops_finished,
                "status": status,
            }
        db_tools.close_connection()


def _fetch_neon_consumption(project_id, label="", wait_min=15, max_retries=10):
    """Wait for Neon consumption metrics, retrying once per minute after the
    initial sleep.

    Args:
        project_id: Neon project ID.
        label: Human-readable label for log messages (e.g. "after").
        wait_min: Minutes to sleep before the first API call.
        max_retries: Number of 60s retry attempts if the API returns nothing.

    Returns:
        Dict with ``all_metrics`` (list of all consumption entries),
        ``count`` (number of entries), and ``summary`` (most recent metrics),
        or None if all retries exhausted.
    """
    print(
        f"Waiting {wait_min} min for Neon consumption metrics ({label})...",
        flush=True,
    )
    for elapsed_min in range(wait_min):
        time.sleep(60)
        if (elapsed_min + 1) % 3 == 0 or elapsed_min + 1 == wait_min:
            print(
                f"  {elapsed_min + 1}/{wait_min} min elapsed...",
                flush=True,
            )

    for attempt in range(max_retries):
        result = NeonToolSuite.get_consumption_metrics(project_id)
        if result and result.get("all_metrics"):
            print(
                f"  Neon metrics {label}: "
                f"collected {result.get('count', 0)} entries from 2-day window",
                flush=True,
            )
            return result
        if attempt < max_retries - 1:
            print(
                f"  No metrics yet, retrying in 60s "
                f"({attempt + 1}/{max_retries})...",
                flush=True,
            )
            time.sleep(60)

    print(
        f"  WARNING: No Neon consumption metrics for {label}",
        flush=True,
    )
    return None


def _capabilities_for(backend) -> dict:
    from microbench.runner2 import create_db_tools  # noqa: F401 (same table)
    from dblib.dolt import DoltToolSuite
    from dblib.dolt_mysql import DoltMySQLToolSuite
    from dblib.seekdb import SeekDBToolSuite
    from dblib.xata import XataToolSuite
    from dblib.file_copy import FileCopyToolSuite

    table = {
        tp.Backend.DOLT: DoltToolSuite,
        tp.Backend.DOLT_MYSQL: DoltMySQLToolSuite,
        tp.Backend.SEEKDB: SeekDBToolSuite,
        tp.Backend.NEON: NeonToolSuite,
        tp.Backend.XATA: XataToolSuite,
        tp.Backend.FILE_COPY: FileCopyToolSuite,
    }
    cls = table.get(backend)
    return cls.capabilities() if cls else {}


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
        "--max-runtime-sec",
        type=int,
        default=0,
        help="Cap total workflow runtime in seconds (0 = no limit).",
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
    if args.max_runtime_sec:
        print(f"Runtime cap: {args.max_runtime_sec}s")

    # Set up backend and database. The macrobench DatabaseSetup has the same
    # fields as the microbench one, so it is passed through as is.
    backend_mgr = BackendManager(
        BackendSetup(backend=config.backend, database_setup=config.database_setup)
    )
    backend_info = backend_mgr.setup()

    # Initialize components
    workflow_ops = get_workflow_ops(
        config.workflow, scale=config.setup.db_scale
    )

    # Estimate logical bytes written per step (for storage amplification)
    bytes_per_step = workflow_ops.estimate_write_bytes_per_step(
        config.step.schema_changes, config.step.data_mutations
    )
    if bytes_per_step > 0:
        print(f"Estimated bytes per step: {bytes_per_step:,}")

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

    # Measure total storage before the workflow (single branch, cheap).
    storage_before = 0
    storage_db_tools = None
    if config.measure_storage:
        try:
            storage_db_tools = _create_db_tools(
                config, backend_info, result_collector
            )
            _flush_to_disk(storage_db_tools)
            storage_before = storage_db_tools._storage_bytes()
            print(f"Storage before workflow: {storage_before} bytes")
        except Exception as e:
            print(f"Warning: could not measure storage before workflow: {e}")

    print(f"\nStarting macrobenchmark with {num_workers} worker(s)...")
    start_time = time.time()
    deadline = (
        time.time() + args.max_runtime_sec if args.max_runtime_sec else None
    )
    completed_work = {}

    stop_event = threading.Event()
    worker_conns = {}

    def _on_deadline():
        stop_event.set()
        for conn in list(worker_conns.values()):
            try:
                conn.cancel()
            except Exception:
                pass

    deadline_timer = None
    if args.max_runtime_sec:
        deadline_timer = threading.Timer(args.max_runtime_sec, _on_deadline)
        deadline_timer.daemon = True
        deadline_timer.start()

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
                    stop_event=stop_event,
                    worker_conns=worker_conns,
                    completed_work=completed_work,
                    max_runtime_sec=args.max_runtime_sec,
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
        # Cancel the deadline timer if it hasn't fired
        if deadline_timer is not None:
            deadline_timer.cancel()

        elapsed = time.time() - start_time
        timed_out = deadline is not None and time.time() > deadline
        print(f"\nCompleted in {elapsed:.1f}s")
        if timed_out:
            print("Run terminated early due to runtime cap.")
        print(
            f"Branch tree: {branch_tree.size()} total nodes, "
            f"{branch_tree.alive_count()} alive"
        )
        if completed_work:
            total_configured = config.setup.total_steps
            total_steps = sum(w["steps"] for w in completed_work.values())
            total_ops = sum(w["ops"] for w in completed_work.values())
            total_possible = num_workers * total_configured
            print(
                f"  Total: {total_steps}/{total_possible} steps, "
                f"{total_ops} ops across {num_workers} worker(s)"
            )

        # Measure total storage after the workflow
        storage_after = 0
        if config.measure_storage and config.backend != tp.Backend.NEON:
            try:
                if storage_db_tools:
                    _flush_to_disk(storage_db_tools)
                    storage_after = storage_db_tools._storage_bytes()
                    print(f"Storage after workflow: {storage_after} bytes")
                    print(
                        f"Storage delta: {storage_after - storage_before} bytes"
                    )
            except Exception as e:
                print(f"Warning: could not measure storage after workflow: {e}")
            finally:
                if storage_db_tools:
                    storage_db_tools.close_connection()

        # Fetch Neon consumption metrics (root/child branch storage).
        neon_consumption = None
        if config.measure_storage and config.backend == tp.Backend.NEON:
            neon_consumption = _fetch_neon_consumption(
                backend_info.neon_project_id, label="after"
            )

        # Write end-to-end stats (always, not just when storage is measured)
        total_estimated_bytes = sum(
            v["steps"] * bytes_per_step for v in completed_work.values()
        )
        e2e_stats = {
            "run_id": config.run_id,
            "backend": tp.Backend.Name(config.backend),
            "workflow": tp.WorkflowType.Name(config.workflow),
            "workers": num_workers,
            "total_steps": config.setup.total_steps,
            "elapsed_sec": round(elapsed, 2),
            "max_runtime_sec": args.max_runtime_sec,
            "timed_out": timed_out,
            "completed_steps": {
                str(k): v["steps"] for k, v in completed_work.items()
            },
            "completed_ops": {
                str(k): v["ops"] for k, v in completed_work.items()
            },
            "worker_status": {
                str(k): v.get("status", "unknown")
                for k, v in completed_work.items()
            },
            "estimated_bytes_per_step": bytes_per_step,
            "total_estimated_bytes_written": total_estimated_bytes,
            "capabilities": _capabilities_for(config.backend),
        }
        e2e_stats.update(result_collector.support_summary())
        if config.measure_storage:
            e2e_stats["storage_before_bytes"] = storage_before
            e2e_stats["storage_after_bytes"] = storage_after
            e2e_stats["storage_delta_bytes"] = storage_after - storage_before

        # Dump all Neon consumption metrics from the 2-day window
        if neon_consumption:
            e2e_stats["neon_metrics_count"] = neon_consumption.get("count", 0)
            e2e_stats["neon_all_metrics"] = neon_consumption.get(
                "all_metrics", []
            )

            # Also include summary metrics from most recent entry for convenience
            summary = neon_consumption.get("summary", {})
            for metric_name, value in summary.items():
                e2e_stats[f"neon_{metric_name}"] = value
        e2e_stats_path = os.path.join(
            args.outdir, f"{config.run_id}_e2e_stats.json"
        )
        os.makedirs(args.outdir, exist_ok=True)
        with open(e2e_stats_path, "w") as f:
            json.dump(e2e_stats, f, indent=2)
        print(f"E2E stats written to {e2e_stats_path}")

        # Write results
        result_collector.write_to_parquet()

        # Cleanup (retry once on transient network errors)
        for attempt in range(2):
            try:
                backend_mgr.cleanup()
                break
            except Exception as e:
                if attempt == 0:
                    print(f"Cleanup failed ({type(e).__name__}), retrying...")
                    time.sleep(2)
                else:
                    print(f"Cleanup failed after retry: {e}")
                    print("You may need to delete the Neon project manually.")


if __name__ == "__main__":
    main()
