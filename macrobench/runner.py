"""Macrobenchmark runner implementing the work-queue automaton from Section 3.3.

T worker threads collectively execute S steps over a shared branch tree.
Each step: Assign -> Branch -> Mutate -> Evaluate -> Merge (probabilistic).
A background pass fires every B completed steps to compare + prune.

Usage:
    python -m macrobench.runner --config macrobench/configs/software_dev.textproto
"""

import argparse
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

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


class StepCounter:
    """Thread-safe atomic step counter.

    Workers call try_claim() to get the next step number. Returns None
    when all S steps have been claimed.
    """

    def __init__(self, total: int):
        self._count = 0
        self._total = total
        self._lock = threading.Lock()

    def try_claim(self) -> Optional[int]:
        """Atomically claim the next step. Returns step number or None."""
        with self._lock:
            if self._count >= self._total:
                return None
            step = self._count
            self._count += 1
            return step

    def current(self) -> int:
        with self._lock:
            return self._count


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
    else:
        raise ValueError(f"Unsupported backend: {backend}")


def _do_merge(db_tools, config, branch_node):
    """Merge a branch into its parent via the DBToolSuite API.

    Connects to the parent (target) branch, then calls merge_branch()
    which dispatches to the backend-specific implementation:
      - Dolt:  native dolt_merge() with auto conflict resolution
      - Neon:  branch restore (overwrites target with source state)
      - KPG/Xata: no-op (timing still recorded)

    Args:
        db_tools: The DBToolSuite instance.
        config: MacroBenchConfig.
        branch_node: The BranchNode being merged into its parent.
    """
    parent = branch_node.parent
    if parent is None:
        return

    # Switch to the target (parent) branch before merging.
    db_tools.connect_branch(parent.name, timed=False)
    db_tools.merge_branch(
        source_branch=branch_node.name,
        timed=True,
        message=f"merge {branch_node.name}",
    )


def _do_delete_branch(db_tools, branch_node):
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
    """
    db_tools.delete_branch(
        branch_name=branch_node.name,
        branch_id=branch_node.branch_id,
        timed=True,
    )


def _run_background_pass(
    db_tools,
    config,
    branch_tree: BranchTree,
    workflow_ops: WorkflowOps,
):
    """Execute the background process: compare + prune.

    1. Run compare queries on each alive branch.
    2. Prune gamma fraction of branches.

    Args:
        db_tools: DBToolSuite for this thread.
        config: MacroBenchConfig.
        branch_tree: Shared BranchTree.
        workflow_ops: Workflow operations providing compare queries.
    """
    gamma = config.background.prune_fraction

    # Phase 1: Cross-branch comparison
    compare_queries = workflow_ops.compare()
    alive_nodes = branch_tree.get_alive_non_root()

    for node in alive_nodes:
        try:
            db_tools.connect_branch(node.name, timed=False)
            for query in compare_queries:
                try:
                    result = db_tools.execute_sql(query, timed=True)
                    # Use the first numeric result as reward signal
                    if result and result[0]:
                        for val in result[0]:
                            if isinstance(val, (int, float)) and val is not None:
                                branch_tree.set_reward(node, float(val))
                                break
                except Exception:
                    pass
        except Exception:
            pass

    # Phase 2: Prune
    if gamma > 0:
        candidates = branch_tree.get_prune_candidates(gamma)
        for node in candidates:
            try:
                # Ensure we're not on the branch we're about to delete.
                db_tools.connect_branch(
                    branch_tree.root.name, timed=False
                )
                _do_delete_branch(db_tools, node)
            except Exception:
                pass
            branch_tree.mark_dead(node)


def worker_fn(
    thread_id: int,
    config,
    backend_info: BackendInfo,
    branch_tree: BranchTree,
    step_counter: StepCounter,
    result_collector: rc.ResultCollector,
    workflow_ops: WorkflowOps,
    progress: SharedProgress,
    bg_lock: threading.Lock,
):
    """Worker thread function implementing the per-step automaton.

    Args:
        thread_id: Unique thread identifier.
        config: MacroBenchConfig.
        backend_info: Connection info.
        branch_tree: Shared branch tree.
        step_counter: Shared atomic step counter.
        result_collector: Shared result collector.
        workflow_ops: SQL operations for the configured workflow.
        progress: Shared progress bar.
        bg_lock: Lock ensuring only one thread runs background at a time.
    """
    rc.set_current_thread_id(thread_id)
    rng = random.Random(42 + thread_id)

    # Create per-thread DB connection
    db_tools = _create_db_tools(config, backend_info, result_collector)

    # Set result context
    result_collector.set_context(
        table_name="macrobench",
        table_schema="ch-benchmark",
        initial_db_size=0,
        seed=42 + thread_id,
    )

    try:
        while True:
            step_id = step_counter.try_claim()
            if step_id is None:
                break

            # --- Assign ---
            parent_node = branch_tree.assign_parent(rng)
            if parent_node is None:
                # Tree is full (no eligible parents). Skip this step.
                progress.update(1)
                continue

            # --- Branch ---
            branch_name = f"macro_t{thread_id}_s{step_id}"
            try:
                # Connect to parent first
                db_tools.connect_branch(parent_node.name, timed=False)
                # Create child branch
                db_tools.create_branch(
                    branch_name, parent_node.branch_id, timed=True
                )
                # Connect to the new branch
                db_tools.connect_branch(branch_name, timed=False)
            except Exception as e:
                progress.write(
                    f"[T{thread_id}] Branch creation failed at step "
                    f"{step_id}: {e}"
                )
                progress.update(1)
                continue

            # Get the branch ID from the backend
            try:
                _, new_branch_id = db_tools.get_current_branch()
            except Exception:
                new_branch_id = branch_name

            child_node = branch_tree.add_child(
                parent_node, branch_name, new_branch_id
            )
            branch_tree.increment_visit(parent_node)

            # --- Mutate ---
            # Execute M_DDL DDL statements
            ddl_stmts = workflow_ops.mutate_ddl(step_id)
            for i, stmt in enumerate(ddl_stmts):
                if i >= config.step.ddl_count:
                    break
                try:
                    db_tools.execute_sql(stmt, timed=True)
                    if not config.autocommit:
                        db_tools.commit_changes(timed=False, message="ddl")
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] DDL failed at step {step_id}: {e}"
                    )

            # Execute M - M_DDL DML statements
            dml_count = config.step.mutations - config.step.ddl_count
            dml_stmts = workflow_ops.mutate_dml(step_id, rng)
            for i, stmt in enumerate(dml_stmts):
                if i >= dml_count:
                    break
                try:
                    db_tools.execute_sql(stmt, timed=True)
                    if not config.autocommit:
                        db_tools.commit_changes(timed=False, message="dml")
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] DML failed at step {step_id}: {e}"
                    )

            # --- Evaluate ---
            eval_queries = workflow_ops.evaluate()
            for i, query in enumerate(eval_queries):
                if i >= config.step.queries:
                    break
                try:
                    result = db_tools.execute_sql(query, timed=True)
                    # Use first numeric result as reward
                    if result and result[0]:
                        for val in result[0]:
                            if isinstance(val, (int, float)) and val is not None:
                                branch_tree.set_reward(child_node, float(val))
                                break
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] Eval failed at step {step_id}: {e}"
                    )

            # --- Merge (probabilistic) ---
            if config.step.merge_prob > 0 and rng.random() < config.step.merge_prob:
                try:
                    _do_merge(db_tools, config, child_node)
                except Exception as e:
                    progress.write(
                        f"[T{thread_id}] Merge failed at step {step_id}: {e}"
                    )

            # --- Background pass (inline) ---
            bg_interval = config.background.interval
            if bg_interval > 0 and (step_id + 1) % bg_interval == 0:
                if bg_lock.acquire(blocking=False):
                    try:
                        _run_background_pass(
                            db_tools, config, branch_tree,
                            workflow_ops,
                        )
                    except Exception as e:
                        progress.write(
                            f"[T{thread_id}] Background pass failed: {e}"
                        )
                    finally:
                        bg_lock.release()

            progress.update(1)

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

    print(f"Run ID: {config.run_id}")
    print(f"Backend: {tp.Backend.Name(config.backend)}")
    print(f"Workflow: {tp.WorkflowType.Name(config.workflow)}")
    print(f"Workers: {config.setup.workers}, Steps: {config.setup.total_steps}")
    print(
        f"Tree: F={config.setup.max_fanout}, D={config.setup.max_depth}"
    )
    print(
        f"Per-step: M={config.step.mutations} "
        f"(DDL={config.step.ddl_count}), Q={config.step.queries}, "
        f"mu={config.step.merge_prob}"
    )
    print(
        f"Background: B={config.background.interval}, "
        f"gamma={config.background.prune_fraction}"
    )

    # Set up backend and database
    micro_config = _build_microbench_config(config)
    backend_info = create_backend_project(micro_config)

    # Initialize components
    workflow_ops = get_workflow_ops(config.workflow)
    step_counter = StepCounter(config.setup.total_steps)

    branch_tree = BranchTree(
        root_name=backend_info.default_branch_name,
        root_id=backend_info.default_branch_id or backend_info.default_branch_name,
        max_fanout=config.setup.max_fanout,
        max_depth=config.setup.max_depth,
    )

    result_collector = rc.ResultCollector(run_id=config.run_id)
    num_workers = max(1, config.setup.workers)
    progress = SharedProgress(
        total=config.setup.total_steps,
        desc=f"Macrobench ({num_workers} workers)",
        disable=args.no_progress,
    )
    bg_lock = threading.Lock()

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
                    step_counter=step_counter,
                    result_collector=result_collector,
                    workflow_ops=workflow_ops,
                    progress=progress,
                    bg_lock=bg_lock,
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
        elapsed = time.time() - start_time
        print(f"\nCompleted in {elapsed:.1f}s")
        print(
            f"Branch tree: {branch_tree.size()} total nodes, "
            f"{branch_tree.alive_count()} alive"
        )

        # Write results
        result_collector.write_to_parquet()

        # Cleanup
        cleanup_backend(micro_config, backend_info)


if __name__ == "__main__":
    main()
