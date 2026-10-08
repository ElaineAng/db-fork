"""
Branch operations for benchmarking version control databases.

Create, connect to, and delete branches through the git-like DBToolSuite
API. Connect is measured as an exec() with an empty script on the target
branch, so the CONNECT row carries the switch cost.
"""

from typing import TYPE_CHECKING

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class BranchCreateOperation(Operation):
    """Create a new branch from the worker's current branch.

    The branch name is generated from the thread ID and a shared counter.
    """

    def __init__(self):
        pass

    def _generate_branch_name(self, context: 'WorkerContext') -> str:
        branch_id = context.get_next_branch_id()
        return f"branch_tid{context.thread_id}_{branch_id}"

    def execute(self, context: 'WorkerContext') -> None:
        branch_name = self._generate_branch_name(context)
        context.db_tools.branch(
            branch_name,
            from_ref=context.current_ref,
            storage=context.measure_storage,
        ).raise_for_status()
        context.add_branch(branch_name)

    def requires_setup_data(self) -> bool:
        return False  # Can create branches without data

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.BRANCH


class BranchConnectOperation(Operation):
    """Switch the worker's connection to a random existing branch."""

    def __init__(self):
        pass

    def _select_branch(self, context: 'WorkerContext') -> str:
        branch_to_connect = context.get_random_branch()
        if not branch_to_connect:
            raise ValueError("No branches available to connect to")
        return branch_to_connect

    def execute(self, context: 'WorkerContext') -> None:
        branch_to_connect = self._select_branch(context)
        context.connect(branch_to_connect)
        context.clear_pk_cache()

    async def execute_async(self, context: 'WorkerContext') -> None:
        branch_to_connect = self._select_branch(context)
        await context.connect_async(branch_to_connect)
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.CONNECT


class BranchDeleteOperation(Operation):
    """Delete a random branch other than the worker's current one."""

    def __init__(self):
        pass

    def _select_branch_to_delete(self, context: 'WorkerContext') -> str:
        """A random branch other than the worker's current one and the
        backend's root branch (Dolt refuses to delete its default)."""
        excluded = {context.current_branch, context.root_branch}
        candidates = [b for b in context.get_all_branches() if b not in excluded]
        if not candidates:
            raise ValueError("No other branches available to delete")
        return context.rnd.choice(candidates)

    def execute(self, context: 'WorkerContext') -> None:
        branch_to_delete = self._select_branch_to_delete(context)
        context.db_tools.delete(
            branch_to_delete, storage=context.measure_storage
        ).raise_for_status()
        context.remove_branch(branch_to_delete)

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.DELETE
