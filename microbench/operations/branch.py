"""
Branch operations for benchmarking version control databases.

This module implements operations for creating, connecting to, and deleting
database branches. These operations are specific to branching databases like
Dolt, Neon, etc.
"""

from typing import TYPE_CHECKING

from dblib import result_pb2 as rslt
from microbench.operations.base import Operation
from microbench import task2_pb2 as tp

if TYPE_CHECKING:
    from microbench.runner2 import WorkerContext


class BranchCreateOperation(Operation):
    """Create a new database branch.

    Creates a new branch from the current branch. The branch name is
    automatically generated based on the thread ID and a unique branch counter.
    """

    def __init__(self):
        pass

    def _generate_branch_info(self, context: 'WorkerContext'):
        """Shared logic: generate branch name and get parent info.

        Returns:
            Tuple of (branch_name, current_branch_id)
        """
        # Get unique branch ID and current branch info
        branch_id = context.get_next_branch_id()
        _, current_branch_id = context.db_tools.get_current_branch()

        # Generate unique branch name
        branch_name = f"branch_tid{context.thread_id}_{branch_id}"

        return (branch_name, current_branch_id)

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed branch creation operation."""
        branch_name, current_branch_id = self._generate_branch_info(context)

        # Create the branch (timed, with optional storage measurement)
        context.db_tools.create_branch(
            branch_name=branch_name,
            parent_id=current_branch_id,
            timed=True,
            storage=context.measure_storage,
        )

        # Track the newly created branch
        context.add_branch(branch_name)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared preparation logic."""
        branch_name, current_branch_id = self._generate_branch_info(context)

        # Create the branch asynchronously (timed, with optional storage measurement)
        await context.db_tools.create_branch_async(
            branch_name=branch_name,
            parent_id=current_branch_id,
            timed=True,
            storage=context.measure_storage,
        )

        # Track the newly created branch
        context.add_branch(branch_name)

    def requires_setup_data(self) -> bool:
        return False  # Can create branches without data

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.BRANCH_CREATE


class BranchConnectOperation(Operation):
    """Connect to a random existing branch.

    Selects a random branch from the list of created branches and
    switches the current connection to that branch.
    """

    def __init__(self):
        pass

    def _select_branch(self, context: 'WorkerContext'):
        """Shared logic: select a random branch to connect to.

        Returns:
            branch_name to connect to
        """
        # Get a random existing branch
        branch_to_connect = context.get_random_branch()
        if not branch_to_connect:
            raise ValueError("No branches available to connect to")
        return branch_to_connect

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed branch connection operation."""
        branch_to_connect = self._select_branch(context)

        # Connect to the branch (timed)
        context.db_tools.connect_branch(branch_to_connect, timed=True)

        # Clear cached primary keys since we're on a different branch
        context.clear_pk_cache()

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared selection logic."""
        branch_to_connect = self._select_branch(context)

        # Connect to the branch asynchronously (timed)
        await context.db_tools.connect_branch_async(branch_to_connect, timed=True)

        # Clear cached primary keys since we're on a different branch
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.BRANCH_CONNECT


class BranchDeleteOperation(Operation):
    """Delete a database branch.

    Deletes an existing branch. Note: Some backends may not support
    deleting branches or may have restrictions (e.g., can't delete
    the current branch).
    """

    def __init__(self):
        pass

    def _select_branch_to_delete(self, context: 'WorkerContext'):
        """Shared logic: select a branch to delete (not current).

        Returns:
            branch_name to delete
        """
        # Get a random branch to delete (not the current one)
        current_branch_name, _ = context.db_tools.get_current_branch()
        branch_to_delete = context.get_random_branch()

        # Don't delete the current branch
        if branch_to_delete == current_branch_name:
            # Try to get another branch
            all_branches = context.get_all_branches()
            other_branches = [b for b in all_branches if b != current_branch_name]
            if not other_branches:
                raise ValueError("No other branches available to delete")
            branch_to_delete = context.rnd.choice(other_branches)

        return branch_to_delete

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed branch deletion operation."""
        branch_to_delete = self._select_branch_to_delete(context)

        # Delete the branch (timed)
        context.db_tools.delete_branch(branch_to_delete, timed=True)

        # Remove from branch tracking
        context.remove_branch(branch_to_delete)

    async def execute_async(self, context: 'WorkerContext') -> None:
        """Async version using shared selection logic."""
        branch_to_delete = self._select_branch_to_delete(context)

        # Delete the branch asynchronously (timed)
        await context.db_tools.delete_branch_async(branch_to_delete, timed=True)

        # Remove from branch tracking
        context.remove_branch(branch_to_delete)

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.BRANCH_DELETE


class ConnectFirstOperation(Operation):
    """Connect to the first branch created during setup.

    This operation is used to measure the cost of connecting to a specific
    branch (the first one created), which may have different performance
    characteristics than random branch connections.
    """

    def __init__(self):
        pass

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed connection to the first branch."""
        # Use the db_tools method that connects to a specific position
        context.db_tools.connect_specific_branch(tp.OperationType.CONNECT_FIRST)

        # Clear cached primary keys since we're on a different branch
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.CONNECT_FIRST


class ConnectMidOperation(Operation):
    """Connect to a branch in the middle of the setup branches.

    This operation is used to measure the cost of connecting to a branch
    that's in the middle of the branch list (by creation order).
    """

    def __init__(self):
        pass

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed connection to a middle branch."""
        # Use the db_tools method that connects to a specific position
        context.db_tools.connect_specific_branch(tp.OperationType.CONNECT_MID)

        # Clear cached primary keys since we're on a different branch
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.CONNECT_MID


class ConnectLastOperation(Operation):
    """Connect to the last branch created during setup.

    This operation is used to measure the cost of connecting to the most
    recently created branch, which may have different performance
    characteristics (e.g., better cache locality).
    """

    def __init__(self):
        pass

    def execute(self, context: 'WorkerContext') -> None:
        """Execute a timed connection to the last branch."""
        # Use the db_tools method that connects to a specific position
        context.db_tools.connect_specific_branch(tp.OperationType.CONNECT_LAST)

        # Clear cached primary keys since we're on a different branch
        context.clear_pk_cache()

    def requires_setup_data(self) -> bool:
        return True  # Needs branches to exist from setup

    def get_operation_type(self) -> rslt.OpType:
        return rslt.OpType.CONNECT_LAST
