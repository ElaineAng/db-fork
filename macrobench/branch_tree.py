"""Thread-safe branch tree for the macrobenchmark execution model.

The tree tracks the branch topology as workers create new branches.
Parent selection: uniformly at random among committed nodes (plus the root)
whose current child count is below their fanout limit and whose depth is
at most D.  The root (depth 0) uses root_fanout; inner nodes use
inner_fanout.  D is the number of levels below the root, so the maximum
node depth in the tree is D (total tree height = D + 1).
"""

import threading
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BranchNode:
    """A single node in the branch tree."""

    name: str
    branch_id: str  # backend-specific identifier
    parent: Optional["BranchNode"] = None
    children: list["BranchNode"] = field(default_factory=list)
    depth: int = 0
    alive: bool = True
    pre_committed: bool = False  # finished DDL/DML/eval, eligible for cross-branch reads
    committed: bool = False      # survived pruning, eligible as parent


class BranchTree:
    """Thread-safe branch tree with split fanout (F_r, F_i) and depth (D).

    All public methods acquire the internal lock, so callers do not need
    external synchronization.

    An optional *max_active_branches* cap prevents more than N branches from
    being alive at the same time.  Workers that call ``wait_for_slot()``
    block until a slot is available (freed by ``mark_dead``).
    """

    def __init__(
        self,
        root_name: str,
        root_id: str,
        root_fanout: int,
        inner_fanout: int,
        max_depth: int,
        max_active_branches: int = 0,
    ):
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._root_fanout = root_fanout
        self._inner_fanout = inner_fanout
        self._max_depth = max_depth
        # 0 means unlimited
        self._max_active = max_active_branches
        # Readers-writer barrier: cross-branch queries (readers) block
        # pruning (writers).  Multiple readers can overlap; a writer
        # must wait until all readers finish.
        self._prune_lock = threading.Condition(threading.Lock())
        self._cross_branch_readers = 0
        self._root = BranchNode(
            name=root_name, branch_id=root_id, depth=0
        )
        # All nodes (including dead ones) for bookkeeping.
        self._all_nodes: list[BranchNode] = [self._root]

    @property
    def root(self) -> BranchNode:
        return self._root

    def wait_for_slot(self, timeout: float = 30.0) -> bool:
        """Block until the alive branch count is below *max_active_branches*.

        Returns True if a slot is available, False on timeout.
        Always returns True immediately when no cap is set.
        """
        with self._cond:
            if self._max_active <= 0:
                return True
            return self._cond.wait_for(
                lambda: self._alive_count_unlocked() < self._max_active,
                timeout=timeout,
            )

    def _alive_count_unlocked(self) -> int:
        """Alive count without acquiring the lock (caller holds it)."""
        return sum(1 for n in self._all_nodes if n.alive)

    def assign_parent(self, rng) -> Optional[BranchNode]:
        """Select a parent uniformly at random among eligible nodes.

        A node is eligible if:
          - it is alive
          - it is the root OR it is committed
          - its alive-child count is below its fanout limit
            (root_fanout for root, inner_fanout for inner nodes)
          - its depth is at most D (so a child at depth+1 <= D+1)

        Args:
            rng: A random.Random instance for thread-safe randomness.

        Returns:
            A BranchNode to branch from, or None if no eligible parent.
        """
        with self._lock:
            eligible = []
            for n in self._all_nodes:
                if not n.alive:
                    continue
                # Root is always eligible; inner nodes must be committed
                if n is not self._root and not n.committed:
                    continue
                # Fanout limit depends on whether this is the root
                fanout_limit = (
                    self._root_fanout if n is self._root
                    else self._inner_fanout
                )
                alive_children = len([c for c in n.children if c.alive])
                if alive_children >= fanout_limit:
                    continue
                if n.depth > self._max_depth:
                    continue
                eligible.append(n)
            if not eligible:
                return None
            return rng.choice(eligible)

    def add_child(
        self, parent: BranchNode, name: str, branch_id: str
    ) -> BranchNode:
        """Add a new child node under the given parent.

        Args:
            parent: The parent node (must have been returned by assign_parent).
            name: Branch name for the new node.
            branch_id: Backend-specific branch identifier.

        Returns:
            The newly created BranchNode.
        """
        with self._lock:
            child = BranchNode(
                name=name,
                branch_id=branch_id,
                parent=parent,
                depth=parent.depth + 1,
            )
            parent.children.append(child)
            self._all_nodes.append(child)
            return child

    def mark_pre_committed(self, node: BranchNode) -> None:
        """Mark a node as pre-committed (finished DDL/DML/eval).

        Pre-committed branches are eligible for cross-branch reads
        but NOT as parent candidates for new branches.
        """
        with self._lock:
            node.pre_committed = True

    def mark_committed(self, node: BranchNode) -> None:
        """Mark a node as committed (survived pruning, eligible as parent)."""
        with self._lock:
            node.committed = True

    def begin_cross_branch(self) -> None:
        """Enter a cross-branch read pass (reader).  Blocks pruning."""
        with self._prune_lock:
            self._cross_branch_readers += 1

    def end_cross_branch(self) -> None:
        """Exit a cross-branch read pass.  Unblocks pruning if last reader."""
        with self._prune_lock:
            self._cross_branch_readers -= 1
            if self._cross_branch_readers == 0:
                self._prune_lock.notify_all()

    def wait_prune_safe(self, timeout: float = 30.0) -> bool:
        """Block until no cross-branch queries are active (writer)."""
        with self._prune_lock:
            return self._prune_lock.wait_for(
                lambda: self._cross_branch_readers == 0,
                timeout=timeout,
            )

    def mark_dead(self, node: BranchNode) -> None:
        """Mark a node as pruned (no longer alive).

        Notifies any workers blocked in ``wait_for_slot()`` that a slot
        has been freed.
        """
        with self._cond:
            node.alive = False
            self._cond.notify_all()

    def get_alive_nodes(self) -> list[BranchNode]:
        """Return all alive nodes (including the root)."""
        with self._lock:
            return [n for n in self._all_nodes if n.alive]

    def get_alive_non_root(self) -> list[BranchNode]:
        """Return all alive non-root nodes."""
        with self._lock:
            return [
                n for n in self._all_nodes if n.alive and n is not self._root
            ]

    def get_pre_committed_leaves(self) -> list[BranchNode]:
        """Return all alive, pre-committed leaf nodes (no alive children).

        Used for cross-branch queries.  Pre-committed branches have
        finished their work and are available for reads, regardless of
        whether they have been promoted to committed (parent-eligible).
        """
        with self._lock:
            result = []
            for n in self._all_nodes:
                if not n.alive or not n.pre_committed or n is self._root:
                    continue
                alive_children = [c for c in n.children if c.alive]
                if not alive_children:
                    result.append(n)
            return result

    def size(self) -> int:
        """Return total number of nodes (alive and dead)."""
        with self._lock:
            return len(self._all_nodes)

    def alive_count(self) -> int:
        """Return number of alive nodes."""
        with self._lock:
            return sum(1 for n in self._all_nodes if n.alive)
