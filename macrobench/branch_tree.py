"""Thread-safe branch tree for the macrobenchmark work-queue automaton.

The tree tracks the branch topology as workers create new branches.
Parent selection follows the paper's definition: uniformly at random among
all alive nodes whose current child count is below F and whose depth is
below D.
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
    visit_count: int = 0
    reward: float = 0.0
    alive: bool = True


class BranchTree:
    """Thread-safe branch tree with fanout (F) and depth (D) constraints.

    All public methods acquire the internal lock, so callers do not need
    external synchronization.
    """

    def __init__(
        self,
        root_name: str,
        root_id: str,
        max_fanout: int,
        max_depth: int,
    ):
        self._lock = threading.Lock()
        self._max_fanout = max_fanout
        self._max_depth = max_depth
        self._root = BranchNode(
            name=root_name, branch_id=root_id, depth=0
        )
        # All nodes (including dead ones) for bookkeeping.
        self._all_nodes: list[BranchNode] = [self._root]

    @property
    def root(self) -> BranchNode:
        return self._root

    def assign_parent(self, rng) -> Optional[BranchNode]:
        """Select a parent uniformly at random among eligible nodes.

        A node is eligible if it is alive, its child count is below F,
        and its depth is below D (so a child at depth+1 <= D).

        Args:
            rng: A random.Random instance for thread-safe randomness.

        Returns:
            A BranchNode to branch from, or None if no eligible parent.
        """
        with self._lock:
            eligible = [
                n
                for n in self._all_nodes
                if n.alive
                and len([c for c in n.children if c.alive]) < self._max_fanout
                and n.depth < self._max_depth
            ]
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

    def mark_dead(self, node: BranchNode) -> None:
        """Mark a node as pruned (no longer alive)."""
        with self._lock:
            node.alive = False

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

    def get_prune_candidates(self, fraction: float) -> list[BranchNode]:
        """Return the bottom fraction of alive non-root nodes for pruning.

        Nodes are sorted by (visit_count, reward) ascending so that the
        least-visited / lowest-reward branches are pruned first.

        Args:
            fraction: Float in [0, 1]. Fraction of alive non-root nodes
                      to return as prune candidates.

        Returns:
            List of BranchNode to prune.
        """
        with self._lock:
            alive = [
                n
                for n in self._all_nodes
                if n.alive and n is not self._root
            ]
            if not alive or fraction <= 0:
                return []
            alive.sort(key=lambda n: (n.visit_count, n.reward))
            count = max(1, int(len(alive) * fraction))
            return alive[:count]

    def increment_visit(self, node: BranchNode) -> None:
        """Increment the visit count for a node."""
        with self._lock:
            node.visit_count += 1

    def set_reward(self, node: BranchNode, reward: float) -> None:
        """Set the reward value for a node."""
        with self._lock:
            node.reward = reward

    def size(self) -> int:
        """Return total number of nodes (alive and dead)."""
        with self._lock:
            return len(self._all_nodes)

    def alive_count(self) -> int:
        """Return number of alive nodes."""
        with self._lock:
            return sum(1 for n in self._all_nodes if n.alive)
