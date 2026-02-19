from __future__ import annotations

import threading
import time
from typing import Optional

from tqdm import tqdm


class SharedBranchManager:
    """Thread-safe manager for branch IDs and branch list across threads."""

    def __init__(
        self, initial_branch_id: int = 1, initial_branches: list[str] = None
    ):
        self._next_branch_id = initial_branch_id
        self._branches: list[str] = (
            list(initial_branches) if initial_branches else []
        )
        self._branch_limit_reached = False
        self._lock = threading.Lock()

    def get_next_branch_id(self) -> int:
        """Atomically get the next branch ID and increment the counter."""
        with self._lock:
            current = self._next_branch_id
            self._next_branch_id += 1
            return current

    def add_branch(self, branch_name: str) -> None:
        """Add a branch to the shared list."""
        with self._lock:
            self._branches.append(branch_name)

    def get_random_branch(self, rnd) -> Optional[str]:
        """Get a random branch from the list."""
        with self._lock:
            if not self._branches:
                return None
            return rnd.choice(self._branches)

    def get_all_branches(self) -> list[str]:
        """Get a copy of all branches."""
        with self._lock:
            return list(self._branches)

    def __len__(self) -> int:
        with self._lock:
            return len(self._branches)

    def is_branch_limit_reached(self) -> bool:
        """Check if the branch limit has been reached."""
        with self._lock:
            return self._branch_limit_reached

    def set_branch_limit_reached(self) -> None:
        """Mark that the branch limit has been reached."""
        with self._lock:
            self._branch_limit_reached = True


class SharedProgress:
    """Thread-safe shared progress bar across multiple threads."""

    def __init__(
        self, total: int, desc: str = "Progress", disable: bool = False
    ):
        self._pbar = tqdm(
            total=total, desc=desc, position=0, leave=True, disable=disable
        )
        self._lock = threading.Lock()
        self._count = 0

    def update(self, n: int = 1) -> None:
        """Thread-safe update of progress bar."""
        with self._lock:
            self._pbar.update(n)
            self._count += n

    def close(self) -> None:
        """Close the progress bar."""
        self._pbar.close()

    def write(self, msg: str) -> None:
        """Thread-safe write message above progress bar."""
        tqdm.write(msg)


class SharedTimer:
    """Thread-safe timer that signals all threads to stop after a duration."""

    def __init__(self, duration_seconds: float):
        self._duration = duration_seconds
        self._stop_event = threading.Event()
        self._start_time = None

    def start(self):
        self._start_time = time.monotonic()
        timer = threading.Timer(self._duration, self._stop_event.set)
        timer.daemon = True
        timer.start()

    def should_continue(self) -> bool:
        return not self._stop_event.is_set()

    def elapsed(self) -> float:
        return time.monotonic() - self._start_time if self._start_time else 0.0
