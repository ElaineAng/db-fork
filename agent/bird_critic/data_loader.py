"""Data loader for BIRD-Critic PostgreSQL dataset from HuggingFace."""

from dataclasses import dataclass
from typing import Optional
from datasets import load_dataset


@dataclass
class BirdCriticProblem:
    """A single problem instance from the BIRD-Critic dataset."""

    id: str
    db_id: str
    query: str  # Natural language user query
    error_sql: str  # The buggy SQL to fix
    sol_sql: Optional[str] = None  # Ground truth (may not be in public dataset)
    preprocess_sql: Optional[str] = None  # SQL to run before executing
    clean_up_sql: Optional[str] = None  # SQL to run after to revert changes
    test_cases: Optional[list] = None  # Test cases for validation

    @classmethod
    def from_dict(cls, data: dict, idx: int) -> "BirdCriticProblem":
        """Create a BirdCriticProblem from a dataset row."""
        return cls(
            id=data.get("instance_id", str(idx)),
            db_id=data.get("db_id", ""),
            query=data.get("query", ""),
            error_sql=data.get("issue_sql", ""),  # Dataset uses 'issue_sql'
            sol_sql=data.get("sol_sql"),  # Not available in public dataset
            preprocess_sql=data.get("preprocess_sql"),
            clean_up_sql=data.get("clean_up_sql"),
            test_cases=data.get("test_cases"),
        )


class BirdCriticDataset:
    """Loader for the BIRD-Critic PostgreSQL dataset."""

    DATASET_NAME = "birdsql/bird-critic-1.0-postgresql"

    def __init__(self, split: str = "train"):
        """Initialize the dataset loader.

        Args:
            split: Dataset split to load (default: "train").
        """
        self.split = split
        self._dataset = None
        self._problems: list[BirdCriticProblem] = []

    def load(self) -> list[BirdCriticProblem]:
        """Load the dataset from HuggingFace.

        Returns:
            List of BirdCriticProblem instances.
        """
        if self._problems:
            return self._problems

        print(f"Loading BIRD-Critic dataset ({self.split} split)...")
        self._dataset = load_dataset(self.DATASET_NAME, split=self.split)

        self._problems = [
            BirdCriticProblem.from_dict(row, idx)
            for idx, row in enumerate(self._dataset)
        ]

        print(f"Loaded {len(self._problems)} problems.")
        return self._problems

    def get_problem(self, idx: int) -> BirdCriticProblem:
        """Get a specific problem by index."""
        if not self._problems:
            self.load()
        return self._problems[idx]

    def get_problems_by_db(self, db_id: str) -> list[BirdCriticProblem]:
        """Get all problems for a specific database."""
        if not self._problems:
            self.load()
        return [p for p in self._problems if p.db_id == db_id]

    def __len__(self) -> int:
        if not self._problems:
            self.load()
        return len(self._problems)

    def __iter__(self):
        if not self._problems:
            self.load()
        return iter(self._problems)


if __name__ == "__main__":
    # Test loading the dataset
    dataset = BirdCriticDataset()
    problems = dataset.load()

    if problems:
        print("\nFirst problem:")
        print(f"  DB: {problems[0].db_id}")
        print(f"  Query: {problems[0].query[:100]}...")
        print(f"  Error SQL: {problems[0].error_sql[:100]}...")
