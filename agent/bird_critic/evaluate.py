"""Evaluation utilities for BIRD-Critic agent predictions."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from .agent import BirdCriticAgent
from .data_loader import BirdCriticDataset, BirdCriticProblem
from .db_utils import DatabaseManager


@dataclass
class EvaluationResult:
    """Result of evaluating a single problem."""

    problem_id: str
    db_id: str
    query: str
    error_sql: str
    predicted_sql: str
    executed_successfully: bool
    execution_error: Optional[str] = None
    matches_ground_truth: Optional[bool] = None
    test_cases_passed: Optional[int] = None
    test_cases_total: Optional[int] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "problem_id": self.problem_id,
            "db_id": self.db_id,
            "query": self.query,
            "error_sql": self.error_sql,
            "predicted_sql": self.predicted_sql,
            "executed_successfully": self.executed_successfully,
            "execution_error": self.execution_error,
            "matches_ground_truth": self.matches_ground_truth,
            "test_cases_passed": self.test_cases_passed,
            "test_cases_total": self.test_cases_total,
        }


@dataclass
class EvaluationSummary:
    """Summary of evaluation results."""

    total_problems: int = 0
    execution_success_count: int = 0
    ground_truth_match_count: int = 0
    test_cases_pass_count: int = 0
    results: list[EvaluationResult] = field(default_factory=list)

    @property
    def execution_success_rate(self) -> float:
        """Percentage of predictions that executed successfully."""
        if self.total_problems == 0:
            return 0.0
        return self.execution_success_count / self.total_problems * 100

    @property
    def ground_truth_match_rate(self) -> float:
        """Percentage of predictions matching ground truth."""
        if self.total_problems == 0:
            return 0.0
        return self.ground_truth_match_count / self.total_problems * 100

    def add_result(self, result: EvaluationResult) -> None:
        """Add an evaluation result to the summary."""
        self.results.append(result)
        self.total_problems += 1

        if result.executed_successfully:
            self.execution_success_count += 1
        if result.matches_ground_truth:
            self.ground_truth_match_count += 1
        if result.test_cases_passed and result.test_cases_total:
            if result.test_cases_passed == result.test_cases_total:
                self.test_cases_pass_count += 1

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_problems": self.total_problems,
            "execution_success_count": self.execution_success_count,
            "execution_success_rate": self.execution_success_rate,
            "ground_truth_match_count": self.ground_truth_match_count,
            "ground_truth_match_rate": self.ground_truth_match_rate,
            "test_cases_pass_count": self.test_cases_pass_count,
            "results": [r.to_dict() for r in self.results],
        }

    def print_summary(self) -> None:
        """Print a summary of the evaluation results."""
        print("\n" + "=" * 60)
        print("EVALUATION SUMMARY")
        print("=" * 60)
        print(f"Total problems evaluated: {self.total_problems}")
        print(
            f"Execution success: {self.execution_success_count}/{self.total_problems} ({self.execution_success_rate:.1f}%)"
        )
        print(
            f"Ground truth match: {self.ground_truth_match_count}/{self.total_problems} ({self.ground_truth_match_rate:.1f}%)"
        )
        print(
            f"Test cases fully passed: {self.test_cases_pass_count}/{self.total_problems}"
        )
        print("=" * 60)


class Evaluator:
    """Evaluator for BIRD-Critic agent predictions."""

    def __init__(
        self,
        agent: BirdCriticAgent,
        db_manager: DatabaseManager,
        output_dir: Optional[str] = None,
    ):
        """Initialize the evaluator.

        Args:
            agent: The agent to evaluate.
            db_manager: Database manager for running queries.
            output_dir: Directory to save evaluation results.
        """
        self.agent = agent
        self.db_manager = db_manager
        self.output_dir = (
            Path(output_dir) if output_dir else Path("./eval_results")
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_problem(
        self, problem: BirdCriticProblem, verbose: bool = False
    ) -> EvaluationResult:
        """Evaluate the agent on a single problem.

        Args:
            problem: The problem to evaluate.
            verbose: Whether to print verbose output.

        Returns:
            EvaluationResult for this problem.
        """
        if verbose:
            print(f"\n{'=' * 50}")
            print(f"Evaluating problem {problem.id}: {problem.db_id}")
            print(f"Query: {problem.query[:100]}...")

        # Connect to the problem's database
        try:
            self.db_manager.connect(problem.db_id)
        except Exception as e:
            error_msg = f"Could not connect to database '{problem.db_id}': {e}"
            print(f"  ✗ Connection error: {error_msg}")
            return EvaluationResult(
                problem_id=problem.id,
                db_id=problem.db_id,
                query=problem.query,
                error_sql=problem.error_sql,
                predicted_sql="",
                executed_successfully=False,
                execution_error=error_msg,
            )

        try:
            # Get the agent's prediction
            predicted_sql = self.agent.solve(problem, verbose=verbose)

            if verbose:
                print(f"Predicted SQL: {predicted_sql[:200]}...")

            # Test if the predicted SQL executes
            success, result, error = self.db_manager.execute_sql(predicted_sql)

            # Check ground truth match if available
            matches_gt = None
            if problem.sol_sql:
                # Normalize and compare (simple string comparison)
                pred_normalized = self._normalize_sql(predicted_sql)
                sol_normalized = self._normalize_sql(problem.sol_sql)
                matches_gt = pred_normalized == sol_normalized

            return EvaluationResult(
                problem_id=problem.id,
                db_id=problem.db_id,
                query=problem.query,
                error_sql=problem.error_sql,
                predicted_sql=predicted_sql,
                executed_successfully=success,
                execution_error=error,
                matches_ground_truth=matches_gt,
            )

        except Exception as e:
            import traceback

            error_msg = f"Agent error: {e}"
            print(f"  ✗ Agent exception for problem {problem.id}: {error_msg}")
            if verbose:
                traceback.print_exc()
            return EvaluationResult(
                problem_id=problem.id,
                db_id=problem.db_id,
                query=problem.query,
                error_sql=problem.error_sql,
                predicted_sql="",
                executed_successfully=False,
                execution_error=error_msg,
            )
        finally:
            self.db_manager.close()

    def evaluate_dataset(
        self,
        problems: list[BirdCriticProblem],
        verbose: bool = False,
        save_results: bool = True,
    ) -> EvaluationSummary:
        """Evaluate the agent on a list of problems.

        Args:
            problems: List of problems to evaluate.
            verbose: Whether to print verbose output.
            save_results: Whether to save results to disk.

        Returns:
            EvaluationSummary with all results.
        """
        summary = EvaluationSummary()

        for i, problem in enumerate(problems):
            print(f"\nProgress: {i + 1}/{len(problems)}")
            result = self.evaluate_problem(problem, verbose=verbose)
            summary.add_result(result)

            if result.executed_successfully:
                print(f"  ✓ [{problem.db_id}] Executed successfully")
            else:
                error_preview = result.execution_error or "Unknown error"
                if len(error_preview) > 200:
                    error_preview = error_preview[:200] + "..."
                print(
                    f"  ✗ [{problem.db_id}] Execution failed: {error_preview}"
                )

        summary.print_summary()

        if save_results:
            self._save_results(summary)

        return summary

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison."""
        import re

        # Remove extra whitespace
        sql = re.sub(r"\s+", " ", sql).strip()
        # Lowercase
        sql = sql.lower()
        # Remove trailing semicolon
        sql = sql.rstrip(";")
        return sql

    def _save_results(self, summary: EvaluationSummary) -> None:
        """Save evaluation results to disk."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"eval_results_{timestamp}.json"

        with open(output_file, "w") as f:
            json.dump(summary.to_dict(), f, indent=2)

        print(f"\nResults saved to: {output_file}")
