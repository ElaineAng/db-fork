"""Evaluation utilities for BIRD-Critic agent predictions."""

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

from .agent import BirdCriticAgent
from .data_loader import BirdCriticProblem
from .db_utils import DatabaseManager

# Import DBToolSuite for branching operations
from dblib.db_api import DBToolSuite
from dblib.result_collector import ResultCollector


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
            f"Execution success: {self.execution_success_count}/{self.total_problems} "
            f"({self.execution_success_rate:.1f}%)"
        )
        print(
            f"Ground truth match: {self.ground_truth_match_count}/{self.total_problems} "
            f"({self.ground_truth_match_rate:.1f}%)"
        )
        print("=" * 60)


class Evaluator:
    """Evaluator for BIRD-Critic agent predictions."""

    def __init__(
        self,
        agent: BirdCriticAgent,
        db_manager: DatabaseManager = None,
        db_tools: DBToolSuite = None,
        output_dir: Optional[str] = None,
        max_retries: int = 1,
        result_collector: ResultCollector = None,
    ):
        """Initialize the evaluator.

        Args:
            agent: The agent to evaluate.
            db_manager: Database manager for running queries (optional).
            db_tools: DBToolSuite for branching operations (optional).
                      At least one of db_manager or db_tools should be provided.
            output_dir: Directory to save evaluation results.
            max_retries: Maximum number of attempts per problem (default: 1, no retries).
            result_collector: Optional ResultCollector for collecting timing metrics.
        """
        if not db_manager and not db_tools:
            raise ValueError(
                "At least one of db_manager or db_tools must be provided"
            )

        self.agent = agent
        self.db_manager = db_manager
        self.db_tools = db_tools
        self.max_retries = max_retries
        self.result_collector = result_collector
        self.output_dir = (
            Path(output_dir) if output_dir else Path("./eval_results")
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _connect_database(self, db_id: str) -> Tuple[bool, Optional[str]]:
        """Connect to a database using available interface.

        Returns:
            Tuple of (success, error_message).
        """
        try:
            if self.db_manager:
                self.db_manager.connect(db_id)
            elif self.db_tools:
                # For DBToolSuite, we need to reconnect with the new database
                import psycopg2

                if hasattr(self.db_tools, "_conn_params"):
                    params = self.db_tools._conn_params.copy()
                    params["database"] = db_id
                    self.db_tools.conn = psycopg2.connect(**params)
                    self.db_tools.conn.autocommit = True

                    # Ensure we're on main branch and commit any pending changes
                    # This makes tables visible to newly created branches
                    try:
                        self.db_tools.connect_branch("main", timed=False)
                        self.db_tools.commit_changes(
                            timed=False, message=f"Init {db_id} tables"
                        )
                    except Exception:
                        pass  # Ignore if already committed or branch doesn't exist
            return True, None
        except Exception as e:
            return False, str(e)

    def _execute_sql(self, sql: str) -> Tuple[bool, Any, Optional[str]]:
        """Execute SQL using available interface.

        Returns:
            Tuple of (success, result, error_message).
        """
        try:
            if self.db_manager:
                return self.db_manager.execute_sql(sql)
            elif self.db_tools:
                result = self.db_tools.execute_sql(sql, timed=False)
                return True, result, None
        except Exception as e:
            return False, None, str(e)

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
        success, error = self._connect_database(problem.db_id)
        if not success:
            error_msg = (
                f"Could not connect to database '{problem.db_id}': {error}"
            )
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
            # Retry loop - give agent multiple attempts to fix the SQL
            last_error = None
            predicted_sql = ""

            for attempt in range(1, self.max_retries + 1):
                if attempt > 1:
                    print(
                        f"    Retry {attempt}/{self.max_retries}: Re-invoking agent with error feedback"
                    )
                    # Feed the error back to the agent
                    problem_with_error = BirdCriticProblem(
                        id=problem.id,
                        db_id=problem.db_id,
                        query=problem.query
                        + f"\n\n[Previous attempt failed with error: {last_error}]",
                        error_sql=predicted_sql,
                        sol_sql=problem.sol_sql,
                        preprocess_sql=problem.preprocess_sql,
                        clean_up_sql=problem.clean_up_sql,
                    )
                    predicted_sql = self.agent.solve(
                        problem_with_error, verbose=verbose
                    )
                else:
                    predicted_sql = self.agent.solve(problem, verbose=verbose)

                if verbose:
                    print(f"Predicted SQL: {predicted_sql[:200]}...")

                # Test if the predicted SQL executes
                success, result, error = self._execute_sql(predicted_sql)

                if success:
                    # Check ground truth match if available
                    matches_gt = None
                    if problem.sol_sql:
                        pred_normalized = self._normalize_sql(predicted_sql)
                        sol_normalized = self._normalize_sql(problem.sol_sql)
                        matches_gt = pred_normalized == sol_normalized

                    return EvaluationResult(
                        problem_id=problem.id,
                        db_id=problem.db_id,
                        query=problem.query,
                        error_sql=problem.error_sql,
                        predicted_sql=predicted_sql,
                        executed_successfully=True,
                        execution_error=None,
                        matches_ground_truth=matches_gt,
                    )
                else:
                    last_error = error
                    print(
                        f"    Attempt {attempt} failed: {error[:100] if error else 'Unknown error'}..."
                    )

            # All retries exhausted
            return EvaluationResult(
                problem_id=problem.id,
                db_id=problem.db_id,
                query=problem.query,
                error_sql=problem.error_sql,
                predicted_sql=predicted_sql,
                executed_successfully=False,
                execution_error=last_error,
                matches_ground_truth=None,
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
            print(f"\n{'=' * 60}")
            print(f"Problem {i + 1}/{len(problems)}: {problem.id}")
            print(f"  Database: {problem.db_id}")
            # Show query (truncate if too long)
            query_preview = problem.query.replace("\n", " ")
            if len(query_preview) > 150:
                query_preview = query_preview[:150] + "..."
            print(f"  Query: {query_preview}")
            # # Show buggy SQL (truncate if too long)
            # sql_preview = problem.error_sql.replace("\n", " ")
            # if len(sql_preview) > 150:
            #     sql_preview = sql_preview[:150] + "..."
            print(f"  Buggy SQL: {problem.error_sql}")
            print(f"{'=' * 60}")

            # Set context for result collector if using DBToolSuite
            if self.result_collector and self.db_tools:
                self.result_collector.set_context(
                    table_name=problem.db_id,
                    table_schema="",
                    initial_db_size=-1,
                    seed=-1,
                )

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

        if save_results:
            self._save_results(summary)

        # Write result collector data to parquet if available
        if self.result_collector and self.db_tools:
            self.result_collector.write_to_parquet()

        return summary

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison."""
        if not sql:
            return ""
        # Basic normalization: lowercase, remove extra whitespace
        normalized = " ".join(sql.lower().split())
        # Remove trailing semicolon
        if normalized.endswith(";"):
            normalized = normalized[:-1]
        return normalized

    def _save_results(self, summary: EvaluationSummary) -> None:
        """Save evaluation results to disk."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"eval_{timestamp}.json"
        filepath = self.output_dir / filename

        with open(filepath, "w") as f:
            json.dump(summary.to_dict(), f, indent=2)

        print(f"\nResults saved to: {filepath}")
