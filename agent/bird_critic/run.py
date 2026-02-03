#!/usr/bin/env python3
"""CLI runner for BIRD-Critic SQL debugging agent."""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Run BIRD-Critic SQL debugging agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run on first 10 problems
  python -m agent.bird_critic.run --limit 10

  # Run on specific problem indices
  python -m agent.bird_critic.run --indices 0 5 10

  # Run with verbose output
  python -m agent.bird_critic.run --limit 5 --verbose

  # Use a specific model
  python -m agent.bird_critic.run --model gemini-2.0-flash --limit 5
""",
    )

    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o",
        help="LLM model to use (default: gpt-4o)",
    )
    parser.add_argument(
        "--split",
        type=str,
        default="pg",
        help="Dataset split to use (default: pg)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of problems to evaluate",
    )
    parser.add_argument(
        "--indices",
        type=int,
        nargs="+",
        default=None,
        help="Specific problem indices to evaluate",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default="localhost",
        help="PostgreSQL host (default: localhost)",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=5433,
        help="PostgreSQL port (default: 5433)",
    )
    parser.add_argument(
        "--db-user",
        type=str,
        default="elaineang",
        help="PostgreSQL user (default: elaineang)",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        default="",
        help="PostgreSQL password",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./eval_results",
        help="Directory to save evaluation results",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save evaluation results to disk",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=15,
        help="Maximum agent iterations per problem (default: 15)",
    )

    args = parser.parse_args()

    # Import here to avoid slow imports during --help
    from .agent import BirdCriticAgent
    from .data_loader import BirdCriticDataset
    from .db_utils import DatabaseManager
    from .evaluate import Evaluator

    # Load dataset
    print(f"Loading BIRD-Critic dataset (split: {args.split})...")
    dataset = BirdCriticDataset(split=args.split)
    problems = dataset.load()

    if not problems:
        print("Error: No problems loaded from dataset")
        sys.exit(1)

    # Filter problems
    if args.indices:
        problems = [problems[i] for i in args.indices if i < len(problems)]
        print(f"Selected {len(problems)} problems by index")
    elif args.limit:
        problems = problems[: args.limit]
        print(f"Limited to first {len(problems)} problems")

    # Create database manager
    db_manager = DatabaseManager(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
    )

    # Create agent
    print(f"Creating agent with model: {args.model}")
    agent = BirdCriticAgent(
        db_manager=db_manager,
        model_name=args.model,
        max_iterations=args.max_iterations,
    )

    # Create evaluator
    evaluator = Evaluator(
        agent=agent,
        db_manager=db_manager,
        output_dir=args.output_dir,
    )

    # Run evaluation
    print(f"\nStarting evaluation on {len(problems)} problems...")
    summary = evaluator.evaluate_dataset(
        problems=problems,
        verbose=args.verbose,
        save_results=not args.no_save,
    )

    # Print final summary
    summary.print_summary()

    # Return exit code based on success rate
    if summary.execution_success_rate >= 50:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
