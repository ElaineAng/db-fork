#!/usr/bin/env python3
"""CLI runner for BIRD-Critic SQL debugging agent."""

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description="Run BIRD-Critic SQL debugging agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run on first 10 problems with simple postgres (default)
  python -m agent.bird_critic.run --limit 10

  # Run with Dolt backend (branching enabled)
  python -m agent.bird_critic.run --backend dolt --limit 5

  # Run with Neon backend
  python -m agent.bird_critic.run --backend neon --limit 5

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
        "--backend",
        type=str,
        choices=["postgres", "dolt", "neon", "kpg"],
        default="postgres",
        help="Database backend: postgres (simple SQL), dolt, neon, or kpg (default: postgres)",
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
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Maximum retry attempts when predicted SQL fails (default: 1, no retries)",
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

    # Initialize database interface based on backend
    db_manager = None
    db_tools = None

    if args.backend == "postgres":
        # Simple postgres mode - use DatabaseManager
        print("Using simple PostgreSQL backend (no branching)")
        db_manager = DatabaseManager(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
        )
    else:
        # Branching mode - use DBToolSuite
        print(f"Using {args.backend.upper()} backend (branching enabled)")

        # Import result collector for DBToolSuite
        from dblib.result_collector import ResultCollector

        # Create a result collector (required by DBToolSuite)
        result_collector = ResultCollector(
            run_id=f"bird_critic_{args.backend}",
            output_dir=args.output_dir,
        )

        # Store connection params for later database switching
        conn_params = {
            "host": args.db_host,
            "port": args.db_port,
            "user": args.db_user,
            "password": args.db_password,
        }

        # Default database to connect to initially
        initial_db = "postgres"

        if args.backend == "dolt":
            from dblib.dolt import DoltToolSuite

            db_tools = DoltToolSuite.init_for_bench(
                collector=result_collector,
                db_name=initial_db,
                autocommit=True,
                default_branch_name="main",
            )
            db_tools.commit_changes(timed=False, message="Init dolt tables")
            # Store connection params for database switching
            db_tools._conn_params = conn_params

        elif args.backend == "neon":
            from dblib.neon import NeonToolSuite

            db_tools = NeonToolSuite.init_for_bench(
                collector=result_collector,
                db_name=initial_db,
                autocommit=True,
                default_branch_name="main",
            )
            db_tools._conn_params = conn_params

        elif args.backend == "kpg":
            from dblib.kpg import KpgToolSuite

            db_tools = KpgToolSuite.init_for_bench(
                collector=result_collector,
                db_name=initial_db,
                autocommit=True,
                default_branch_name="main",
            )
            db_tools._conn_params = conn_params

    # Create agent
    print(f"Creating agent with model: {args.model}")
    agent = BirdCriticAgent(
        db_manager=db_manager,
        db_tools=db_tools,
        model_name=args.model,
        max_iterations=args.max_iterations,
    )

    # Create evaluator - use whichever database interface is available
    # If neither is set, create a db_manager as fallback
    if db_manager is None and db_tools is None:
        db_manager = DatabaseManager(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
        )

    evaluator = Evaluator(
        agent=agent,
        db_manager=db_manager,
        db_tools=db_tools,
        output_dir=args.output_dir,
        max_retries=args.max_retries,
        result_collector=result_collector if db_tools else None,
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
