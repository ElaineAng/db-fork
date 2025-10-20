import argparse
import random
from typing import Callable

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node, RenderTree

from dblib import timer
from dblib.dolt import DoltToolSuite
from microbench import mylogger, sampling
from tasks import DatabaseTask


PG_USER = "postgres"
PG_PASSWORD = "password"
PG_HOST = "localhost"
PG_PORT = 5432


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def BETA_DIST(sample_size):
    return sampling.beta_distribution(sample_size, alpha=10, beta=1.0)


def build_branch_tree(root_branch: str, tree_depth: int, degree: int) -> Node:
    root_node = Node(root_branch)

    current_level_nodes = [root_node]
    for d in range(tree_depth):
        next_level_nodes = []
        for idx, parent_node in enumerate(current_level_nodes):
            for i in range(degree):
                branch_name = f"branch_d{d + 1}_n{idx * degree + i + 1}"
                child_node = Node(branch_name, parent=parent_node)
                next_level_nodes.append(child_node)
        current_level_nodes = next_level_nodes

    return root_node


class BenchmarkSuite:
    def __init__(
        self,
        backend: str,
        db_name: str = "microbench",
        delete_db_after_done: bool = True,
        require_setup: bool = True,
    ):
        self.backend = backend
        self._db_name = db_name
        self.delete_db_after_done = delete_db_after_done
        self.require_setup = require_setup
        self.preload_data_dir = ""
        self.preloaded_tables = set()

        self.timer = timer.Timer()

        self.create_benchmark_database()

    def __enter__(self):
        if self.backend == "dolt":
            # TODO: Consider making these parameters configurable.
            uri = format_db_uri(
                PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, self._db_name
            )

        # Timed connection and tools to measure timing.
        self.conn = psycopg2.connect(uri)
        timed_tools = DoltToolSuite(
            connection=self.conn,
            timed_cursor=lambda *args, **kwargs: timer.TimerCursor(
                *args, **kwargs, timer=self.timer
            ),
        )

        self.db_task = DatabaseTask(
            db_tools=timed_tools,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.delete_db_after_done:
            try:
                self.db_task.delete_db(self._db_name)
            except Exception as e:
                if "database not found" in str(e):  # Database does not exist
                    print("DB deleted successfully.")
                else:
                    print(f"Error deleting database: {e}")
        self.conn.close()

    def create_benchmark_database(self):
        if not self.require_setup:
            return
        try:
            # Create a new database over a separate connection.
            uri = format_db_uri(
                PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, "postgres"
            )
            conn = psycopg2.connect(uri)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cur = conn.cursor()
            create_db_command = f"CREATE DATABASE {self._db_name};"
            try:
                cur.execute(create_db_command)
                print("Database created successfully.")
            except psycopg2.errors.DuplicateDatabase:
                print(f"Database '{self._db_name}' already exists.")
        except Exception as e:
            print(f"Error creating database: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def setup_benchmark_database(
        self,
        db_schema_str: str = "",
        db_schema_path: str = "",
        preload_data_dir: str = "",
    ) -> None:
        # Setup the database and initialize the schema.
        if not db_schema_str and not db_schema_path:
            raise ValueError(
                "Database schema must be provided via string or path."
            )

        if db_schema_str:
            print(f"Setting up database schema from string {db_schema_str}...")
            self.db_task.setup_db(self._db_name, db_schema_str)
        elif db_schema_path:
            print(f"Setting up database schema from file {db_schema_path}...")
            with open(db_schema_path, "r") as f:
                db_schema_str = f.read()
            self.db_task.setup_db(self._db_name, db_schema_str)

        if preload_data_dir:
            print("Preloading data for benchmarks...")
            self.preload_data_dir = preload_data_dir
            self.db_task.preload_db_data(self.preload_data_dir)

    def read_skip_setup(
        self,
        table_name: str = "",
        sampling_rate: float = 0.01,
        max_sample_size: int = 500,
        dist_lambda: Callable[..., list[float]] = BETA_DIST,
        sort_idx: int = 0,
        branch_name: str = "",
    ) -> None:
        if branch_name:
            self.db_task.connect_branch(branch_name, timed=False)
        total_rows = self.db_task.get_table_row_count(table_name)
        print(f"Table {table_name} has {total_rows} rows.")
        self.db_task.point_read(
            table_name,
            sampling_rate=sampling_rate,
            max_sampling_size=max_sample_size,
            dist_lambda=dist_lambda,
            sort_idx=sort_idx,
        )
        print(
            f"Average read time for table {table_name}: "
            f"{self.timer.report_average_time():.6f} seconds\n"
        )
        self.timer.reset()

    def read_bench(
        self,
        table_names: list[str] = [],
        sampling_rate: float = 0.01,
        dist_lambda: Callable[..., list[float]] = BETA_DIST,
        sort_idx: int = 0,
        max_sample_size: int = 500,
    ) -> None:
        # Simple read bench requires pre-loaded data.
        if not self.preload_data_dir:
            return

        print("\n ====== Running read benchmark...\n", flush=True)

        benchmark_tables = table_names if table_names else self.preloaded_tables
        for table in benchmark_tables:
            self.read_skip_setup(
                table,
                sampling_rate,
                max_sample_size,
                dist_lambda,
                sort_idx,
            )

    def insert_bench(self, num_inserted: int = 100) -> None:
        print("\n ====== Running insert benchmark...\n", flush=True)
        for table in self.db_task.get_all_tables():
            self.db_task.insert(table, num_rows=num_inserted)
            print(
                f"Average insert time for table {table}: "
                f"{self.timer.report_average_time():.6f} seconds\n"
            )
            self.timer.reset()

    def update_bench(
        self,
        table_names: list[str] = [],
        sampling_rate: float = 0.01,
        dist_lambda: Callable[..., list[float]] = BETA_DIST,
        sort_idx: int = 0,
        max_sample_size: int = 500,
    ) -> None:
        # Simple update bench requires pre-loaded data.
        if not self.preload_data_dir:
            return

        print("\n ====== Running update benchmark...\n", flush=True)

        benchmark_tables = table_names if table_names else self.preloaded_tables
        for table in benchmark_tables:
            total_rows = self.db_task.get_table_row_count(table)
            print(f"Table {table} has {total_rows} rows.")
            self.db_task.update(
                table,
                sampling_rate=sampling_rate,
                max_sampling_size=max_sample_size,
                dist_lambda=dist_lambda,
                sort_idx=sort_idx,
            )
            print(
                f"Average read time for table {table}: "
                f"{self.timer.report_average_time():.6f} seconds\n"
            )
            self.timer.reset()

    def branch_insert_op(
        self,
        tree_depth: int = 10,
        degree: int = 2,
        insert_per_branch: int = 1,
        time_branching: bool = False,
        time_inserts: bool = False,
    ) -> str:
        root = build_branch_tree(
            root_branch="main", tree_depth=tree_depth, degree=degree
        )
        # print(RenderTree(root))

        # pick a random table to do minimal inserts
        all_tables = self.db_task.get_all_tables()
        insert_table = random.choice(all_tables)
        total_branches = degree ** (tree_depth + 1) - 1 // (degree - 1)
        current_inserted = 0
        current_level_nodes = [root]
        for node in current_level_nodes:
            self.db_task.connect_branch(node.name, timed=False)
            self.db_task.insert(
                insert_table, num_rows=insert_per_branch, timed=time_inserts
            )
            current_inserted += 1
            mylogger.log_progress(
                f"Progress:    {current_inserted}/{total_branches} branches "
            )
            for child in node.children:
                self.db_task.create_branch(child.name, timed=time_branching)
            current_level_nodes.extend(node.children)
        print(f"branch for the read: {node.name}")

        if time_branching:
            print(
                f"Average branch creation time: {self.timer.report_average_time():.6f} seconds\n"
            )
        if time_inserts:
            print(
                f"Average insertion time: {self.timer.report_average_time():.6f} seconds\n"
            )
        self.timer.reset()
        return insert_table

    def branch_bench(self, tree_depth: int = 10, degree: int = 2) -> None:
        print("\n ====== Running branch benchmark...\n", flush=True)
        self.branch_insert_op(
            tree_depth=tree_depth, degree=degree, time_branching=True
        )

    def branch_insert_bench(
        self, tree_depth: int = 10, degree: int = 2, insert_per_branch: int = 10
    ) -> None:
        print("\n ====== Running branch insert benchmark...\n", flush=True)
        self.branch_insert_op(
            tree_depth=tree_depth,
            degree=degree,
            insert_per_branch=insert_per_branch,
            time_inserts=True,
        )

    def branch_insert_read_bench(
        self,
        sampling_rate: float = 0.01,
        max_sample_size: int = 500,
        dist_lambda: Callable[..., list[float]] = BETA_DIST,
        sort_idx: int = 0,
        tree_depth: int = 10,
        degree: int = 2,
        insert_per_branch: int = 10,
    ) -> None:
        table_name = self.branch_insert_op(
            tree_depth=tree_depth,
            degree=degree,
            insert_per_branch=insert_per_branch,
        )
        self.read_skip_setup(
            table_name,
            sampling_rate,
            max_sample_size,
            dist_lambda,
            sort_idx,
        )

    def branch_update_bench(self):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run database benchmarks.")
    parser.add_argument(
        "--backend",
        default="dolt",
        choices=["dolt"],
        help="The database backend to use.",
    )

    parser.add_argument(
        "--db_schema_path",
        default="db_setup/tpcc_schema.sql",
        help="Path to the database schema SQL file.",
    )

    parser.add_argument(
        "--preload_data_dir",
        default="/tmp/db-fork/",
        help="Path to the directory with data files to preload.",
    )

    parser.add_argument(
        "--no_cleanup",
        action="store_true",
        help="Keep the database after benchmarks are done.",
    )

    parser.add_argument(
        "--branch_only",
        action="store_true",
        help="Only run the branch benchmark.",
    )

    parser.add_argument(
        "--read_only",
        action="store_true",
        help="Only run the read benchmark after preloading data.",
    )

    parser.add_argument(
        "--insert_only",
        action="store_true",
        help="Only run the insert benchmark.",
    )

    parser.add_argument(
        "--branch_insert",
        action="store_true",
        help="Only run the branch insert benchmark.",
    )

    parser.add_argument(
        "--branch_insert_read",
        action="store_true",
        help="Run branch insert followed by read benchmark.",
    )

    parser.add_argument(
        "--read_no_setup",
        action="store_true",
        help=(
            "Run read benchmark without preloading data. This assumes data is "
            "already present. --table_name must be provided."
        ),
    )

    parser.add_argument(
        "--table_name",
        type=str,
        help="Name of the table to run the read benchmark on.",
    )

    parser.add_argument(
        "--alpha",
        type=float,
        help="Alpha parameter for the read predicate's distribution.",
    )

    parser.add_argument(
        "--beta",
        type=float,
        help="Beta parameter for the read predicate's distribution.",
    )

    parser.add_argument(
        "--branch_name",
        type=str,
        help="Name of the branch to connect to for the ops.",
    )

    args = parser.parse_args()

    with BenchmarkSuite(
        backend=args.backend,
        db_name="microbench",
        delete_db_after_done=not args.no_cleanup,
        require_setup=not args.read_no_setup,
    ) as single_task_bench:
        if not args.read_no_setup:
            single_task_bench.setup_benchmark_database(
                db_schema_path=args.db_schema_path
            )
        if args.branch_only:
            single_task_bench.branch_bench(200, 1)
        elif args.insert_only:
            single_task_bench.insert_bench(num_inserted=1000)
        elif args.branch_insert:
            single_task_bench.branch_insert_bench(
                tree_depth=10, degree=2, insert_per_branch=100
            )
        elif args.branch_insert_read:
            single_task_bench.branch_insert_read_bench(
                sampling_rate=0.05,
                max_sample_size=100,
                tree_depth=5,
                degree=2,
                insert_per_branch=1000,
            )
        elif args.read_no_setup:
            single_task_bench.read_skip_setup(
                table_name=args.table_name,
                sampling_rate=0.5,
                max_sample_size=100,
                dist_lambda=lambda size: sampling.beta_distribution(
                    size,
                    alpha=args.alpha if args.alpha else 10,
                    beta=args.beta if args.beta else 1.0,
                ),
            )
