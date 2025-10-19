import argparse
import random
from typing import Callable

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node

from dblib import timer
from dblib.dolt import DoltToolSuite
from microbench import sampling
from tasks import DatabaseTask


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def BETA_DIST(sample_size):
    return sampling.beta_distribution(sample_size, alpha=2.0, beta=5.0)


def build_branch_tree(root_branch: str, tree_depth: int, degree: int) -> Node:
    root_node = Node(root_branch)

    current_level_nodes = [root_node]
    for d in range(tree_depth):
        next_level_nodes = []
        for parent_node in current_level_nodes:
            for i in range(degree):
                branch_name = f"branch_d{d + 1}_n{i + 1}"
                child_node = Node(branch_name, parent=parent_node)
                next_level_nodes.append(child_node)
        current_level_nodes = next_level_nodes

    return root_node


class BenchmarkSuite:
    def __init__(
        self,
        backend: str,
        db_name: str = "microbench",
        db_schema_str: str = "",
        db_schema_path: str = "",
        delete_db_after_done: bool = True,
        preload_data_dir: str = "",
    ):
        self.backend = backend
        self._db_name = db_name
        self.delete_db_after_done = delete_db_after_done
        self.preload_data_dir = preload_data_dir
        self.preloaded_tables = set()

        self.timer = timer.Timer()

        timed_tools, regular_tools = None, None
        self.setup_benchmark_database()

        if backend == "dolt":
            # TODO: Consider making these parameters configurable.
            uri = format_db_uri(
                "postgres", "password", "localhost", 5432, self._db_name
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

        # Setup the database and initialize the schema.
        if not db_schema_str and not db_schema_path:
            raise ValueError(
                "Database schema must be provided via string or path."
            )

        if db_schema_str:
            print("Setting up database schema from string...")
            self.db_task.setup_db(self._db_name, db_schema_str)
        elif db_schema_path:
            print("Setting up database schema from file...")
            with open(db_schema_path, "r") as f:
                db_schema_str = f.read()
            self.db_task.setup_db(self._db_name, db_schema_str)

        if self.preload_data_dir:
            print("Preloading data for benchmarks...")
            self.preloaded_tablesself.db_task.preload_db_data(
                self.preload_data_dir
            )

    def __del__(self):
        if self.delete_db_after_done:
            self.db_task.delete_db(self._db_name)
        self.conn.close()

    def setup_benchmark_database(self):
        try:
            # Create a new database over a separate connection.
            uri = format_db_uri(
                "postgres", "password", "localhost", 5432, "postgres"
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

        print("Running read benchmark...")

        benchmark_tables = table_names if table_names else self.preloaded_tables
        for table in benchmark_tables:
            total_rows = self.db_task.get_table_row_count(table)
            print(f"Table {table} has {total_rows} rows.")
            self.db_task.point_read(
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

    def insert_bench(self, num_inserted: int = 100) -> None:
        print("Running insert benchmark...")
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

        print("Running update benchmark...")

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

    def branch_bench(self, tree_depth: int = 10, degree: int = 5) -> None:
        root = build_branch_tree(
            root_branch="main", tree_depth=tree_depth, degree=degree
        )

        print("Running branch benchmark...")
        # pick a random table to do minimal inserts
        all_tables = self.db_task.get_all_tables()
        insert_table = random.choice(all_tables)

        current_level_nodes = [root]
        for node in current_level_nodes:
            self.db_task.connect_branch(node.name, timed=False)
            self.db_task.insert(insert_table, num_rows=2, timed=False)
            for child in node.children:
                self.db_task.create_branch(child.name, timed=True)
            current_level_nodes.extend(node.children)
        print(
            f"Average branch creation time: {self.timer.report_average_time():.6f} seconds\n"
        )
        self.timer.reset()

    def branch_insert_read_bench(self):
        pass

    def branch_update_read_bench(self):
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

    args = parser.parse_args()

    single_task_bench = BenchmarkSuite(
        backend=args.backend,
        db_name="microbench",
        db_schema_path=args.db_schema_path,
        preload_data_dir=args.preload_data_dir,
    )

    # single_task_bench.read_bench()
    # single_task_bench.insert_bench()
    # single_task_bench.update_bench()
    single_task_bench.branch_bench()
