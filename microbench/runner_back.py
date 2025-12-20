import argparse
import random
import sys
from typing import Self, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node, RenderTree

from google.protobuf import text_format
from microbench import task_pb2 as tp

from dblib import result_collector as rc
from dblib.dolt import DoltToolSuite
from dblib.neon import NeonToolSuite
from microbench import mylogger, sampling
from tasks import DatabaseTask


def BETA_DIST(sample_size):
    return sampling.beta_distribution(sample_size, alpha=10, beta=1.0)


def build_branch_tree(
    root_branch: str, tree_depth: int, degree: int
) -> Tuple[Node, int]:
    root_node = Node(root_branch)
    total_branches = 1

    current_level_nodes = [root_node]
    for d in range(tree_depth):
        next_level_nodes = []
        for idx, parent_node in enumerate(current_level_nodes):
            for i in range(degree):
                branch_name = f"branch_d{d + 1}_n{idx * degree + i + 1}"
                total_branches += 1
                child_node = Node(branch_name, parent=parent_node)
                next_level_nodes.append(child_node)
        current_level_nodes = next_level_nodes

    return root_node, total_branches


class BenchmarkSuite:
    def __init__(
        self,
        config: tp.TaskConfig,
    ):
        self._backend = config.backend
        self._db_name = config.db_name
        self._delete_db_after_done = config.delete_db_after_done
        # Whether we need to create and setup the benchmark database. If false,
        # we assume the database is already created and setup.
        self._require_db_setup = config.require_db_setup

        # Use the provided Neon project ID if we are not setting up a new
        # database.
        self._neon_project_id = neon_project_id if not require_db_setup else ""

        self.result_collector = rc.ResultCollector(run_id=run_id)

    def __enter__(self) -> Self:
        db_tools = None
        # NOTE: If self.require_db_setup, create_benchmark_database() must be
        # called before this method returns.
        try:
            if self._backend == "dolt":
                default_uri = DoltToolSuite.get_default_connection_uri()
                print(f"Default Dolt connection URI: {default_uri}")
                self.create_benchmark_database(default_uri)
                db_tools = DoltToolSuite.init_for_bench(
                    self.result_collector, self._db_name
                )
                self.root_branch_name = "main"
            elif self._backend == "neon":
                default_branch_id = ""
                if self.require_db_setup:
                    # If the database hasn't been setup yet, the default neon
                    # uri depends on the created project, so we create the
                    # project first.
                    neon_project = NeonToolSuite.create_neon_project(
                        f"project_{self._db_name}"
                    )
                    self._neon_project_id = neon_project["project"]["id"]
                    print(f"Neon project ID: {self._neon_project_id}")
                    default_uri = (
                        neon_project["connection_uris"][0]["connection_uri"]
                        if neon_project["connection_uris"]
                        else ""
                    )
                    print(f"Default Neon connection URI: {default_uri}")
                    # Create the benchmark database on the root branch.
                    self.create_benchmark_database(default_uri)
                    default_branch_id = neon_project["branch"]["id"]
                    self.root_branch_name = neon_project["branch"]["name"]
                else:
                    # Otherwise we try to get the default branch ID
                    # and name from the specified project.
                    proj_branches = NeonToolSuite.get_project_branches(
                        self._neon_project_id
                    )
                    for branch in proj_branches["branches"]:
                        if branch["default"]:
                            self.root_branch_name = branch["name"]
                            default_branch_id = branch["id"]
                            break

                # Now get the connection uri for the benchmark database.
                print(
                    f"Default Neon branch name: {self.root_branch_name}, ID: {default_branch_id}"
                )
                db_tools = NeonToolSuite.init_for_bench(
                    self.result_collector,
                    self._neon_project_id,
                    default_branch_id,
                    self.root_branch_name,
                    self._db_name,
                )
            else:
                raise ValueError(f"Unsupported backend: {self._backend}")

            self.db_task = DatabaseTask(
                db_tools=db_tools,
            )
            return self
        except Exception as e:
            print(f"Error during BenchmarkSuite setup: {e}")
            if (
                self.delete_db_after_done
                and self._backend == "neon"
                and self._neon_project_id
            ):
                NeonToolSuite.delete_project(self._neon_project_id)
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Exiting BenchmarkSuite context...")

        # Write benchmark results to parquet file
        self.result_collector.write_to_parquet()

        if self.delete_db_after_done:
            try:
                self.db_task.delete_db(self._db_name)
                print("Database deleted successfully.")
            except Exception as e:
                if "database not found" in str(e):  # Database does not exist
                    print(
                        "Database not found. Assuming it was already deleted."
                    )
                else:
                    print(f"Error deleting database: {e}")
            if self._backend == "neon" and self._neon_project_id:
                NeonToolSuite.delete_project(self._neon_project_id)

        self.db_task.close_current_connection()

    def create_benchmark_database(self, uri):
        """
        Creates the benchmark database on the root branch.
        """
        if not self.require_db_setup:
            return
        try:
            # Create a new database over a separate connection.
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
            self.db_task.setup_db(db_schema_str)
        elif db_schema_path:
            print(f"Setting up database schema from file {db_schema_path}...")
            with open(db_schema_path, "r") as f:
                db_schema_str = f.read()
            self.db_task.setup_db(db_schema_str)

        if preload_data_dir:
            print("Preloading data for benchmarks...")
            self.preload_data_dir = preload_data_dir
            self.db_task.preload_db_data(self.preload_data_dir)

    def read_skip_setup(
        self,
        table_name: str = "",
        sampling_args: sampling.SamplingArgs = None,
        branch_name: str = "",
        pk_file: str = "",
    ) -> None:
        if branch_name:
            self.db_task.connect_branch(branch_name, timed=False)
        print(f"Running from branch {self.db_task.get_current_branch()}")

        # Set context for proto collection
        table_schema = self.db_task.db_tools.get_table_schema(table_name)
        self.result_collector.set_context(
            table_name=table_name,
            table_schema=table_schema,
            initial_db_size=self.db_task.get_db_size(),
        )

        self.db_task.point_read(
            table_name,
            sampling_args=sampling_args,
            pk_file=pk_file,
        )

        self.result_collector.reset()

    def _run_update_bench(
        self,
        table_names: list[str] = [],
        sampling_args: sampling.SamplingArgs = None,
        branch_name: str = "",
        pk_file: str = "",
        range_size: int = 0,
    ) -> None:
        """
        Common implementation for update benchmarks.

        Args:
            range_size: If > 0, performs range updates with this size.
                        If 0, performs point updates.
        """
        if branch_name:
            self.db_task.connect_branch(branch_name, timed=False)
        print(f"Running from branch {self.db_task.get_current_branch()}")

        benchmark_tables = (
            table_names if table_names else self.db_task.get_all_tables()
        )
        is_range_update = range_size > 0

        for table in benchmark_tables:
            # Set context for proto collection
            table_schema = self.db_task.db_tools.get_table_schema(table)
            self.result_collector.set_context(
                table_name=table,
                table_schema=table_schema,
                initial_db_size=self.db_task.get_db_size(),
            )

            if is_range_update:
                print(f"\n--- Running range update on table {table} ---\n")
                self.db_task.update_range(
                    table,
                    sampling_args=sampling_args,
                    range_size=range_size,
                    timed=True,
                    pk_file=pk_file,
                )
            else:
                print(f"\n--- Running point update on table {table} ---\n")
                self.db_task.update(
                    table,
                    sampling_args=sampling_args,
                    timed=True,
                    pk_file=pk_file,
                )

            self.result_collector.reset()

    def branch_insert_op(
        self,
        tree_depth: int = 9,
        degree: int = 1,
        insert_per_branch: int = 1,
        table_name: str = "",
        time_branching: bool = False,
        time_inserts: bool = False,
    ) -> str:
        (root, total_branches) = build_branch_tree(
            root_branch=self.root_branch_name,
            tree_depth=tree_depth,
            degree=degree,
        )
        print(RenderTree(root))

        # Choose the table specified by `table_name`, or pick a random table for
        # inserts.
        # Branching is generally a database (or higher) -level operation and
        # shouldn't concern a specific table.
        all_tables = self.db_task.get_all_tables()
        if table_name and table_name in all_tables:
            insert_table = table_name
        else:
            insert_table = random.choice(all_tables)

        # Set context for result proto collection.
        table_schema = self.db_task.db_tools.get_table_schema(insert_table)
        self.result_collector.set_context(
            table_name=insert_table if not time_branching else "",
            table_schema=table_schema if not time_branching else "",
            initial_db_size=self.db_task.get_db_size(),
        )

        current_visited = 0
        current_level_nodes = [root]
        for node in current_level_nodes:
            # We don't time the branch switching for inserts since it's part of
            # setup.
            self.db_task.connect_branch(node.name, timed=time_branching)
            if insert_per_branch > 0:
                self.db_task.insert(
                    insert_table,
                    num_rows=insert_per_branch,
                    timed=time_inserts,
                )
            current_visited += 1
            # TODO: Change this to use tqdm for the for-loop.
            mylogger.log_progress(
                f"Progress:    {current_visited}/{total_branches} branches, "
                f"inserted {insert_per_branch} records each."
            )
            _, cur_branch_id = self.db_task.get_current_branch()
            for child in node.children:
                success = self.db_task.create_branch(
                    child.name, timed=time_branching, parent_id=cur_branch_id
                )
                if success:
                    current_level_nodes.append(child)
        print(
            f"{current_visited} branches created, "
            f"current one for following operations: {node.name}"
        )

        self.result_collector.reset()
        return insert_table

    # =========================================================================
    # Benchmark code below
    # =========================================================================

    # Benchmark that creates a branch tree and measures the time taken to create
    # the branches.
    def branch_bench(
        self, tree_depth: int = 9, degree: int = 1, insert_per_branch: int = 1
    ) -> None:
        print("\n ====== Running branch benchmark...\n", flush=True)

        self.branch_insert_op(
            tree_depth=tree_depth,
            degree=degree,
            insert_per_branch=insert_per_branch,
            time_branching=True,
            time_inserts=False,
        )

    def insert_bench(
        self, num_inserts: int = 100, table_names: list[str] = []
    ) -> None:
        print("\n ====== Running insert benchmark...\n", flush=True)

        tables_to_insert = (
            table_names if table_names else self.db_task.get_all_tables()
        )
        for table in tables_to_insert:
            # Set context for proto collection
            table_schema = self.db_task.db_tools.get_table_schema(table)
            self.result_collector.set_context(
                table_name=table,
                table_schema=table_schema,
                initial_db_size=self.db_task.get_db_size(),
            )

            self.db_task.insert(table, num_rows=num_inserts, timed=True)
            self.result_collector.reset()

    # Benchmark that creates a branch tree and measures the time taken to insert
    # records into the branches.
    def branch_insert_bench(
        self,
        tree_depth: int = 9,
        degree: int = 1,
        insert_per_branch: int = 1,
        table_name: str = "",
    ) -> None:
        print("\n ====== Running branch insert benchmark...\n", flush=True)
        self.branch_insert_op(
            tree_depth=tree_depth,
            degree=degree,
            insert_per_branch=insert_per_branch,
            table_name=table_name,
            time_branching=False,
            time_inserts=True,
        )

    # Benchmark that creates a branch tree, inserts records into the branches,
    # and measures the time taken to read the records.
    def branch_insert_read_bench(
        self,
        sampling_args: sampling.SamplingArgs = None,
        tree_depth: int = 9,
        degree: int = 1,
        insert_per_branch: int = 1,
        intended_table_name: str = "",
    ) -> None:
        final_table_name = self.branch_insert_op(
            tree_depth=tree_depth,
            degree=degree,
            insert_per_branch=insert_per_branch,
            table_name=intended_table_name,
        )
        self.read_skip_setup(final_table_name, sampling_args)

    # Benchmark that measures the time taken to update a single record in
    # table(s) specified by `table_names`.
    def update_bench(
        self,
        table_names: list[str] = [],
        sampling_args: sampling.SamplingArgs = None,
        branch_name: str = "",
        pk_file: str = "",
    ) -> None:
        self._run_update_bench(
            table_names=table_names,
            sampling_args=sampling_args,
            branch_name=branch_name,
            pk_file=pk_file,
            range_size=0,
        )

    # Benchmark that measures the time taken to update a range of records in
    # table(s) specified by `table_names`.
    def range_update_bench(
        self,
        table_names: list[str] = [],
        sampling_args: sampling.SamplingArgs = None,
        branch_name: str = "",
        pk_file: str = "",
        range_size: int = 20,
    ) -> None:
        self._run_update_bench(
            table_names=table_names,
            sampling_args=sampling_args,
            branch_name=branch_name,
            pk_file=pk_file,
            range_size=range_size,
        )


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Run database benchmarks from config file."
    )

    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the task configuration file (textproto format).",
    )
    args = parser.parse_args()

    # Load and parse the textproto config file
    try:
        config = tp.TaskConfig()
        with open(args.config, "r") as f:
            text_format.Parse(f.read(), config)

        print(f"Loaded configuration from {args.config}")
        print(f"Run ID: {config.run_id}")
        print(f"Backend: {tp.TaskConfig.Backend.Name(config.backend)}")
        print(
            f"Operation: {tp.TaskConfig.OperationType.Name(config.operation_type)}"
        )

    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing config file: {e}")
        sys.exit(1)

    # TODO: Consider init BenchmarkSuite with just `config` and has a cleaner
    # `Init()` method.

    # Decide when we need to setup the database.
    require_db_setup = not (args.read_no_setup or args.reuse_existing_db)
    if args.backend == "neon":
        assert require_db_setup or args.neon_project_id, (
            "When reusing existing Neon database, --neon_project_id "
            "must be provided."
        )
    # Decide whether we need to cleanup database after benchmark.
    delete_db_after_done = not (
        args.no_cleanup or args.reuse_existing_db or args.preload_only
    )

    with BenchmarkSuite(
        backend=args.backend,
        db_name="microbench",
        # If we preload data, we most certainly want to keep the database.
        delete_db_after_done=delete_db_after_done,
        # Specifying read_no_setup means the DB to be read has already been setup.
        require_db_setup=require_db_setup,
        neon_project_id=args.neon_project_id,
        run_id=args.run_id,
    ) as single_task_bench:
        table_names = args.table_names.split(",") if args.table_names else []
        if require_db_setup:
            single_task_bench.setup_benchmark_database(
                db_schema_path=args.db_schema_path,
                preload_data_dir=args.preload_data_dir
                if args.preload_data_dir
                else "",
            )
        if args.branch_only:
            single_task_bench.branch_bench(
                tree_depth=args.branch_depth,
                degree=args.branch_degree,
                insert_per_branch=args.num_inserts or 0,
            )
        elif args.insert_only:
            single_task_bench.insert_bench(
                num_inserts=args.num_inserts or 100,
                table_name=table_names,
            )

        elif args.update_only or args.range_update_only:
            sampling_args = sampling.SamplingArgs(
                sampling_rate=args.sampling_rate,
                max_sampling_size=args.max_sample_size,
                distribution=lambda size: sampling.beta_distribution(
                    size,
                    alpha=args.alpha if args.alpha else 10,
                    beta=args.beta if args.beta else 1,
                ),
                sort_idx=args.sort_idx or -1,
            )
            if args.range_update_only:
                single_task_bench.range_update_bench(
                    table_names=table_names,
                    sampling_args=sampling_args,
                    branch_name=args.branch_name if args.branch_name else "",
                    pk_file=args.pk_file,
                    range_size=args.range_size,
                )
            else:
                single_task_bench.update_bench(
                    table_names=table_names,
                    sampling_args=sampling_args,
                    branch_name=args.branch_name if args.branch_name else "",
                    pk_file=args.pk_file,
                )

        elif args.branch_insert:
            single_task_bench.branch_insert_bench(
                tree_depth=args.branch_depth,
                degree=args.branch_degree,
                insert_per_branch=args.num_inserts or 100,
                table_name=table_names,
            )

        elif args.branch_insert_read:
            sampling_args = sampling.SamplingArgs(
                sampling_rate=args.sampling_rate,
                max_sampling_size=args.max_sample_size,
                distribution=lambda size: sampling.beta_distribution(
                    size,
                    alpha=args.alpha if args.alpha else 10,
                    beta=args.beta if args.beta else 1.0,
                ),
                sort_idx=args.sort_idx or -1,
            )
            single_task_bench.branch_insert_read_bench(
                sampling_args=sampling_args,
                tree_depth=args.branch_depth,
                degree=args.branch_degree,
                insert_per_branch=args.num_inserts or 100,
                intended_table_name=table_names,
            )
        elif args.read_no_setup:
            sampling_args = sampling.SamplingArgs(
                sampling_rate=args.sampling_rate,
                max_sampling_size=args.max_sample_size,
                distribution=lambda size: sampling.beta_distribution(
                    size,
                    alpha=args.alpha if args.alpha else 10,
                    beta=args.beta if args.beta else 1.0,
                ),
                sort_idx=args.sort_idx or -1,
            )
            single_task_bench.read_skip_setup(
                table_name=table_names,
                sampling_args=sampling_args,
                branch_name=args.branch_name if args.branch_name else "",
                pk_file=args.pk_file,
            )
