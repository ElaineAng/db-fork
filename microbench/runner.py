from util.import_db import load_sql_file
import argparse
import random
import sys
import time
from typing import Self, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from anytree import Node

from google.protobuf import text_format
from microbench import task_pb2 as tp
from microbench.datagen import DynamicDataGenerator
from util import db_helper as dbh

from dblib import result_collector as rc
from dblib.dolt import DoltToolSuite
from dblib.neon import NeonToolSuite
from microbench import sampling


def BETA_DIST(sample_size):
    return sampling.beta_distribution(sample_size, alpha=10, beta=1.0)


def OPS_WEIGHT(op_type: tp.OperationType):
    if op_type == tp.OperationType.BRANCH:
        return 1
    else:
        return 5


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


def validate_config(config: tp.TaskConfig):
    if config.backend == tp.TaskConfig.Backend.NEON:
        source = config.database_setup.source
        assert source.sql_dump or source.existing_db.neon_project_id, (
            "When reusing existing Neon database, neon_project_id "
            "must be provided."
        )


class BenchmarkSuite:
    def __init__(
        self,
        config: tp.TaskConfig,
    ):
        self._db_name = config.database_setup.db_name
        self._config = config
        self._require_db_setup = (
            config.database_setup.WhichOneof("source") == "sql_dump"
        )

        # Mapping between table name and data generator.
        self._table_datagen = None

        # List of all branches created.
        self._all_branches = []

        # Cached keys to read from.

        # Mapping from branch ID to list of modified keys by this benchmark.
        self._modified_keys = {}

        # List of existing primary keys in the database for the current branch.
        self._existing_pks = []

    def __enter__(self) -> Self:
        db_tools = None
        # NOTE: If self.require_db_setup, create_benchmark_database() must be
        # called before this method returns.
        result_collector = rc.ResultCollector(run_id=config.run_id)
        try:
            if self._config.backend == "dolt":
                default_uri = DoltToolSuite.get_default_connection_uri()
                print(f"Default Dolt connection URI: {default_uri}")
                self.create_benchmark_database(default_uri)
                db_tools = DoltToolSuite.init_for_bench(
                    result_collector, self._db_name
                )
                self._root_branch_name = "main"
            elif self._config.backend == "neon":
                default_branch_id = ""
                if self._require_db_setup:
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
                    self._root_branch_name = neon_project["branch"]["name"]
                else:
                    # Otherwise we try to get the default branch ID
                    # and name from the specified project in the config.
                    self._neon_project_id = self._config.database_setup.source.existing_db.neon_project_id
                    proj_branches = NeonToolSuite.get_project_branches(
                        self._neon_project_id
                    )
                    for branch in proj_branches["branches"]:
                        if branch["default"]:
                            self._root_branch_name = branch["name"]
                            default_branch_id = branch["id"]
                            break

                # Now get the connection uri for the benchmark database.
                print(
                    f"Default Neon branch name: {self._root_branch_name}, ID: {default_branch_id}"
                )
                self._all_branches.append(self._root_branch_name)
                db_tools = NeonToolSuite.init_for_bench(
                    result_collector,
                    self._neon_project_id,
                    default_branch_id,
                    self._root_branch_name,
                    self._db_name,
                )
            else:
                raise ValueError(f"Unsupported backend: {self._config.backend}")

            self.db_tools = db_tools
            return self
        except Exception as e:
            print(f"Error during BenchmarkSuite setup: {e}")
            if (
                self._config.delete_db_after_done
                and self._config.backend == "neon"
                and self._neon_project_id
            ):
                NeonToolSuite.delete_project(self._neon_project_id)
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Exiting BenchmarkSuite context...")

        # Write benchmark results to parquet file
        self.db_tools.result_collector.write_to_parquet()

        if self._config.delete_db_after_done:
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
            if self._config.backend == "neon" and self._neon_project_id:
                NeonToolSuite.delete_project(self._neon_project_id)

        self.db_task.close_current_connection()

    def create_benchmark_database(self, uri):
        """
        Creates the benchmark database on the root branch.
        """
        if not self._require_db_setup:
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

    def maybe_setup_db(self) -> None:
        # Setup the database and initialize the schema.
        if not self._require_db_setup:
            return
        load_sql_file(self.db_tools.get_current_connection(), self._db_name)

    def maybe_branch_and_reconnect(self, next_bid, rnd) -> bool:
        cur_name, cur_id = self.db_tools.get_current_branch()

        next_branch_name = f"branch_{next_bid}"
        created = self.db_tools.create_branch(
            branch_name=next_branch_name, parent_branch_id=cur_id
        )
        if created:
            self._all_branches.append(next_branch_name)

            # Toss a fair coin to connect to the new branch, or stay on the
            # current branch.
            if rnd.random() < 0.5:
                self.db_tools.connect_branch(next_branch_name)
                # clear existing pks cache if switing to a different branch.
                self._existing_pks = []
        else:
            to_connect = random.choice(self._all_branches)
            self.db_tools.connect_branch(to_connect)
            # clear existing pks cache if switing to a different branch.
            self._existing_pks = []
        return created

    def read(self, rnd):
        _, cur_branch_id = self.db_tools.get_current_branch()

        # Get a random key to read.
        key_to_read = None
        pk_columns = dbh.get_pk_column_names(
            self.db_tools.get_current_connection(), self._db_name
        )
        existing_pks = self._existing_pks or dbh.get_pk_values(
            self.db_tools.get_current_connection(),
            self._db_name,
            pk_columns,
        )
        if not existing_pks or (
            self._modified_keys.get(cur_branch_id) and rnd.random() < 0.5
        ):
            key_to_read = random.choice(self._modified_keys[cur_branch_id])
        else:
            key_to_read = random.choice(existing_pks)

        if not key_to_read:
            print("No existing keys found, do nothing")
            return

        # Build the SQL query to read the key.
        where_clause = " AND ".join(
            [f"{pk_name} = %s" for pk_name in pk_columns]
        )
        select_sql = f"SELECT * FROM {self._table_name} WHERE {where_clause};"

        # Read only touches a single key. We might be able to set this in
        # execute_sql() but doing it here is easier.
        self.db_tools.result_collector.record_num_keys_touched(1)

        # Run the read.
        self.db_tools.execute_sql(select_sql, key_to_read, timed=True)

    def insert(self) -> bool:
        _, cur_branch_id = self.db_tools.get_current_branch()

        col_names = dbh.get_all_columns(self._table_name)
        placeholders = ", ".join([f"%({name})s" for name in col_names])
        insert_sql = f"INSERT INTO {self._table_name} ({', '.join(col_names)}) VALUES ({placeholders});"

        pk_columns = dbh.get_pk_column_names(
            self.db_tools.get_current_connection(), self._table_name
        )
        inserted = False

        # Pre-record the number of keys for this op.
        self.db_tools.result_collector.record_num_keys_touched(1)
        for _ in range(5):
            # Generate a new row. Note that this is using a data generator that
            # isn't initialized with the current seed. But this should be fine
            # since we shouldn't care about the exact values inserted.
            row_data = self._table_datagen.generate_row()
            pk_tuple = tuple(row_data[pk] for pk in pk_columns)

            # Try to insert it, it may fail for PK collision.
            try:
                self.db_tools.execute_sql(insert_sql, row_data, timed=True)
                self._modified_keys.setdefault(cur_branch_id, []).append(
                    pk_tuple
                )
                inserted = True
                break
            except Exception:
                continue
        return inserted

    def update(self):
        pass

    def range_update(self):
        pass

    def run_benchmark(self):
        # Get the benchmark table and load the data generator for the table.
        benchmark_table = self._config.table_name
        if not benchmark_table:
            all_tables = dbh.get_all_tables(
                self.db_tools.get_current_connection()
            )
            benchmark_table = random.choice(all_tables)

        table_schema = self.db_tools.get_table_schema(benchmark_table)
        if not table_schema:
            raise ValueError(
                f"Could not fetch DDL for table {benchmark_table}."
            )

        self._table_datagen = DynamicDataGenerator(table_schema)

        # Get the random seed for all remainder operations in the benchmark.
        seed = time.time()
        random.seed(seed)

        # Set context for result proto collection.
        self.db_tools.result_collector.set_context(
            table_name=benchmark_table,
            table_schema=table_schema,
            initial_db_size=dbh.get_db_size(
                self.db_tools.get_current_connection()
            ),
            seed=seed,
        )

        # Get the list of operations to perform, and the probability of each
        # operation.
        all_operations = self._config.operations
        ops_weights = [OPS_WEIGHT(op) for op in all_operations]

        # Main benchmark loop
        next_bid = 1
        for _ in range(self._config.num_ops):
            # Get the operation
            cur_ops = random.choices(all_operations, ops_weights)[0]

            if cur_ops == tp.OperationType.BRANCH:
                created = self.maybe_branch_and_reconnect(next_bid, random)
                if created:
                    next_bid += 1
            elif cur_ops == tp.OperationType.READ:
                self.read()
            elif cur_ops == tp.OperationType.INSERT:
                self.insert()
            elif cur_ops == tp.OperationType.UPDATE:
                self.update()
            elif cur_ops == tp.OperationType.RANGE_UPDATE:
                self.range_update()


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Run database benchmarks from config file."
    )

    parser.add_argument(
        "--config",
        type=str,
        default="microbench/test_config.textproto",
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

    validate_config(config)

    with BenchmarkSuite(config) as bench:
        bench.maybe_setup_db()
        bench.run_benchmark()
