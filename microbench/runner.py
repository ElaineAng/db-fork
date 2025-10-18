import sys
from time import time

import psycopg2
from psycopg2.extensions import cursor as _pgcursor
from tasks import DatabaseTask
from microbench.timer import Timer
from microbench.sampling import beta_distribution
from dblib.dolt import DoltToolSuite


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


# TODO: Allow reporting the query which the current elapsed time was collected for.
class TimerCursor(_pgcursor):
    def __init__(self, *args, **kwargs):
        self.timer = kwargs.pop("timer", None)
        super(TimerCursor, self).__init__(*args, **kwargs)

    def execute(self, query: str, vars=None):
        start_timestamp = time()
        try:
            super(TimerCursor, self).execute(query, vars)
        finally:
            end_timestamp = time()
            if self.timer:
                self.timer.collect_elapsed(end_timestamp - start_timestamp)


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

        self.timer = Timer()
        self.timer_cursor = lambda *args, **kwargs: TimerCursor(
            *args, **kwargs, timer=self.timer
        )
        timed_tools, regular_tools = None, None
        if backend == "dolt":
            # TODO: Consider making these parameters configurable.
            uri = format_db_uri(
                "postgres", "password", "localhost", 5432, self._db_name
            )

            # Timed connection and tools to measure timing.
            timed_conn = psycopg2.connect(uri, cursor_factory=self.timer_cursor)
            timed_tools = DoltToolSuite(connection=timed_conn)

            # Regular connection to perform all other misc queries and get stats
            # of the database.
            regular_conn = psycopg2.connect(uri)
            regular_tools = DoltToolSuite(connection=regular_conn)

        if not timed_tools or not regular_tools:
            raise ValueError(f"Unsupported backend: {backend}")

        self.db_task = DatabaseTask(
            timed_tools=timed_tools,
            regular_tools=regular_tools,
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

    def read_bench(self, table_names: list[str] = []):
        # Simple read bench requires pre-loaded data.
        if not self.preload_data_dir:
            return

        benchmark_tables = table_names if table_names else self.preloaded_tables
        for table in benchmark_tables:
            total_rows = self.db_task.get_table_row_count(table)
            print(f"Table {table} has {total_rows} rows.")
            self.db_task.point_read(
                table,
                sampling_rate=0.05,
                max_sampling_size=500,
                dist_lambda=lambda sample_size: beta_distribution(
                    sample_size, alpha=2.0, beta=5.0
                ),
                sort_idx=0,
            )
            print(
                f"Average read time for table {table}: {self.timer.report_average_time():.6f} seconds\n"
            )
            self.timer.reset()

    def insert_bench(self):
        pass

    def update_bench(self):
        pass

    def branch_bench(self):
        pass

    def branch_insert_read_bench(self):
        pass

    def branch_update_read_bench(self):
        pass


if __name__ == "__main__":
    # Args:
    #   backend: The database backend to use. Supported backends: dolt
    backend = sys.argv[1]
    supported_backends = ["dolt"]
    if backend not in supported_backends:
        print("Supported backend list: ", supported_backends)
        sys.exit(1)

    benchmark_suite = BenchmarkSuite(
        backend=backend,
        db_name="microbench",
        db_schema_path="db_setup/tpcc_schema.sql",
    )

    benchmark_suite.read_bench()
