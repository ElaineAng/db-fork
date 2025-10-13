from time import time
import psycopg2
from psycopg2.extensions import cursor as _pgcursor
from dblib.dolt import DoltToolSuite
from microbench.timer import Timer


def format_db_uri(user: str, password: str, host: str, port: int) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/microbench"


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


class DatabaseTask:
    def __init__(self, backend: str, timer: Timer):
        self.backend = backend
        self.timer = timer
        self.timer_cursor = lambda *args, **kwargs: TimerCursor(
            *args, **kwargs, timer=self.timer
        )
        if backend == "dolt":
            uri = format_db_uri("postgres", "password", "localhost", 5432)

            self.conn = psycopg2.connect(uri, cursor_factory=self.timer_cursor)
            self.db_tools = DoltToolSuite(connection=self.conn)

        if not self.db_tools:
            raise ValueError(f"Unsupported backend: {self.backend}")

    def report_total(self):
        return self.timer.report_total_time()

    def branch(self, branch_name: str):
        self.db_tools.create_db_branch(branch_name)

    def read(self):
        pass

    def insert(self):
        pass

    def update(self):
        pass
