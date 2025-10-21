import numpy as np
from time import time
from psycopg2.extensions import cursor as _pgcursor
from psycopg2.extensions import connection as _pgconn


class Timer:
    def __init__(self):
        self.reset()

    def reset(self):
        self.each_cursor_elapsed = []
        self.cursor_elapsed_by_tag = {}
        self.each_connection_elapsed = []
        self.connection_elapsed_by_tag = {}

    def collect_cursor_elapsed(self, duration: float, tag: str = "") -> None:
        self.each_cursor_elapsed.append(duration)
        if tag:
            if tag not in self.cursor_elapsed_by_tag:
                self.cursor_elapsed_by_tag[tag] = []
            self.cursor_elapsed_by_tag[tag].append(duration)

    def report_cursor_elapsed(self, tag: str = "") -> list[float]:
        if tag:
            return self.cursor_elapsed_by_tag.get(tag, [])
        return self.each_cursor_elapsed

    def collect_connection_elapsed(
        self,
        duration: float,
        tag: str = "",
    ) -> None:
        self.each_connection_elapsed.append(duration)
        if tag:
            if tag not in self.connection_elapsed_by_tag:
                self.connection_elapsed_by_tag[tag] = []
            self.connection_elapsed_by_tag[tag].append(duration)

    def report_connection_elapsed(self, tag: str = "") -> list[float]:
        if tag:
            return self.connection_elapsed_by_tag.get(tag, [])
        return self.each_connection_elapsed


def get_average(times: list[float]) -> float:
    if not times:
        return 0.0
    return np.mean(times)


def get_stddev(times: list[float]) -> float:
    if not times:
        return 0.0
    return np.std(times)


def get_sum(times: list[float]) -> float:
    if not times:
        return 0.0
    return np.sum(times)


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
                # If we want finer grained control (e.g. collecting time per
                # query type) probably worth pass in a tag explicitly every time
                # we get a cursor.
                self.timer.collect_cursor_elapsed(
                    end_timestamp - start_timestamp, tag="execute"
                )

    def fetchall(self):
        start_timestamp = time()
        try:
            return super().fetchall()
        finally:
            end_timestamp = time()
            if self.timer:
                self.timer.collect_cursor_elapsed(
                    end_timestamp - start_timestamp, tag="fetchall"
                )


class TimerConnection(_pgconn):
    def __init__(self, *args, **kwargs):
        self.timer = kwargs.pop("timer", None)
        super(TimerConnection, self).__init__(*args, **kwargs)

    def commit(self):
        start_timestamp = time()
        try:
            super(TimerConnection, self).commit()
        finally:
            end_timestamp = time()
            if self.timer:
                self.timer.collect_connection_elapsed(
                    end_timestamp - start_timestamp, tag="commit"
                )
