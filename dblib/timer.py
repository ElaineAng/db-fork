import numpy as np
import time
from psycopg2.extensions import cursor as _pgcursor


class Timer:
    def __init__(self):
        self.reset()

    def reset(self):
        self.each_elapsed = []

    def collect_elapsed(self, duration: float):
        self.each_elapsed.append(duration)

    def report_total_time(self):
        return sum(self.each_elapsed)

    def report_average_time(self):
        if not self.each_elapsed:
            return 0.0
        return np.mean(self.each_elapsed)

    def report_each_elapsed(self):
        return np.sort(self.each_elapsed)


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
