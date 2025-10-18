import numpy as np


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
