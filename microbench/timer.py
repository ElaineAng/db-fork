# TODO: Use histograms to track distribution of times.
class Timer:
    def __init__(self):
        self.total_elapsed = 0.0

    def collect_elapsed(self, duration: float):
        self.total_elapsed += duration

    def report_total_time(self):
        return self.total_elapsed
