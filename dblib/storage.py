import time
from contextlib import contextmanager


class StorageMeasurer:
    """Wraps operations with before/after storage measurement.

    When enabled, records disk_size_before and disk_size_after via
    the result collector's thread-local state. When disabled, no-op.
    """

    def __init__(self, db_tools, enabled=True):
        self._db_tools = db_tools
        self._enabled = enabled

    @contextmanager
    def measure(self):
        if self._enabled:
            size = self._db_tools.get_total_storage_bytes()
            self._db_tools.result_collector.record_disk_size_before(size)
        yield
        if self._enabled:
            time.sleep(0.01)  # 10ms: test APFS delayed st_blocks hypothesis
            size = self._db_tools.get_total_storage_bytes()
            self._db_tools.result_collector.record_disk_size_after(size)
