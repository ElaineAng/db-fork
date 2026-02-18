import os
import shutil
from os import walk
from os.path import exists, join


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_directory_size_bytes(path: str) -> int:
    """Get physical disk usage of a directory using st_blocks.

    Uses os.stat().st_blocks * 512 instead of os.path.getsize() to measure
    actual allocated disk blocks rather than logical file sizes. Tracks inodes
    to avoid double-counting hard-linked files.
    """
    if not exists(path):
        return 0
    total = 0
    seen_inodes = set()
    for dirpath, _, filenames in walk(path):
        for f in filenames:
            st = os.stat(join(dirpath, f))
            if st.st_ino not in seen_inodes:
                seen_inodes.add(st.st_ino)
                total += st.st_blocks * 512
    return total


def get_volume_usage_bytes(path: str) -> int:
    """Get physical disk usage of the filesystem/volume containing ``path``.

    Uses ``shutil.disk_usage(path).used`` which queries statvfs().
    On APFS this correctly accounts for CoW deduplication — cloned blocks
    are counted only once.

    IMPORTANT: Only meaningful when ``path`` is on an isolated volume
    dedicated to the database under test.
    """
    if not exists(path):
        return 0
    return shutil.disk_usage(path).used
