from os import walk
from os.path import exists, getsize, join


def format_db_uri(
    user: str, password: str, host: str, port: int, db_name: str
) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_directory_size_bytes(path: str) -> int:
    if not exists(path):
        return 0
    total = 0
    for dirpath, _, filenames in walk(path):
        for f in filenames:
            total += getsize(join(dirpath, f))
    return total
