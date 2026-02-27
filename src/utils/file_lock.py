from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock


@contextmanager
def file_lock(path: Path):
    # Hold an exclusive lock on a companion lock file for the given path.
    # Use around read/write of path to get single-writer consistency.
    # use filelock to shoot differen platform compatibility issue.
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(lock_path)):
        yield
