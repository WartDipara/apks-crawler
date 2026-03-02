# Controller of the crawler, submit fetch tasks, graceful shutdown, cleanup of partial files.
import signal
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, TypeVar
from config import DataConfig

T = TypeVar("T")


class Dispatch:
    def __init__(self, config: DataConfig) -> None:
        self._config = config
        self._executor: ThreadPoolExecutor | None = None
        self._shutdown_requested = False

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    def start(self) -> None:
        if self._executor is not None:
            return
        tp = self._config.thread_pool
        self._executor = ThreadPoolExecutor(max_workers=tp.max_workers)

    def submit(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T] | None:
        if self._shutdown_requested or self._executor is None:
            return None
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True, cancel_futures: bool = True) -> None:
        if self._executor is None:
            return
        try:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        except Exception:
            pass
        self._executor = None

    def register_signal_handlers(self) -> None:
        def _handler(signum: int, frame: Any) -> None:
            self._shutdown_requested = True
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    @property
    def shutdown_timeout_seconds(self) -> float:
        return self._config.thread_pool.shutdown_timeout_seconds

# Remove index entries whose apk file is missing or empty; delete orphan apk files.
def cleanup_partial_files(storage: Any) -> None:
    from src.storage.platform_storage import PlatformStorage

    if not isinstance(storage, PlatformStorage):
        return
    index = storage.read_index()
    keys_to_remove: list[tuple[str, str]] = []
    for e in index:
        path_str = e.get("path") or ""
        if not path_str:
            keys_to_remove.append((e.get("app_id") or "", e.get("version") or ""))
            continue
        path = storage.apk_path(path_str)
        if not path.exists() or path.stat().st_size == 0:
            keys_to_remove.append((e.get("app_id") or "", e.get("version") or ""))
            if path.exists():
                try:
                    path.unlink()
                except OSError:
                    pass
    if keys_to_remove:
        storage.clear_keys(keys_to_remove)
    index = storage.read_index()
    paths_in_index = {e.get("path") for e in index if e.get("path")}
    apks_dir = storage._paths.apks_dir
    if not apks_dir.exists():
        return
    for f in apks_dir.iterdir():
        if not f.is_file():
            continue
        if f.name not in paths_in_index:
            try:
                f.unlink()
            except OSError:
                pass


_instance: Dispatch | None = None

def get_dispatch(config: DataConfig | None = None) -> Dispatch:
    global _instance
    if _instance is None:
        from config import get_config
        _instance = Dispatch(config or get_config())
    return _instance


def init_dispatch(config: DataConfig) -> Dispatch:
    global _instance
    _instance = Dispatch(config)
    return _instance
