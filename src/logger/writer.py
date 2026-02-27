import threading
from datetime import datetime, timezone
from pathlib import Path

# Write log lines to logs_dir, one file per day (YYYY-MM-DD.log). Thread-safe.
class LogWriter:
    def __init__(self, logs_dir: Path) -> None:
        self._logs_dir = Path(logs_dir).resolve()
        self._lock = threading.Lock()

    def _log(self, level: str, message: str) -> None:
        self._logs_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = self._logs_dir / f"{date_str}.log"
        line = f"{datetime.now(timezone.utc).isoformat()} {level} {message}\n"
        with self._lock:
            with log_file.open("a", encoding="utf-8") as f:
                f.write(line)

    def info(self, message: str) -> None:
        self._log("INFO", message)

    def error(self, message: str) -> None:
        self._log("ERROR", message)

    def debug(self, message: str) -> None:
        self._log("DEBUG", message)

    def warning(self, message: str) -> None:
        self._log("WARNING", message)
