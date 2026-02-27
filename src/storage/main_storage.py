import json
from pathlib import Path
from typing import Any

from .paths import StoragePaths
from ..utils import atomic_write, file_lock


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        return []
    return data

# Main storage is used to store the current app+version index and the final APK files.
class MainStorage:
    def __init__(self, paths: StoragePaths) -> None:
        self._paths = paths

    def read_index(self) -> list[dict[str, Any]]:
        with file_lock(self._paths.main_index):
            return _read_json_list(self._paths.main_index)

    def write_index(self, entries: list[dict[str, Any]]) -> None:
        with file_lock(self._paths.main_index):
            atomic_write(
                self._paths.main_index,
                json.dumps(entries, ensure_ascii=False, indent=2),
            )

    def apk_path(self, filename: str) -> Path:
        return self._paths.main_apks_dir / filename
