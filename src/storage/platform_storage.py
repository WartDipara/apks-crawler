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


class PlatformStorage:
    """Per-platform storage: index (app+version list) and APK files for one source (apkpure/uptodown)."""

    def __init__(self, paths: StoragePaths) -> None:
        self._paths = paths

    def read_index(self) -> list[dict[str, Any]]:
        with file_lock(self._paths.index_file):
            return _read_json_list(self._paths.index_file)

    def append_entry(self, entry: dict[str, Any]) -> None:
        with file_lock(self._paths.index_file):
            data = _read_json_list(self._paths.index_file)
            data.append(entry)
            atomic_write(self._paths.index_file, json.dumps(data, ensure_ascii=False, indent=2))

    def clear_keys(self, keys: list[tuple[str, str]]) -> None:
        # Remove entries where (app_id, version) is in keys.
        key_set = set(keys)
        with file_lock(self._paths.index_file):
            data = _read_json_list(self._paths.index_file)
            data = [e for e in data if (e.get("app_id"), e.get("version")) not in key_set]
            atomic_write(self._paths.index_file, json.dumps(data, ensure_ascii=False, indent=2))

    def apk_path(self, filename: str) -> Path:
        return self._paths.apks_dir / filename
