from pathlib import Path
from config import DataConfig


# Per-platform layout under storage root: data/<platform>/index.json, data/<platform>/apks/
# Logs are shared: data/logs/ (or configurable dir_name)
class StoragePaths:
    INDEX_FILENAME = "index.json"
    APKS_DIRNAME = "apks"

    def __init__(self, root: Path, logs_dir_name: str, platform: str) -> None:
        self._root = root.resolve()
        self._logs_dir_name = logs_dir_name
        self._platform = platform.strip().lower()

    @property
    def root(self) -> Path:
        return self._root

    @property
    def platform(self) -> str:
        return self._platform

    @property
    def platform_dir(self) -> Path:
        return self._root / self._platform

    @property
    def index_file(self) -> Path:
        return self.platform_dir / self.INDEX_FILENAME

    @property
    def apks_dir(self) -> Path:
        return self.platform_dir / self.APKS_DIRNAME

    @property
    def logs_dir(self) -> Path:
        return self._root / self._logs_dir_name

    def ensure_dirs(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        self.apks_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


def get_paths(config: DataConfig, platform: str) -> StoragePaths:
    return StoragePaths(
        root=config.storage.root,
        logs_dir_name=config.logger.dir_name,
        platform=platform,
    )
