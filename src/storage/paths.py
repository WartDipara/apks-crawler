from pathlib import Path
from config import DataConfig

# File-system layout under storage root. All paths relative to root.
# Use file system is more flexible and easy to deploy, consider in single machine.
# Main storage and staging storage are separated, and logs are separated.
# Main storage is used to store the current app+version index and the final APK files.
# Staging storage is used to store the pending app+version index and the downloaded APK files.
# Logs are used to store the logs.
# All paths are relative to the storage root.
# All paths are stored in the storage root.
class StoragePaths:
    STAGING_INDEX = "staging/index.json"
    STAGING_APKS = "staging/apks"
    MAIN_INDEX = "main/index.json"
    MAIN_APKS = "main/apks"

    def __init__(self, root: Path, logs_dir_name: str) -> None:
        self._root = root.resolve()
        self._logs_dir_name = logs_dir_name

    @property
    def root(self) -> Path:
        return self._root

    @property
    def staging_index(self) -> Path:
        return self._root / self.STAGING_INDEX

    @property
    def staging_apks_dir(self) -> Path:
        return self._root / self.STAGING_APKS

    @property
    def main_index(self) -> Path:
        return self._root / self.MAIN_INDEX

    @property
    def main_apks_dir(self) -> Path:
        return self._root / self.MAIN_APKS

    @property
    def logs_dir(self) -> Path:
        return self._root / self._logs_dir_name

    # Create root, staging apks, main apks, and logs dirs if missing.
    def ensure_dirs(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        self.staging_apks_dir.mkdir(parents=True, exist_ok=True)
        self.main_apks_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
def get_paths(config: DataConfig) -> StoragePaths:
    return StoragePaths(
        root=config.storage.root,
        logs_dir_name=config.logger.dir_name,
    )
