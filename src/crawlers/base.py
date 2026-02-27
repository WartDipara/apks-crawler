from abc import ABC, abstractmethod

from src.logger import LogWriter
from src.storage import StagingStorage


def _sanitize_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in s)


class BaseCrawler(ABC):
    def __init__(
        self,
        source_name: str,
        staging: StagingStorage,
        logger: LogWriter,
    ) -> None:
        self.source_name = source_name
        self._staging = staging
        self._logger = logger

    @abstractmethod
    def resolve_download_url(self, app_id: str, version: str, **kwargs: object) -> tuple[str, dict[str, str]]:
        ...

    def _staging_filename(self, app_id: str, version: str) -> str:
        return f"{_sanitize_filename(app_id)}_{_sanitize_filename(version)}_{self.source_name}.apk"

    @abstractmethod
    def fetch(self, app_id: str, version: str, **kwargs: object) -> None:
        ...
