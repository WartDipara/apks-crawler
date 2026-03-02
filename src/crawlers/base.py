from abc import ABC, abstractmethod
from src.logger import LogWriter
from src.storage import PlatformStorage


def _sanitize_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in s)


class BaseCrawler(ABC):
    def __init__(
        self,
        source_name: str,
        storage: PlatformStorage,
        logger: LogWriter,
    ) -> None:
        self.source_name = source_name
        self._storage = storage
        self._logger = logger

    @abstractmethod
    def resolve_download_url(self, app_id: str, version: str, **kwargs: object) -> tuple[str, dict[str, str]]:
        ...

    def _apk_filename(self, app_id: str, version: str) -> str:
        return f"{_sanitize_filename(app_id)}_{_sanitize_filename(version)}_{self.source_name}.apk"

    @abstractmethod
    def fetch(self, app_id: str, version: str, **kwargs: object) -> str | None:
        """Return resolved version on success, None when skipped or on error."""
        ...

    def get_category_game_list(self, category: str) -> list[dict]:
        """List games in category without fetching per-game details (e.g. versions). For peek."""
        return self.get_full_game_list(category)
