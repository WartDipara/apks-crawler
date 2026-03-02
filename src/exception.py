__all__ = [
    "CrawlerError",
    "CrawlerPageError",
    "CrawlerDownloadError",
    "ConfigError",
]


class CrawlerError(Exception):
    def __init__(
        self,
        message: str,
        *,
        source_name: str | None = None,
        app_id: str | None = None,
    ) -> None:
        self.message = message
        self.source_name = source_name
        self.app_id = app_id
        parts = [message]
        if source_name:
            parts.append(f"source={source_name}")
        if app_id:
            parts.append(f"app_id={app_id}")
        super().__init__(" | ".join(parts))

    def for_log(self) -> str:
        out = f"message={self.message!r}"
        if self.source_name:
            out += f" source={self.source_name!r}"
        if self.app_id:
            out += f" app_id={self.app_id!r}"
        return out

class CrawlerPageError(CrawlerError):
    pass

class CrawlerDownloadError(CrawlerError):
    pass

class ConfigError(Exception):
    def __init__(self, message: str, path: str | None = None) -> None:
        self.message = message
        self.path = path
        detail = f" path={path}" if path else ""
        super().__init__(f"{message}{detail}")
