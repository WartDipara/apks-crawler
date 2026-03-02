from pathlib import Path
from typing import Any
from pydantic import BaseModel, Field, field_validator


class StorageConfig(BaseModel):
    root: Path = Field(description="Storage root directory")

    @field_validator("root", mode="before")
    @classmethod
    def root_to_path(cls, v: Any) -> Path:
        return Path(v) if isinstance(v, str) else v


class ThreadPoolConfig(BaseModel):
    max_workers: int = Field(ge=1, le=32)
    shutdown_timeout_seconds: float = Field(ge=1.0)


class DownloadConfig(BaseModel):
    retries: int = Field(ge=0)
    timeout_seconds: float = Field(ge=1.0)
    progress_enabled: bool = Field()


class LoggerConfig(BaseModel):
    dir_name: str = Field()


class BrowserConfig(BaseModel):
    headless: bool = Field(default=True, description="Run Playwright browser in headless mode")


class ApkpureConfig(BaseModel):
    load_more_count: int = Field(default=0, ge=0, description="Number of Load More clicks per category; 0 = no limit")


class UptodownConfig(BaseModel):
    load_more_count: int = Field(default=1, ge=0, description="Number of category pages to load (page 1, 2, ...)")


class DataConfig(BaseModel):
    storage: StorageConfig = Field()
    thread_pool: ThreadPoolConfig = Field()
    download: DownloadConfig = Field()
    logger: LoggerConfig = Field()
    browser: BrowserConfig = Field(default_factory=BrowserConfig)
    apkpure: ApkpureConfig = Field(default_factory=ApkpureConfig)
    uptodown: UptodownConfig = Field(default_factory=UptodownConfig)
    download_number: int = Field(
        default=0,
        ge=0,
        description="Number of APKs to actually download per list; 0 = download all. Already-downloaded (in platform index) are skipped and not counted.",
    )

    model_config = {"extra": "ignore"}

    @classmethod
    def from_json_file(cls, path: Path) -> "DataConfig":
        import json
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.model_validate(data)


def get_config(config_path: Path | None = None) -> DataConfig:
    path = config_path or Path("config.json")
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    if get_config._instance is None:
        get_config._instance = DataConfig.from_json_file(path)
    return get_config._instance


get_config._instance: DataConfig | None = None
