from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class DatabaseSettings(BaseModel):
    url: str
    echo: bool = False


class RedisSettings(BaseModel):
    url: str


class WorkerSettings(BaseModel):
    default_confirmation_blocks: int = 12
    default_poll_interval_ms: int = 3000
    notify_concurrency: int = 50
    config_reload_interval_s: int = 5
    shutdown_grace_s: int = 30


class WebSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class LoggingSettings(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


class DeliveryRecordsSettings(BaseModel):
    max_success_rows: int = 50000
    cleanup_interval_seconds: int = 300
    cleanup_batch_size: int = 1000


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHAIN_INDEXER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    database: DatabaseSettings = Field(
        default_factory=lambda: DatabaseSettings(url="sqlite+aiosqlite:///./chain_indexer.db")
    )
    redis: RedisSettings = Field(default_factory=lambda: RedisSettings(url="redis://localhost:6379/0"))
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    web: WebSettings = Field(default_factory=WebSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    delivery_records: DeliveryRecordsSettings = Field(default_factory=DeliveryRecordsSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Precedence (high → low): env > yaml (passed via init kwargs) > dotenv > secrets > defaults
        return (env_settings, init_settings, dotenv_settings, file_secret_settings)


def load_settings(path: Path | str = Path("config.yaml")) -> Settings:
    """Load YAML config into Settings; env vars take precedence (see settings_customise_sources)."""
    p = Path(path)
    base: dict[str, Any] = {}
    if p.exists():
        with p.open("r") as f:
            base = yaml.safe_load(f) or {}
    return Settings(**base)
