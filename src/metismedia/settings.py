"""Application settings using pydantic-settings."""

import json
import logging
from functools import lru_cache
from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_SETTINGS_LOGGER = logging.getLogger("metismedia.settings")


class Settings(BaseSettings):
    """Application settings."""

    # Database
    database_url: str = "postgresql://postgres:postgres@localhost:5432/metismedia"
    database_url_async: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/metismedia"
    postgres_user: str | None = None
    postgres_password: str | None = None
    postgres_db: str | None = None
    postgres_host: str | None = None
    postgres_port: int | None = None

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_host: str | None = None
    redis_port: int | None = None

    # Application
    app_name: str = "MetisMedia"
    app_env: str = "local"
    debug: bool = False
    log_level: str = "INFO"

    # Budget defaults
    default_budget_max_dollars: float = 5.0
    default_budget_provider_call_caps: str | None = None  # JSON e.g. '{"firecrawl": 50, "exa": 25}'

    # API
    api_v1_prefix: str = "/api/v1"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("default_budget_provider_call_caps", mode="before")
    @classmethod
    def parse_provider_call_caps(cls, v: Any) -> str | None:
        """Leave as string; callers can parse JSON. Accept dict from env parse."""
        if v is None or v == "":
            return None
        if isinstance(v, dict):
            return json.dumps(v)
        return str(v)

    def get_default_budget_provider_call_caps(self) -> dict[str, int]:
        """Return parsed provider call caps or empty dict."""
        if not self.default_budget_provider_call_caps:
            return {}
        try:
            out = json.loads(self.default_budget_provider_call_caps)
            return {str(k): int(v) for k, v in out.items()}
        except (json.JSONDecodeError, TypeError, ValueError):
            raw = str(self.default_budget_provider_call_caps)
            if len(raw) > 200:
                raw = raw[:200] + "..."
            _SETTINGS_LOGGER.warning(
                "Failed to parse default_budget_provider_call_caps, using empty dict: %s",
                raw,
            )
            return {}


@lru_cache()
def get_settings() -> Settings:
    """Get settings instance (cached)."""
    return Settings()
