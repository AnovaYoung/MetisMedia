"""Application settings using pydantic-settings."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # API
    api_v1_prefix: str = "/api/v1"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get settings instance (cached)."""
    return Settings()
