"""Async database engine configuration."""

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from metismedia.settings import get_settings

_engine: AsyncEngine | None = None


def get_async_engine() -> AsyncEngine:
    """Get or create async database engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url_async,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            echo=settings.debug,
        )
    return _engine


def reset_engine() -> None:
    """Reset the engine (for testing)."""
    global _engine
    _engine = None
