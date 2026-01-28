"""Async session management."""

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from metismedia.db.engine import get_async_engine, reset_engine

T = TypeVar("T")

_async_session_factory: async_sessionmaker[AsyncSession] | None = None


def reset_session_factory() -> None:
    """Reset the session factory (for testing)."""
    global _async_session_factory
    reset_engine()
    _async_session_factory = None


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get or create the async session factory."""
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            get_async_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
    return _async_session_factory


@asynccontextmanager
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Async context manager yielding an AsyncSession."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def run_in_tx(
    session: AsyncSession,
    fn: Callable[[AsyncSession], Awaitable[T]],
) -> T:
    """Run a function inside a transaction, committing on success."""
    async with session.begin():
        result = await fn(session)
    return result


@asynccontextmanager
async def transaction(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    """Context manager for explicit transaction control."""
    async with session.begin():
        yield session
