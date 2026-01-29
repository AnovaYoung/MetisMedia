"""Pytest configuration and fixtures."""

import random

import pytest

from metismedia.db.session import reset_session_factory


@pytest.fixture(autouse=True)
def reset_db_state():
    """Reset database engine/session state before each test.

    This prevents event loop conflicts when running multiple async tests.
    """
    reset_session_factory()
    yield
    reset_session_factory()


@pytest.fixture(autouse=True)
def seed_random():
    """Seed random for deterministic tests."""
    random.seed(42)
    yield


@pytest.fixture
async def redis_client():
    """Create a Redis async client for testing."""
    import redis.asyncio as redis

    from metismedia.settings import get_settings

    settings = get_settings()
    client = redis.from_url(settings.redis_url)
    yield client
    await client.aclose()


@pytest.fixture
async def clean_redis(redis_client):
    """Clean Redis streams and keys before/after test."""
    from metismedia.events.constants import GROUP_NAME, STREAM_DLQ, STREAM_MAIN

    async def cleanup():
        try:
            await redis_client.delete(STREAM_MAIN)
        except Exception:
            pass
        try:
            await redis_client.delete(STREAM_DLQ)
        except Exception:
            pass
        keys = await redis_client.keys("idem:*")
        if keys:
            await redis_client.delete(*keys)

    await cleanup()
    yield redis_client
    await cleanup()
