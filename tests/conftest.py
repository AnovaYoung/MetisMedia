"""Pytest configuration and fixtures."""

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
