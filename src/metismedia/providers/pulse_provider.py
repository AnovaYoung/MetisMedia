"""Pulse provider interface for fetching recent content summaries."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, Field


class RecentSummary(BaseModel):
    """A summary of recent content from an influencer."""

    title: str
    url: str
    date: datetime
    summary: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


class PulseProvider(Protocol):
    """Protocol for fetching recent content summaries."""

    async def fetch_recent_summaries(
        self,
        url: str,
        limit: int = 3,
    ) -> list[RecentSummary]:
        """Fetch recent content summaries from a URL.

        Args:
            url: Primary URL of the influencer
            limit: Maximum number of summaries to return

        Returns:
            List of RecentSummary objects
        """
        ...


class MockPulseProvider:
    """Mock implementation for testing without external calls."""

    def __init__(
        self,
        default_summaries: list[dict[str, Any]] | None = None,
        call_counter: dict[str, int] | None = None,
    ) -> None:
        """Initialize mock provider.

        Args:
            default_summaries: Default summaries to return for any URL
            call_counter: Optional dict to track calls per URL
        """
        self._default_summaries = default_summaries or []
        self._url_summaries: dict[str, list[dict[str, Any]]] = {}
        self._call_counter = call_counter if call_counter is not None else {}

    def set_summaries_for_url(
        self,
        url: str,
        summaries: list[dict[str, Any]],
    ) -> None:
        """Configure summaries to return for a specific URL."""
        self._url_summaries[url] = summaries

    def get_call_count(self, url: str | None = None) -> int:
        """Get number of calls made to fetch_recent_summaries.

        Args:
            url: If provided, return calls for specific URL; else total calls
        """
        if url is not None:
            return self._call_counter.get(url, 0)
        return sum(self._call_counter.values())

    async def fetch_recent_summaries(
        self,
        url: str,
        limit: int = 3,
    ) -> list[RecentSummary]:
        """Return configured summaries for URL (mock implementation)."""
        self._call_counter[url] = self._call_counter.get(url, 0) + 1

        summaries_data = self._url_summaries.get(url, self._default_summaries)

        result = []
        for i, s in enumerate(summaries_data[:limit]):
            result.append(
                RecentSummary(
                    title=s.get("title", f"Mock Post {i + 1}"),
                    url=s.get("url", f"{url}/post/{i + 1}"),
                    date=s.get("date", datetime.now(timezone.utc)),
                    summary=s.get("summary", f"Mock summary for {url}"),
                    metadata=s.get("metadata", {}),
                )
            )

        return result
