"""Type aliases and data structures for database access layer."""

from dataclasses import dataclass
from datetime import datetime
from typing import NewType
from uuid import UUID

# Type aliases
TenantId = NewType("TenantId", UUID)
RunId = NewType("RunId", UUID)
TraceId = NewType("TraceId", UUID)
UUIDStr = NewType("UUIDStr", str)


@dataclass
class PaginationParams:
    """Pagination parameters."""

    limit: int
    offset: int = 0

    def __post_init__(self) -> None:
        """Validate pagination parameters."""
        if self.limit < 1:
            raise ValueError("limit must be >= 1")
        if self.offset < 0:
            raise ValueError("offset must be >= 0")


@dataclass
class TimeRange:
    """Time range for filtering queries."""

    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        """Validate time range."""
        if self.start is not None and self.end is not None:
            if self.start > self.end:
                raise ValueError("start must be <= end")
