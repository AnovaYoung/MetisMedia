"""Event envelope for Redis streams."""

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from metismedia.contracts.enums import NodeName


class EventEnvelope(BaseModel):
    """Event envelope for Redis event bus.

    Matches the canonical contract structure with Redis-specific serialization.
    All events must be tenant-scoped and originate from a specific node.
    """

    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tenant_id: UUID
    node: NodeName
    event_name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    trace_id: str
    run_id: str
    idempotency_key: str
    attempt: int = Field(default=0, ge=0)

    model_config = {"extra": "forbid", "frozen": False}

    def as_redis_fields(self) -> dict[str, str]:
        """Convert envelope to Redis stream fields (all values as strings).

        Returns:
            Dictionary with string keys and string values suitable for Redis XADD
        """
        return {
            "event_id": str(self.event_id),
            "occurred_at": self.occurred_at.isoformat(),
            "tenant_id": str(self.tenant_id),
            "node": self.node.value,
            "event_name": self.event_name,
            "payload": json.dumps(self.payload),
            "trace_id": self.trace_id,
            "run_id": self.run_id,
            "idempotency_key": self.idempotency_key,
            "attempt": str(self.attempt),
        }
