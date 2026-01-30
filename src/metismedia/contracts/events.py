"""Event definitions and envelope for event bus."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

# Event name constants
EVENT_CAMPAIGN_CREATED = "campaign.created"
EVENT_CAMPAIGN_COMPLETED = "campaign.completed"
EVENT_NODE_STARTED = "node.started"
EVENT_NODE_COMPLETED = "node.completed"
EVENT_NODE_FAILED = "node.failed"


class EventEnvelope(BaseModel):
    """Event envelope for event bus (requires trace_id + idempotency_key)."""

    event_id: UUID = Field(default_factory=uuid4)
    event_name: str
    trace_id: UUID
    idempotency_key: str
    tenant_id: UUID | None = None
    run_id: UUID | None = None
    node: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    attempt: int = 1
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid", "frozen": False}

    @field_validator("trace_id")
    @classmethod
    def validate_trace_id(cls, v: UUID) -> UUID:
        """Validate trace_id is provided."""
        if not v:
            raise ValueError("trace_id is required")
        return v

    @field_validator("idempotency_key")
    @classmethod
    def validate_idempotency_key(cls, v: str) -> str:
        """Validate idempotency_key is provided."""
        if not v or not v.strip():
            raise ValueError("idempotency_key is required and cannot be empty")
        return v.strip()
