"""Pydantic models for orchestration run state and results."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class RunStatus(str, Enum):
    """Status of an orchestration run."""

    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunRecord(BaseModel):
    """Record for a single run."""

    run_id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID
    trace_id: UUID
    status: RunStatus = RunStatus.CREATED
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    state: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


class DossierResult(BaseModel):
    """Result dossier for a completed run."""

    run_id: UUID
    campaign_id: UUID
    tenant_id: UUID | None = None
    trace_id: str | None = None
    status: str = "completed"
    targets_count: int = 0
    drafts_count: int = 0
    target_cards_count: int = 0
    cost_summary: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)
    total_cost_dollars: float = 0.0
    completed_at: datetime | None = None
    error_message: str | None = None

    model_config = {"extra": "forbid"}
