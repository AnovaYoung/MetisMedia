"""Node B payload and result models. Maps to DirectiveObject + reason codes."""

from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from metismedia.contracts.reasons import ReasonCode


class NodeBInput(BaseModel):
    """Node B input payload: campaign context and query parameters."""

    campaign_id: UUID
    query_embedding_id: UUID
    desired_count: int = Field(ge=1, le=100)
    risk_profile: str = "default"
    polarity_desired: int = Field(ge=-10, le=10, description="Desired polarity in [-10, +10]")
    slot_values: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


class NodeBResult(BaseModel):
    """Node B result: maps directly to DirectiveObject + reason codes."""

    campaign_id: UUID
    influencer_id: UUID | None = None
    action: str = Field(description="proceed | skip | reserve | block")
    reason_codes: list[ReasonCode] = Field(default_factory=list)
    reservation_id: UUID | None = None
    mms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        allowed = {"proceed", "skip", "reserve", "block"}
        if v not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v

    model_config = {"extra": "forbid"}
