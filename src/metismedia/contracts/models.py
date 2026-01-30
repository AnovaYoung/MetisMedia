"""Pydantic v2 models aligned to MetisMedia v2.1 Master Contract."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from metismedia.contracts.enums import (
    CacheStatus,
    CommercialMode,
    Platform,
    PolarityIntent,
    PulseStatus,
    ReceiptType,
)
from metismedia.contracts.reasons import ReasonCode


class BaseContractModel(BaseModel):
    """Base model for all contracts with common fields."""

    model_config = {"extra": "forbid", "frozen": False}


class TimestampedModel(BaseContractModel):
    """Model with timestamp fields."""

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProvenanceModel(TimestampedModel):
    """Model with provenance tracking."""

    tenant_id: UUID | None = None
    trace_id: UUID = Field(default_factory=uuid4)
    run_id: UUID = Field(default_factory=uuid4)
    idempotency_key: str | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)


class CampaignBrief(ProvenanceModel):
    """Campaign brief from Node A briefing session."""

    campaign_id: UUID = Field(default_factory=uuid4)
    name: str
    description: str
    polarity_intent: PolarityIntent
    commercial_mode: CommercialMode
    target_psychographics: dict[str, Any] = Field(default_factory=dict)
    budget_limit: float | None = None
    slot_values: dict[str, Any] = Field(default_factory=dict)
    missing_slots: list[str] = Field(default_factory=list)
    finalized: bool = False


class CampaignQuery(ProvenanceModel):
    """Query derived from CampaignBrief for discovery."""

    query_id: UUID = Field(default_factory=uuid4)
    campaign_id: UUID
    search_terms: list[str] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    max_results: int = 100
    embedding_model: str | None = None
    embedding_dimensions: int | None = None
    embedding_normalization: str | None = None


class Receipt(ProvenanceModel):
    """Evidence receipt from discovery (must include provenance)."""

    receipt_id: UUID = Field(default_factory=uuid4)
    receipt_type: ReceiptType
    platform: Platform
    url: str
    canonical_url: str | None = None
    title: str | None = None
    content: str | None = None
    published_at: datetime | None = None
    engagement_metrics: dict[str, Any] = Field(default_factory=dict)
    raw_data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL is not empty."""
        if not v or not v.strip():
            raise ValueError("url cannot be empty")
        return v.strip()


class RawCandidate(ProvenanceModel):
    """Proof-carrying candidate: must include receipts."""

    candidate_id: UUID = Field(default_factory=uuid4)
    receipts: list[Receipt] = Field(min_length=1)
    platform_handles: dict[Platform, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("receipts")
    @classmethod
    def validate_receipts_not_empty(cls, v: list[Receipt]) -> list[Receipt]:
        """Validate receipts list is not empty."""
        if not v:
            raise ValueError("receipts cannot be empty for RawCandidate")
        return v


class InfluencerEntity(ProvenanceModel):
    """Core influencer entity (embeddings stored as refs/ids, not raw vectors)."""

    influencer_id: UUID = Field(default_factory=uuid4)
    canonical_name: str
    bio: str | None = None
    bio_embedding_id: str | None = None
    recent_embedding_id: str | None = None
    verified: bool = False
    follower_count: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class TargetCard(ProvenanceModel):
    """Target card built from receipts with evidence citations."""

    target_card_id: UUID = Field(default_factory=uuid4)
    influencer_id: UUID
    campaign_id: UUID
    polarity_score: float | None = None
    psychographic_match: dict[str, Any] = Field(default_factory=dict)
    evidence_receipt_ids: list[UUID] = Field(default_factory=list)
    claims: dict[str, Any] = Field(default_factory=dict)
    unknown_fields: list[str] = Field(default_factory=list)
    reason_codes: list[ReasonCode] = Field(default_factory=list)


class CostEstimate(ProvenanceModel):
    """Cost estimate for an operation."""

    estimate_id: UUID = Field(default_factory=uuid4)
    operation: str
    provider: str | None = None
    estimated_cost: float
    currency: str = "USD"
    metadata: dict[str, Any] = Field(default_factory=dict)


class DirectiveObject(ProvenanceModel):
    """Node B output directive."""

    directive_id: UUID = Field(default_factory=uuid4)
    campaign_id: UUID
    influencer_id: UUID | None = None
    action: str
    reason_codes: list[ReasonCode] = Field(default_factory=list)
    cost_estimate: CostEstimate | None = None
    reservation_id: UUID | None = None
    cache_status: CacheStatus | None = None
    pulse_status: PulseStatus | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        """Validate action."""
        allowed = {"proceed", "skip", "reserve", "block"}
        if v not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v
