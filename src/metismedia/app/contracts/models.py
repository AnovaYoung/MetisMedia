"""Pydantic v2 models matching the Master Contract."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from metismedia.app.contracts.enums import (
    CommercialMode,
    NodeStatus,
    Platform,
    PolarityTarget,
    ReasonCode,
    ReceiptType,
)


class BaseContractModel(BaseModel):
    """Base model for all contracts with common fields."""

    model_config = {"extra": "forbid", "frozen": False}


class TimestampedModel(BaseContractModel):
    """Model with timestamp fields."""

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProvenanceModel(TimestampedModel):
    """Model with provenance tracking."""

    correlation_id: UUID = Field(default_factory=uuid4)
    run_id: UUID = Field(default_factory=uuid4)
    provenance: dict[str, Any] = Field(default_factory=dict)


class CampaignBrief(ProvenanceModel):
    """Campaign brief from Node A briefing session."""

    campaign_id: UUID = Field(default_factory=uuid4)
    name: str
    description: str
    polarity_target: PolarityTarget
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


class InfluencerEntity(ProvenanceModel):
    """Core influencer entity."""

    influencer_id: UUID = Field(default_factory=uuid4)
    canonical_name: str
    bio: str | None = None
    bio_embedding: list[float] | None = None
    recent_embedding: list[float] | None = None
    verified: bool = False
    follower_count: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class InfluencerPlatform(ProvenanceModel):
    """Platform-specific influencer profile."""

    platform_id: UUID = Field(default_factory=uuid4)
    influencer_id: UUID
    platform: Platform
    handle: str
    display_name: str | None = None
    url: str | None = None
    follower_count: int | None = None
    verified: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("handle")
    @classmethod
    def validate_handle(cls, v: str) -> str:
        """Validate handle is not empty."""
        if not v or not v.strip():
            raise ValueError("handle cannot be empty")
        return v.strip()


class Receipt(ProvenanceModel):
    """Evidence receipt from discovery."""

    receipt_id: UUID = Field(default_factory=uuid4)
    influencer_id: UUID | None = None
    platform_id: UUID | None = None
    receipt_type: ReceiptType
    platform: Platform
    url: str
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


class ContactMethod(ProvenanceModel):
    """Contact method for an influencer."""

    contact_id: UUID = Field(default_factory=uuid4)
    influencer_id: UUID
    method_type: str
    value: str
    verified: bool = False
    verification_date: datetime | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("method_type")
    @classmethod
    def validate_method_type(cls, v: str) -> str:
        """Validate method type."""
        allowed = {"email", "instagram_dm", "twitter_dm", "linkedin", "website_form", "other"}
        if v not in allowed:
            raise ValueError(f"method_type must be one of {allowed}")
        return v


class ContactBundle(ProvenanceModel):
    """Bundle of contact methods for an influencer."""

    bundle_id: UUID = Field(default_factory=uuid4)
    influencer_id: UUID
    contact_methods: list[ContactMethod] = Field(default_factory=list)
    preferred_method: UUID | None = None
    completeness_score: float = 0.0


class DraftRecord(ProvenanceModel):
    """Individual draft record."""

    draft_id: UUID = Field(default_factory=uuid4)
    target_card_id: UUID
    contact_bundle_id: UUID | None = None
    commercial_mode: CommercialMode
    variant: str
    content: str
    included_receipt_ids: list[UUID] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DraftPackage(ProvenanceModel):
    """Package of drafts for a target."""

    package_id: UUID = Field(default_factory=uuid4)
    target_card_id: UUID
    campaign_id: UUID
    drafts: list[DraftRecord] = Field(default_factory=list)
    status: NodeStatus = NodeStatus.PENDING
    reason_codes: list[ReasonCode] = Field(default_factory=list)


class NodeBDirective(ProvenanceModel):
    """Directive output from Node B."""

    directive_id: UUID = Field(default_factory=uuid4)
    campaign_id: UUID
    influencer_id: UUID
    action: str
    reason_codes: list[ReasonCode] = Field(default_factory=list)
    cost_estimate: float | None = None
    reservation_id: UUID | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        """Validate action."""
        allowed = {"proceed", "skip", "reserve", "block"}
        if v not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v


class DiscoveryDirective(ProvenanceModel):
    """Directive for discovery operations."""

    directive_id: UUID = Field(default_factory=uuid4)
    campaign_id: UUID
    query_id: UUID | None = None
    squad: str
    budget_allocation: float | None = None
    max_results: int = 100
    filters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveryBatch(ProvenanceModel):
    """Batch of discovery results."""

    batch_id: UUID = Field(default_factory=uuid4)
    directive_id: UUID
    receipts: list[Receipt] = Field(default_factory=list)
    influencers: list[InfluencerEntity] = Field(default_factory=list)
    platforms: list[InfluencerPlatform] = Field(default_factory=list)
    status: NodeStatus = NodeStatus.PENDING
    reason_codes: list[ReasonCode] = Field(default_factory=list)
    cost_actual: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)
