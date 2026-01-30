"""Node A Briefing Session API routes."""

import logging
from typing import Any
from uuid import UUID, uuid4

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from metismedia.contracts.enums import CommercialMode, NodeName, PolarityIntent
from metismedia.contracts.models import CampaignBrief
from metismedia.db.repos import BriefingSessionRepo, CampaignRepo, RunRepo
from metismedia.db.session import db_session
from metismedia.events import EventBus, EventEnvelope, make_idempotency_key
from metismedia.providers.node_a_provider import (
    REQUIRED_SLOTS,
    SLOT_SCHEMA,
    MockNodeAProvider,
    NodeAProvider,
    compute_missing_slots,
    is_ready_to_finalize,
)
from metismedia.settings import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/briefing", tags=["briefing"])

_provider: NodeAProvider | None = None


def get_provider() -> NodeAProvider:
    """Get the Node A provider (singleton)."""
    global _provider
    if _provider is None:
        _provider = MockNodeAProvider()
    return _provider


def set_provider(provider: NodeAProvider) -> None:
    """Set the Node A provider (for testing)."""
    global _provider
    _provider = provider


async def get_redis() -> redis.Redis:
    """Get Redis client."""
    settings = get_settings()
    return redis.from_url(settings.redis_url)


class CreateSessionRequest(BaseModel):
    """Request to create a new briefing session."""

    tenant_id: UUID
    initial_message: str | None = None

    model_config = {"extra": "forbid"}


class CreateSessionResponse(BaseModel):
    """Response from creating a briefing session."""

    session_id: UUID
    status: str
    slots: dict[str, Any] = Field(default_factory=dict)
    confidences: dict[str, float] = Field(default_factory=dict)
    missing_slots: list[str] = Field(default_factory=list)
    next_question: str | None = None
    messages: list[dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


class SubmitMessageRequest(BaseModel):
    """Request to submit a message to a briefing session."""

    tenant_id: UUID
    message: str

    model_config = {"extra": "forbid"}


class SubmitMessageResponse(BaseModel):
    """Response from submitting a message."""

    session_id: UUID
    status: str
    slots: dict[str, Any] = Field(default_factory=dict)
    confidences: dict[str, float] = Field(default_factory=dict)
    missing_slots: list[str] = Field(default_factory=list)
    extracted_count: int = 0
    next_question: str | None = None
    ready_to_finalize: bool = False
    blocking_slots: list[str] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


class FinalizeRequest(BaseModel):
    """Request to finalize a briefing session."""

    tenant_id: UUID

    model_config = {"extra": "forbid"}


class FinalizeResponse(BaseModel):
    """Response from finalizing a session."""

    session_id: UUID
    run_id: UUID
    campaign_id: UUID
    trace_id: str
    event_published: bool = False
    brief: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


@router.post("/sessions", response_model=CreateSessionResponse)
async def create_session(
    request: CreateSessionRequest,
    provider: NodeAProvider = Depends(get_provider),
) -> CreateSessionResponse:
    """Create a new briefing session."""
    tenant_id = request.tenant_id

    initial_missing = list(REQUIRED_SLOTS)
    initial_slots: dict[str, Any] = {}
    confidences: dict[str, float] = {}

    if request.initial_message:
        result = await provider.extract_slots(request.initial_message, {})
        initial_slots = result.updated_slots
        confidences = result.confidences
        initial_missing = compute_missing_slots(initial_slots, confidences)

    async with db_session() as session:
        repo = BriefingSessionRepo(session)

        session_id = await repo.create_session(
            tenant_id=tenant_id,
            initial_slots=initial_slots,
            initial_confidences=confidences,
        )

        if request.initial_message:
            await repo.add_message(tenant_id, session_id, "user", request.initial_message)

            if result.extracted_count > 0:
                await repo.update_slots(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    slots=initial_slots,
                    confidences=confidences,
                )

        await session.commit()

        session_data = await repo.get_session(tenant_id, session_id)

    next_question = await provider.suggest_next_question(
        initial_slots, initial_missing, confidences
    )

    return CreateSessionResponse(
        session_id=session_id,
        status="active",
        slots=initial_slots,
        confidences=confidences,
        missing_slots=initial_missing,
        next_question=next_question,
        messages=session_data.get("messages_json", []) if session_data else [],
    )


@router.post("/sessions/{session_id}/message", response_model=SubmitMessageResponse)
async def submit_message(
    session_id: UUID,
    request: SubmitMessageRequest,
    provider: NodeAProvider = Depends(get_provider),
) -> SubmitMessageResponse:
    """Submit a message to a briefing session."""
    tenant_id = request.tenant_id

    async with db_session() as session:
        repo = BriefingSessionRepo(session)

        session_data = await repo.get_session(tenant_id, session_id)
        if not session_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session {session_id} not found",
            )

        if session_data["status"] != "active":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Session is {session_data['status']}, cannot add messages",
            )

        current_slots = session_data.get("slots_json", {})
        current_confidences = session_data.get("confidences_json", {})

        result = await provider.extract_slots(request.message, current_slots)

        merged_slots = {**current_slots, **result.updated_slots}
        merged_confidences = {**current_confidences, **result.confidences}

        missing_slots = compute_missing_slots(merged_slots, merged_confidences)
        ready, blocking = is_ready_to_finalize(merged_slots, merged_confidences)

        await repo.add_message(tenant_id, session_id, "user", request.message)
        await repo.update_slots(
            tenant_id=tenant_id,
            session_id=session_id,
            slots=merged_slots,
            confidences=merged_confidences,
        )

        await session.commit()

    next_question = await provider.suggest_next_question(
        merged_slots, missing_slots, merged_confidences
    )

    return SubmitMessageResponse(
        session_id=session_id,
        status="active",
        slots=merged_slots,
        confidences=merged_confidences,
        missing_slots=missing_slots,
        extracted_count=result.extracted_count,
        next_question=next_question,
        ready_to_finalize=ready,
        blocking_slots=blocking,
    )


@router.post("/sessions/{session_id}/finalize", response_model=FinalizeResponse)
async def finalize_session(
    session_id: UUID,
    request: FinalizeRequest,
) -> FinalizeResponse:
    """Finalize a briefing session and emit node_a.brief_finalized event."""
    tenant_id = request.tenant_id

    async with db_session() as session:
        repo = BriefingSessionRepo(session)

        session_data = await repo.get_session(tenant_id, session_id)
        if not session_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session {session_id} not found",
            )

        if session_data["status"] != "active":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Session is already {session_data['status']}",
            )

        slots = session_data.get("slots_json", {})
        confidences = session_data.get("confidences_json", {})

        ready, blocking = is_ready_to_finalize(slots, confidences)
        if not ready:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot finalize: missing or low-confidence slots: {blocking}",
            )

        run_repo = RunRepo(session)
        campaign_repo = CampaignRepo(session)

        trace_id = str(uuid4())
        run_id = await run_repo.create_run(
            tenant_id=tenant_id,
            trace_id=trace_id,
            status="pending",
        )

        polarity_str = slots.get("polarity_intent", "allies")
        try:
            polarity = PolarityIntent(polarity_str)
        except ValueError:
            polarity = PolarityIntent.ALLIES

        commercial_str = slots.get("commercial_mode", "earned")
        try:
            commercial = CommercialMode(commercial_str)
        except ValueError:
            commercial = CommercialMode.EARNED

        brief = CampaignBrief(
            tenant_id=tenant_id,
            trace_id=UUID(trace_id),
            run_id=run_id,
            name=slots.get("campaign_name", "Untitled Campaign"),
            description=slots.get("campaign_description", ""),
            polarity_intent=polarity,
            commercial_mode=commercial,
            target_psychographics={
                "vibe": slots.get("vibe"),
                "influence_tier": slots.get("influence_tier"),
                "third_rail_terms": slots.get("third_rail_terms"),
            },
            slot_values=slots,
            missing_slots=[],
            finalized=True,
        )

        campaign_id = await campaign_repo.create_campaign(
            tenant_id=tenant_id,
            trace_id=trace_id,
            run_id=str(run_id),
            brief_json=brief.model_dump(mode="json"),
        )

        brief.campaign_id = campaign_id

        await run_repo.link_campaign(tenant_id, run_id, campaign_id)
        await repo.finalize_session(tenant_id, session_id, run_id, campaign_id)

        await session.commit()

    event_published = False
    try:
        redis_client = await get_redis()
        bus = EventBus(redis_client)

        idem_key = make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.A,
            event_name="node_a.brief_finalized",
            step="finalize",
        )

        envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.A,
            event_name="node_a.brief_finalized",
            trace_id=trace_id,
            run_id=str(run_id),
            idempotency_key=idem_key,
            payload={
                "session_id": str(session_id),
                "campaign_id": str(campaign_id),
                "brief": brief.model_dump(mode="json"),
            },
        )

        await bus.publish(envelope)
        event_published = True

        logger.info(
            f"Published node_a.brief_finalized for session {session_id}, "
            f"run {run_id}, campaign {campaign_id}"
        )

        await redis_client.aclose()

    except Exception as e:
        logger.error(f"Failed to publish event: {e}")

    return FinalizeResponse(
        session_id=session_id,
        run_id=run_id,
        campaign_id=campaign_id,
        trace_id=trace_id,
        event_published=event_published,
        brief=brief.model_dump(mode="json"),
    )


@router.get("/sessions/{session_id}", response_model=CreateSessionResponse)
async def get_session(
    session_id: UUID,
    tenant_id: UUID,
    provider: NodeAProvider = Depends(get_provider),
) -> CreateSessionResponse:
    """Get the current state of a briefing session."""
    async with db_session() as session:
        repo = BriefingSessionRepo(session)

        session_data = await repo.get_session(tenant_id, session_id)
        if not session_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Session {session_id} not found",
            )

    slots = session_data.get("slots_json", {})
    confidences = session_data.get("confidences_json", {})
    missing = session_data.get("missing_slots", [])

    next_question = None
    if session_data["status"] == "active":
        next_question = await provider.suggest_next_question(slots, missing, confidences)

    return CreateSessionResponse(
        session_id=session_id,
        status=session_data["status"],
        slots=slots,
        confidences=confidences,
        missing_slots=missing,
        next_question=next_question,
        messages=session_data.get("messages_json", []),
    )
