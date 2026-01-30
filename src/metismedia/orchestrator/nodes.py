"""Mock node handlers for orchestrator pipeline."""

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.contracts.enums import NodeName
from metismedia.db.repos import (
    ContactRepo,
    DraftRepo,
    EmbeddingRepo,
    InfluencerRepo,
    ReceiptRepo,
    TargetCardRepo,
)
from metismedia.db.queries.node_b import reserve_top_influencers_for_review
from metismedia.events.envelope import EventEnvelope
from metismedia.orchestrator.runtime import NodeRuntime

logger = logging.getLogger(__name__)


async def node_a_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node A: Brief finalization (no-op, brief already exists)."""
    runtime.record_cost(
        envelope=envelope,
        provider="internal",
        operation="brief_validate",
        unit_cost=0.0,
        quantity=1.0,
    )
    logger.info(f"Node A: Brief finalized for campaign {envelope.payload.get('campaign_id')}")
    return []


async def node_b_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node B: Reserve top influencers and emit directive."""
    tenant_id = envelope.tenant_id
    campaign_id = UUID(envelope.payload["campaign_id"])
    query_embedding_id = envelope.payload.get("query_embedding_id")

    events: list[EventEnvelope] = []

    if query_embedding_id:
        reserved = await reserve_top_influencers_for_review(
            session=session,
            tenant_id=tenant_id,
            query_embedding_id=UUID(query_embedding_id),
            limit=envelope.payload.get("limit", 10),
            reason=f"campaign:{campaign_id}",
        )

        runtime.record_cost(
            envelope=envelope,
            provider="postgres",
            operation="vector_search",
            unit_cost=0.001,
            quantity=len(reserved),
        )

        for r in reserved:
            events.append(
                EventEnvelope(
                    tenant_id=tenant_id,
                    node=NodeName.B,
                    event_name="node_b.directive_emitted",
                    trace_id=envelope.trace_id,
                    run_id=envelope.run_id,
                    idempotency_key=f"{envelope.run_id}:b:{r.influencer_id}",
                    payload={
                        "campaign_id": str(campaign_id),
                        "influencer_id": str(r.influencer_id),
                        "reservation_id": str(r.reservation_id),
                        "similarity": r.similarity,
                        "action": "proceed",
                    },
                )
            )

        logger.info(f"Node B: Reserved {len(reserved)} influencers for campaign {campaign_id}")
    else:
        logger.warning("Node B: No query_embedding_id provided, skipping reservation")

    return events


async def node_c_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node C: Mock discovery - insert receipts + influencer rows."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")

    if influencer_id:
        influencer_id = UUID(influencer_id)

        receipt_repo = ReceiptRepo(session)
        receipt_id = await receipt_repo.insert_receipt(
            tenant_id=tenant_id,
            influencer_id=influencer_id,
            type_="social",
            url=f"https://mock.example.com/{influencer_id}",
            excerpt="Mock receipt content for discovery",
            occurred_at=datetime.now(timezone.utc),
            source_platform="mock",
            confidence=0.85,
            provenance_json={
                "trace_id": envelope.trace_id,
                "run_id": envelope.run_id,
                "node": "C",
            },
        )

        runtime.record_cost(
            envelope=envelope,
            provider="mock_discovery",
            operation="scrape",
            unit_cost=0.02,
            quantity=1.0,
            metadata={"influencer_id": str(influencer_id)},
        )

        logger.info(f"Node C: Created receipt {receipt_id} for influencer {influencer_id}")

        return [
            EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.C,
                event_name="node_c.batch_complete",
                trace_id=envelope.trace_id,
                run_id=envelope.run_id,
                idempotency_key=f"{envelope.run_id}:c:{influencer_id}",
                payload={
                    "campaign_id": campaign_id,
                    "influencer_id": str(influencer_id),
                    "receipt_id": str(receipt_id),
                },
            )
        ]

    return []


async def node_d_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node D: Mock profiler - write target_cards payload."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")

    if campaign_id and influencer_id:
        campaign_uuid = UUID(campaign_id)
        influencer_uuid = UUID(influencer_id)

        target_card_repo = TargetCardRepo(session)
        card_id = await target_card_repo.insert_target_card(
            tenant_id=tenant_id,
            campaign_id=campaign_uuid,
            influencer_id=influencer_uuid,
            payload_json={
                "polarity_score": 0.75,
                "psychographic_match": {"tech_interest": 0.8, "sustainability": 0.6},
                "claims": {"verified_email": True, "active_last_30d": True},
                "profile_summary": "Mock profile generated by Node D",
            },
        )

        runtime.record_cost(
            envelope=envelope,
            provider="mock_llm",
            operation="profile",
            unit_cost=0.01,
            quantity=1.0,
        )

        logger.info(f"Node D: Created target card {card_id}")

        return [
            EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.D,
                event_name="node_d.profile_ready",
                trace_id=envelope.trace_id,
                run_id=envelope.run_id,
                idempotency_key=f"{envelope.run_id}:d:{influencer_id}",
                payload={
                    "campaign_id": campaign_id,
                    "influencer_id": influencer_id,
                    "target_card_id": str(card_id),
                },
            )
        ]

    return []


async def node_e_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node E: Stub - write dummy contact_methods."""
    tenant_id = envelope.tenant_id
    influencer_id = envelope.payload.get("influencer_id")

    if influencer_id:
        influencer_uuid = UUID(influencer_id)

        contact_repo = ContactRepo(session)
        contact_id = await contact_repo.insert_contact_method(
            tenant_id=tenant_id,
            influencer_id=influencer_uuid,
            method="email",
            value=f"mock_{influencer_id[:8]}@example.com",
            confidence=0.7,
            verified=False,
            provenance_json={
                "trace_id": envelope.trace_id,
                "run_id": envelope.run_id,
                "node": "E",
            },
        )

        runtime.record_cost(
            envelope=envelope,
            provider="mock_contact",
            operation="lookup",
            unit_cost=0.005,
            quantity=1.0,
        )

        logger.info(f"Node E: Created contact method {contact_id}")

        return [
            EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.E,
                event_name="node_e.contact_ready",
                trace_id=envelope.trace_id,
                run_id=envelope.run_id,
                idempotency_key=f"{envelope.run_id}:e:{influencer_id}",
                payload={
                    "campaign_id": envelope.payload.get("campaign_id"),
                    "influencer_id": influencer_id,
                    "contact_id": str(contact_id),
                },
            )
        ]

    return []


async def node_f_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node F: Mock draft writer - insert drafts."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")

    if campaign_id and influencer_id:
        campaign_uuid = UUID(campaign_id)
        influencer_uuid = UUID(influencer_id)

        draft_repo = DraftRepo(session)
        draft_id = await draft_repo.insert_draft(
            tenant_id=tenant_id,
            campaign_id=campaign_uuid,
            influencer_id=influencer_uuid,
            channel="email",
            subject="Partnership Opportunity",
            body="Hi,\n\nWe'd love to collaborate with you on an upcoming campaign.\n\nBest regards",
            status="draft",
        )

        runtime.record_cost(
            envelope=envelope,
            provider="mock_llm",
            operation="draft_generate",
            unit_cost=0.015,
            quantity=1.0,
        )

        logger.info(f"Node F: Created draft {draft_id}")

        return [
            EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.F,
                event_name="node_f.draft_ready",
                trace_id=envelope.trace_id,
                run_id=envelope.run_id,
                idempotency_key=f"{envelope.run_id}:f:{influencer_id}",
                payload={
                    "campaign_id": campaign_id,
                    "influencer_id": influencer_id,
                    "draft_id": str(draft_id),
                },
            )
        ]

    return []


async def node_g_handler(
    envelope: EventEnvelope,
    runtime: NodeRuntime,
    session: AsyncSession,
) -> list[EventEnvelope]:
    """Node G: Stub (no-op)."""
    runtime.record_cost(
        envelope=envelope,
        provider="internal",
        operation="finalize",
        unit_cost=0.0,
        quantity=1.0,
    )
    logger.info("Node G: No-op stub executed")
    return []


NODE_HANDLERS = {
    NodeName.A: node_a_handler,
    NodeName.B: node_b_handler,
    NodeName.C: node_c_handler,
    NodeName.D: node_d_handler,
    NodeName.E: node_e_handler,
    NodeName.F: node_f_handler,
    NodeName.G: node_g_handler,
}
