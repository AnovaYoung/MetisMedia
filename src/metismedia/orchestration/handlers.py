"""Event handlers for orchestration pipeline: DB writes + next event publish."""

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.contracts.enums import NodeName
from metismedia.core.budget import Budget, BudgetState, budget_guard
from metismedia.core.ledger import CostEntry, CostLedger, compute_cost
from metismedia.db.queries.node_b import reserve_top_influencers_for_review
from metismedia.db.repos import (
    ContactRepo,
    DraftRepo,
    RunRepo,
    TargetCardRepo,
)
from metismedia.db.repos.receipt import ReceiptRepo
from metismedia.events.bus import EventBus
from metismedia.events.envelope import EventEnvelope
from metismedia.events.idemkeys import make_idempotency_key

logger = logging.getLogger(__name__)


def _record_cost(
    envelope: EventEnvelope,
    ledger: CostLedger | None,
    node: NodeName,
    provider: str,
    operation: str,
    unit_cost: float,
    quantity: float,
    metadata: dict[str, Any] | None = None,
    budget: Budget | None = None,
    budget_state: BudgetState | None = None,
) -> None:
    dollars = compute_cost(unit_cost, quantity)
    entry = CostEntry(
        tenant_id=envelope.tenant_id,
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        node=node,
        provider=provider,
        operation=operation,
        unit_cost=unit_cost,
        quantity=quantity,
        dollars=dollars,
        metadata=metadata or {},
    )
    if ledger is not None:
        ledger.record(entry)
    if budget is not None and budget_state is not None:
        budget_guard(
            budget,
            budget_state,
            cost_delta=entry.dollars,
            provider=entry.provider,
            calls_delta=1,
            node=entry.node.value,
        )
        budget_state.dollars_spent += entry.dollars
        budget_state.provider_calls[entry.provider] = (
            budget_state.provider_calls.get(entry.provider, 0) + 1
        )


async def _mark_run_completed_no_targets(
    session: AsyncSession,
    tenant_id: UUID,
    run_id: str,
    campaign_id: str,
) -> None:
    """Mark run as completed with 0 target cards and 0 drafts."""
    run_repo = RunRepo(session)
    await run_repo.update_status(
        tenant_id=tenant_id,
        run_id=UUID(run_id),
        status="completed",
        result_json={
            "target_cards_count": 0,
            "targets_count": 0,
            "drafts_count": 0,
            "total_cost_dollars": 0.0,
            "cost_summary": {},
            "notes": [],
        },
    )


async def handle_node_a_brief_finalized(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node A: record cost, publish node_b.input with query_embedding_id or mark completed."""
    _record_cost(
        envelope, ledger, NodeName.A, "internal", "brief_validate", 0.0, 1.0,
        budget=budget, budget_state=budget_state,
    )
    logger.info(f"Node A: Brief finalized for campaign {envelope.payload.get('campaign_id')}")

    brief = envelope.payload.get("brief") or {}
    slot_values = brief.get("slot_values") or {}
    query_embedding_id = slot_values.get("query_embedding_id")
    campaign_id = envelope.payload.get("campaign_id", "")

    if not query_embedding_id:
        logger.warning("No query_embedding_id, marking run completed with 0 targets")
        await _mark_run_completed_no_targets(
            session, envelope.tenant_id, envelope.run_id, campaign_id
        )
        return

    next_envelope = EventEnvelope(
        tenant_id=envelope.tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=envelope.tenant_id,
            run_id=envelope.run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="reserve",
        ),
        payload={
            "campaign_id": campaign_id,
            "query_embedding_id": query_embedding_id,
            "limit": 10,
        },
    )
    await bus.publish(next_envelope)


async def handle_node_b_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node B: reserve top influencers, record cost, publish node_b.directive_emitted per reserved."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    query_embedding_id = envelope.payload.get("query_embedding_id")
    limit = envelope.payload.get("limit", 10)

    if not query_embedding_id:
        logger.warning("Node B: No query_embedding_id")
        return

    reserved = await reserve_top_influencers_for_review(
        session=session,
        tenant_id=tenant_id,
        query_embedding_id=UUID(query_embedding_id),
        limit=limit,
        reason=f"campaign:{campaign_id}",
    )

    _record_cost(
        envelope, ledger, NodeName.B, "postgres", "vector_search", 0.001, float(len(reserved)),
        budget=budget, budget_state=budget_state,
    )

    if not reserved:
        logger.warning("Node B: No influencers reserved, marking run completed with 0 targets")
        await _mark_run_completed_no_targets(
            session, tenant_id, envelope.run_id, str(campaign_id)
        )
        return

    for r in reserved:
        next_envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.B,
            event_name="node_b.directive_emitted",
            trace_id=envelope.trace_id,
            run_id=envelope.run_id,
            idempotency_key=make_idempotency_key(
                tenant_id=tenant_id,
                run_id=envelope.run_id,
                node=NodeName.B,
                event_name="node_b.directive_emitted",
                step=f"reserve:{r.influencer_id}",
            ),
            payload={
                "campaign_id": str(campaign_id),
                "influencer_id": str(r.influencer_id),
                "reservation_id": str(r.reservation_id),
                "similarity": r.similarity,
                "action": "proceed",
            },
        )
        await bus.publish(next_envelope)

    logger.info(f"Node B: Reserved {len(reserved)} influencers for campaign {campaign_id}")


async def handle_node_b_directive_emitted(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Forward: publish node_c.input for this influencer."""
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")
    if not campaign_id or not influencer_id:
        return
    next_envelope = EventEnvelope(
        tenant_id=envelope.tenant_id,
        node=NodeName.C,
        event_name="node_c.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=envelope.tenant_id,
            run_id=envelope.run_id,
            node=NodeName.C,
            event_name="node_c.input",
            step=f"discover:{influencer_id}",
        ),
        payload={"campaign_id": campaign_id, "influencer_id": influencer_id},
    )
    await bus.publish(next_envelope)


async def handle_node_c_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node C: mock discovery, insert receipt, record cost, publish node_d.input."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")
    if not influencer_id:
        return
    influencer_uuid = UUID(influencer_id)

    receipt_repo = ReceiptRepo(session)
    receipt_id = await receipt_repo.insert_receipt(
        tenant_id=tenant_id,
        influencer_id=influencer_uuid,
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

    _record_cost(
        envelope, ledger, NodeName.C, "mock_discovery", "scrape", 0.02, 1.0,
        metadata={"influencer_id": influencer_id},
        budget=budget, budget_state=budget_state,
    )

    next_envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.D,
        event_name="node_d.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=envelope.run_id,
            node=NodeName.D,
            event_name="node_d.input",
            step=f"profile:{influencer_id}",
        ),
        payload={"campaign_id": campaign_id, "influencer_id": influencer_id, "receipt_id": str(receipt_id)},
    )
    await bus.publish(next_envelope)
    logger.info(f"Node C: Created receipt {receipt_id} for influencer {influencer_id}")


async def handle_node_d_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node D: mock profile, insert target card, record cost, publish node_e.input."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")
    if not campaign_id or not influencer_id:
        return
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

    _record_cost(
        envelope, ledger, NodeName.D, "mock_llm", "profile", 0.01, 1.0,
        budget=budget, budget_state=budget_state,
    )

    next_envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.E,
        event_name="node_e.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=envelope.run_id,
            node=NodeName.E,
            event_name="node_e.input",
            step=f"contact:{influencer_id}",
        ),
        payload={"campaign_id": campaign_id, "influencer_id": influencer_id, "target_card_id": str(card_id)},
    )
    await bus.publish(next_envelope)
    logger.info(f"Node D: Created target card {card_id}")


async def handle_node_e_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node E: mock contact lookup, insert contact, record cost, publish node_f.input."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")
    if not influencer_id:
        return
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

    _record_cost(
        envelope, ledger, NodeName.E, "mock_contact", "lookup", 0.005, 1.0,
        budget=budget, budget_state=budget_state,
    )

    next_envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.F,
        event_name="node_f.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=envelope.run_id,
            node=NodeName.F,
            event_name="node_f.input",
            step=f"draft:{influencer_id}",
        ),
        payload={"campaign_id": campaign_id, "influencer_id": influencer_id, "contact_id": str(contact_id)},
    )
    await bus.publish(next_envelope)
    logger.info(f"Node E: Created contact method {contact_id}")


async def handle_node_f_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node F: mock draft writer, insert draft, record cost, publish node_g.input."""
    tenant_id = envelope.tenant_id
    campaign_id = envelope.payload.get("campaign_id")
    influencer_id = envelope.payload.get("influencer_id")
    if not campaign_id or not influencer_id:
        return
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

    _record_cost(
        envelope, ledger, NodeName.F, "mock_llm", "draft_generate", 0.015, 1.0,
        budget=budget, budget_state=budget_state,
    )

    next_envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.G,
        event_name="node_g.input",
        trace_id=envelope.trace_id,
        run_id=envelope.run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=envelope.run_id,
            node=NodeName.G,
            event_name="node_g.input",
            step=f"finalize:{influencer_id}",
        ),
        payload={"campaign_id": campaign_id, "influencer_id": influencer_id, "draft_id": str(draft_id)},
    )
    await bus.publish(next_envelope)
    logger.info(f"Node F: Created draft {draft_id}")


async def handle_node_g_input(
    envelope: EventEnvelope,
    session: AsyncSession,
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    budget_state: BudgetState | None = None,
) -> None:
    """Node G: record cost, count target_cards/drafts, update run status completed."""
    _record_cost(
        envelope, ledger, NodeName.G, "internal", "finalize", 0.0, 1.0,
        budget=budget, budget_state=budget_state,
    )
    logger.info("Node G: Finalizing run")

    campaign_id_str = envelope.payload.get("campaign_id")
    if not campaign_id_str:
        return
    campaign_id = UUID(campaign_id_str)
    tenant_id = envelope.tenant_id
    run_id = envelope.run_id

    target_card_repo = TargetCardRepo(session)
    draft_repo = DraftRepo(session)
    target_cards = await target_card_repo.list_target_cards(tenant_id, campaign_id)
    drafts = await draft_repo.list_drafts(tenant_id, campaign_id)

    total_cost_dollars = 0.0
    cost_summary: dict[str, Any] = {}
    if ledger is not None and hasattr(ledger, "total_dollars") and hasattr(ledger, "summary"):
        total_cost_dollars = ledger.total_dollars(run_id=run_id)
        cost_summary = ledger.summary(run_id=run_id)

    run_repo = RunRepo(session)
    await run_repo.update_status(
        tenant_id=tenant_id,
        run_id=UUID(run_id),
        status="completed",
        result_json={
            "target_cards_count": len(target_cards),
            "targets_count": len(target_cards),
            "drafts_count": len(drafts),
            "total_cost_dollars": total_cost_dollars,
            "cost_summary": cost_summary,
            "notes": [],
        },
    )


HANDLER_MAP: dict[str, Any] = {
    "node_a.brief_finalized": handle_node_a_brief_finalized,
    "node_b.input": handle_node_b_input,
    "node_b.directive_emitted": handle_node_b_directive_emitted,
    "node_c.input": handle_node_c_input,
    "node_d.input": handle_node_d_input,
    "node_e.input": handle_node_e_input,
    "node_f.input": handle_node_f_input,
    "node_g.input": handle_node_g_input,
}
