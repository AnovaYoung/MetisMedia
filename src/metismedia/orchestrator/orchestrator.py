"""Orchestrator for running the agent pipeline."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from metismedia.contracts.enums import CommercialMode, NodeName, PolarityIntent
from metismedia.contracts.models import CampaignBrief
from metismedia.core import Budget, BudgetState, CostLedger, JsonLogLedger
from metismedia.db.repos import CampaignRepo, DraftRepo, RunRepo, TargetCardRepo
from metismedia.db.session import db_session
from metismedia.events import EventBus, EventEnvelope
from metismedia.orchestrator.nodes import NODE_HANDLERS
from metismedia.orchestrator.runtime import NodeRuntime

logger = logging.getLogger(__name__)


class DossierResult(BaseModel):
    """Result of an orchestrator run."""

    run_id: UUID
    campaign_id: UUID
    tenant_id: UUID
    trace_id: str
    status: str
    target_cards_count: int = 0
    drafts_count: int = 0
    total_cost_dollars: float = 0.0
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: str | None = None

    model_config = {"extra": "forbid"}


class Orchestrator:
    """Orchestrator for running the full pipeline."""

    def __init__(
        self,
        budget: Budget | None = None,
        ledger: CostLedger | None = None,
        poll_interval_seconds: float = 0.5,
        max_poll_iterations: int = 100,
    ) -> None:
        self.budget = budget or Budget(max_dollars=5.0)
        self.ledger = ledger or JsonLogLedger()
        self.poll_interval_seconds = poll_interval_seconds
        self.max_poll_iterations = max_poll_iterations
        self.budget_state = BudgetState()

    async def run(
        self,
        tenant_id: UUID,
        brief: CampaignBrief,
    ) -> DossierResult:
        """Run the full orchestration pipeline.

        1. Create run record
        2. Create campaign with brief JSON
        3. Emit node_a.brief_finalized event
        4. Process nodes sequentially (A -> B -> C -> D -> E -> F -> G)
        5. Poll for draft completion
        6. Return DossierResult
        """
        trace_id = str(brief.trace_id)
        run_id = uuid4()

        async with db_session() as session:
            run_repo = RunRepo(session)
            campaign_repo = CampaignRepo(session)

            run_id = await run_repo.create_run(
                tenant_id=tenant_id,
                trace_id=trace_id,
                status="running",
            )

            campaign_id = await campaign_repo.create_campaign(
                tenant_id=tenant_id,
                trace_id=trace_id,
                run_id=str(run_id),
                brief_json=brief.model_dump(mode="json"),
            )

            await run_repo.link_campaign(tenant_id, run_id, campaign_id)
            await session.commit()

            logger.info(f"Created run {run_id} with campaign {campaign_id}")

        try:
            initial_envelope = EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.A,
                event_name="node_a.brief_finalized",
                trace_id=trace_id,
                run_id=str(run_id),
                idempotency_key=f"{run_id}:a:init",
                payload={
                    "campaign_id": str(campaign_id),
                    "brief": brief.model_dump(mode="json"),
                },
            )

            await self._process_pipeline(
                tenant_id=tenant_id,
                campaign_id=campaign_id,
                run_id=run_id,
                initial_envelope=initial_envelope,
            )

            async with db_session() as session:
                target_card_repo = TargetCardRepo(session)
                draft_repo = DraftRepo(session)

                target_cards = await target_card_repo.list_target_cards(
                    tenant_id, campaign_id
                )
                drafts = await draft_repo.list_drafts(tenant_id, campaign_id)

                run_repo = RunRepo(session)
                await run_repo.update_status(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    status="completed",
                    result_json={
                        "target_cards_count": len(target_cards),
                        "drafts_count": len(drafts),
                        "total_cost_dollars": self.budget_state.dollars_spent,
                    },
                )
                await session.commit()

            return DossierResult(
                run_id=run_id,
                campaign_id=campaign_id,
                tenant_id=tenant_id,
                trace_id=trace_id,
                status="completed",
                target_cards_count=len(target_cards),
                drafts_count=len(drafts),
                total_cost_dollars=self.budget_state.dollars_spent,
            )

        except Exception as e:
            logger.exception(f"Orchestrator failed: {e}")

            async with db_session() as session:
                run_repo = RunRepo(session)
                await run_repo.update_status(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    status="failed",
                    error_message=str(e),
                )
                await session.commit()

            return DossierResult(
                run_id=run_id,
                campaign_id=campaign_id,
                tenant_id=tenant_id,
                trace_id=trace_id,
                status="failed",
                error_message=str(e),
                total_cost_dollars=self.budget_state.dollars_spent,
            )

    async def _process_pipeline(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        run_id: UUID,
        initial_envelope: EventEnvelope,
    ) -> None:
        """Process the pipeline by running nodes in sequence.

        The pipeline flow:
        A -> B (with query_embedding_id) -> C (per influencer) -> D -> E -> F -> G
        """
        trace_id = initial_envelope.trace_id
        base_payload = initial_envelope.payload.copy()
        query_embedding_id = base_payload.get("brief", {}).get(
            "slot_values", {}
        ).get("query_embedding_id")

        runtime_a = NodeRuntime(
            node=NodeName.A,
            budget=self.budget,
            budget_state=self.budget_state,
            ledger=self.ledger,
        )

        async with db_session() as session:
            handler_a = NODE_HANDLERS[NodeName.A]
            await runtime_a.run_with_timeout(
                handler_a(initial_envelope, runtime_a, session)
            )
            await session.commit()

        logger.info(f"Node A completed for run {run_id}")

        if not query_embedding_id:
            logger.warning("No query_embedding_id provided, skipping Node B")
            return

        node_b_envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.B,
            event_name="node_b.input",
            trace_id=trace_id,
            run_id=str(run_id),
            idempotency_key=f"{run_id}:b:init",
            payload={
                "campaign_id": str(campaign_id),
                "query_embedding_id": query_embedding_id,
                "limit": 10,
            },
        )

        runtime_b = NodeRuntime(
            node=NodeName.B,
            budget=self.budget,
            budget_state=self.budget_state,
            ledger=self.ledger,
        )

        directive_events: list[EventEnvelope] = []
        async with db_session() as session:
            handler_b = NODE_HANDLERS[NodeName.B]
            directive_events = await runtime_b.run_with_timeout(
                handler_b(node_b_envelope, runtime_b, session)
            )
            await session.commit()

        logger.info(f"Node B completed, reserved {len(directive_events)} influencers")

        if not directive_events:
            logger.warning("No influencers reserved, pipeline complete")
            return

        for directive in directive_events:
            influencer_id = directive.payload.get("influencer_id")
            if not influencer_id:
                continue

            node_payload = {
                "campaign_id": str(campaign_id),
                "influencer_id": influencer_id,
            }

            for node in [NodeName.C, NodeName.D, NodeName.E, NodeName.F]:
                runtime = NodeRuntime(
                    node=node,
                    budget=self.budget,
                    budget_state=self.budget_state,
                    ledger=self.ledger,
                )

                envelope = EventEnvelope(
                    tenant_id=tenant_id,
                    node=node,
                    event_name=f"node_{node.value.lower()}.input",
                    trace_id=trace_id,
                    run_id=str(run_id),
                    idempotency_key=f"{run_id}:{node.value.lower()}:{influencer_id}",
                    payload=node_payload,
                )

                handler = NODE_HANDLERS.get(node)
                if handler:
                    async with db_session() as session:
                        await runtime.run_with_timeout(
                            handler(envelope, runtime, session)
                        )
                        await session.commit()

        runtime_g = NodeRuntime(
            node=NodeName.G,
            budget=self.budget,
            budget_state=self.budget_state,
            ledger=self.ledger,
        )
        node_g_envelope = EventEnvelope(
            tenant_id=tenant_id,
            node=NodeName.G,
            event_name="node_g.input",
            trace_id=trace_id,
            run_id=str(run_id),
            idempotency_key=f"{run_id}:g:final",
            payload={"campaign_id": str(campaign_id)},
        )

        async with db_session() as session:
            handler_g = NODE_HANDLERS[NodeName.G]
            await runtime_g.run_with_timeout(
                handler_g(node_g_envelope, runtime_g, session)
            )
            await session.commit()

        logger.info(f"Pipeline completed for run {run_id}")


def create_minimal_brief(
    name: str = "Demo Campaign",
    description: str = "A demo campaign for testing",
    tenant_id: UUID | None = None,
) -> CampaignBrief:
    """Create a minimal CampaignBrief for testing."""
    return CampaignBrief(
        tenant_id=tenant_id,
        name=name,
        description=description,
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
    )
