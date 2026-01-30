"""Orchestrator: start run, publish initial event, await completion via polling."""

import asyncio
import json
import logging
import time
from typing import Any
from uuid import UUID, uuid4

from metismedia.contracts.enums import NodeName
from metismedia.contracts.models import CampaignBrief
from metismedia.db.repos import CampaignRepo, RunRepo
from metismedia.db.session import db_session
from metismedia.events.bus import EventBus
from metismedia.events.envelope import EventEnvelope
from metismedia.events.idemkeys import make_idempotency_key
from metismedia.orchestration.run_models import DossierResult

logger = logging.getLogger(__name__)


class Orchestrator:
    """Event-driven orchestrator: start_run publishes node_a.brief_finalized; await_completion polls runs."""

    def __init__(
        self,
        bus: EventBus,
        poll_interval_seconds: float = 0.2,
        max_poll_iterations: int = 500,
    ) -> None:
        self.bus = bus
        self.poll_interval_seconds = poll_interval_seconds
        self.max_poll_iterations = max_poll_iterations

    async def start_run(self, tenant_id: UUID, brief: CampaignBrief) -> UUID:
        """Create run + campaign, publish initial EventEnvelope (node_a.brief_finalized). Returns run_id."""
        trace_id = str(brief.trace_id)
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

            envelope = EventEnvelope(
                tenant_id=tenant_id,
                node=NodeName.A,
                event_name="node_a.brief_finalized",
                trace_id=trace_id,
                run_id=str(run_id),
                idempotency_key=make_idempotency_key(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    node=NodeName.A,
                    event_name="node_a.brief_finalized",
                    step="brief_finalized",
                ),
                payload={
                    "campaign_id": str(campaign_id),
                    "brief": brief.model_dump(mode="json"),
                },
            )
            await self.bus.publish(envelope)
            logger.info(f"Published node_a.brief_finalized for run {run_id} campaign {campaign_id}")
            return run_id

    async def await_completion(
        self,
        tenant_id: UUID,
        run_id: UUID,
        timeout_s: float,
    ) -> DossierResult:
        """Poll runs table until status is completed/failed or timeout. Return DossierResult."""
        deadline = time.monotonic() + timeout_s
        poll_interval = self.poll_interval_seconds
        iterations = 0

        while iterations < self.max_poll_iterations:
            if time.monotonic() >= deadline:
                break
            async with db_session() as session:
                run_repo = RunRepo(session)
                row = await run_repo.get_by_id(tenant_id, run_id)
            if row is None:
                await asyncio.sleep(poll_interval)
                iterations += 1
                continue
            status = row.get("status") or "pending"
            if status == "completed":
                result_json = row.get("result_json")
                if isinstance(result_json, str):
                    result_json = json.loads(result_json) if result_json else {}
                if not isinstance(result_json, dict):
                    result_json = {}
                return _row_to_dossier(tenant_id, run_id, row, result_json, status=status)
            if status == "failed":
                return _row_to_dossier(
                    tenant_id,
                    run_id,
                    row,
                    {},
                    status=status,
                    error_message=row.get("error_message"),
                )
            await asyncio.sleep(poll_interval)
            iterations += 1

        async with db_session() as session:
            run_repo = RunRepo(session)
            row = await run_repo.get_by_id(tenant_id, run_id)
        return _row_to_dossier(
            tenant_id,
            run_id,
            row or {},
            {},
            status="failed",
            error_message="await_completion timeout",
        )


def _row_to_dossier(
    tenant_id: UUID,
    run_id: UUID,
    row: dict[str, Any],
    result_json: dict[str, Any],
    status: str,
    error_message: str | None = None,
) -> DossierResult:
    campaign_id = row.get("campaign_id")
    if campaign_id is None:
        campaign_id = uuid4()
    return DossierResult(
        run_id=run_id,
        campaign_id=campaign_id,
        tenant_id=tenant_id,
        trace_id=row.get("trace_id") or "",
        status=status,
        targets_count=result_json.get("targets_count", result_json.get("target_cards_count", 0)),
        drafts_count=result_json.get("drafts_count", 0),
        target_cards_count=result_json.get("target_cards_count", result_json.get("targets_count", 0)),
        cost_summary=result_json.get("cost_summary", {}),
        notes=result_json.get("notes", []),
        total_cost_dollars=result_json.get("total_cost_dollars", 0.0),
        completed_at=row.get("completed_at"),
        error_message=error_message or row.get("error_message"),
    )
