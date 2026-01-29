#!/usr/bin/env python3
"""Demo runner for the event-driven orchestrator pipeline.

Usage:
    uv run python scripts/demo_run.py

Requires:
    - Docker compose running (postgres + redis)
    - Database migrated (make migrate)
"""

import asyncio
import logging
import sys
from uuid import uuid4

from redis.asyncio import Redis

from metismedia.contracts.enums import CommercialMode, PolarityIntent
from metismedia.contracts.models import CampaignBrief
from metismedia.core import Budget, InMemoryLedger
from metismedia.db.repos import EmbeddingRepo, InfluencerRepo
from metismedia.db.session import db_session
from metismedia.events import EventBus, Worker
from metismedia.orchestration import DossierResult, Orchestrator
from metismedia.orchestration.registry import build_handler_registry
from metismedia.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

cost_logger = logging.getLogger("metismedia.cost")
cost_logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)


async def seed_test_data(tenant_id) -> str | None:
    """Seed test influencers with embeddings, return query embedding ID."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        query_vector = [1.0] + [0.0] * 1535

        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="demo",
            dims=1536,
            norm="l2",
            vector=query_vector,
        )

        for i in range(5):
            similarity_offset = 0.1 * i
            vec = [1.0 - similarity_offset, similarity_offset] + [0.0] * 1534

            bio_emb_id = await emb_repo.create_embedding(
                tenant_id=tenant_id,
                kind="bio",
                model="demo",
                dims=1536,
                norm="l2",
                vector=vec,
            )

            await inf_repo.upsert_influencer(
                tenant_id=tenant_id,
                canonical_name=f"Demo Influencer {i + 1}",
                primary_url=f"https://demo.example.com/influencer-{i + 1}-{tenant_id}",
                platform="substack",
                follower_count=1000 * (i + 1),
                bio_embedding_id=bio_emb_id,
                bio_text=f"I write about technology and innovation. Influencer #{i + 1}.",
            )

        await session.commit()
        logger.info(f"Seeded 5 influencers for tenant {tenant_id}")

        return str(query_emb_id)


async def main() -> DossierResult:
    """Run the demo: start run, run worker, await completion, return DossierResult."""
    tenant_id = uuid4()
    logger.info(f"Starting demo run with tenant_id: {tenant_id}")

    query_embedding_id = await seed_test_data(tenant_id)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Demo Tech Campaign",
        description="A demo campaign targeting tech-focused content creators",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        target_psychographics={
            "interests": ["technology", "innovation", "startups"],
            "tone": "professional",
        },
        finalized=True,
    )

    if query_embedding_id:
        brief.slot_values["query_embedding_id"] = query_embedding_id

    budget = Budget(
        max_dollars=5.0,
        max_provider_calls={"mock_discovery": 100, "mock_llm": 100},
        max_node_seconds={
            "A": 10.0,
            "B": 30.0,
            "C": 30.0,
            "D": 30.0,
            "E": 10.0,
            "F": 30.0,
            "G": 10.0,
        },
    )

    ledger = InMemoryLedger()
    settings = get_settings()
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    bus = EventBus(redis)
    orchestrator = Orchestrator(
        bus=bus,
        poll_interval_seconds=0.1,
        max_poll_iterations=500,
    )

    handler_registry = build_handler_registry(budget=budget, ledger=ledger, bus=bus)
    worker = Worker(redis, bus)

    logger.info("Starting orchestrator run (event-driven)...")
    run_id = await orchestrator.start_run(tenant_id=tenant_id, brief=brief)

    worker_task = asyncio.create_task(
        worker.run(
            handler_registry,
            stop_after=None,
            budget=budget,
            ledger=ledger,
        )
    )

    result = await orchestrator.await_completion(
        tenant_id=tenant_id,
        run_id=run_id,
        timeout_s=30.0,
    )

    worker.stop()
    await worker_task
    await redis.aclose()

    print("\n" + "=" * 60)
    print("DOSSIER RESULT")
    print("=" * 60)
    print(f"Run ID:            {result.run_id}")
    print(f"Campaign ID:       {result.campaign_id}")
    print(f"Tenant ID:         {result.tenant_id}")
    print(f"Trace ID:          {result.trace_id}")
    print(f"Status:            {result.status}")
    print(f"Target Cards:      {result.target_cards_count}")
    print(f"Drafts:            {result.drafts_count}")
    print(f"Total Cost:        ${result.total_cost_dollars:.4f}")
    print(f"Completed At:      {result.completed_at}")
    if result.error_message:
        print(f"Error:             {result.error_message}")
    print("=" * 60)

    return result


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result.status == "completed" else 1)
