"""End-to-end tests for the event-driven orchestrator demo run."""

import asyncio
import json
import logging
from uuid import UUID, uuid4

import pytest

from metismedia.contracts.enums import CommercialMode, NodeName, PolarityIntent
from metismedia.contracts.models import CampaignBrief
from metismedia.core import Budget, CostEntry, CostLedger, InMemoryLedger, JsonLogLedger
from metismedia.db.repos import (
    DraftRepo,
    EmbeddingRepo,
    InfluencerRepo,
    RunRepo,
    TargetCardRepo,
)
from metismedia.db.session import db_session
from metismedia.events import EventBus, EventEnvelope, Worker, make_idempotency_key
from metismedia.orchestration import DossierResult, Orchestrator
from metismedia.orchestration.registry import build_handler_registry


@pytest.fixture
def tenant_id():
    return uuid4()


@pytest.fixture
def in_memory_ledger():
    return InMemoryLedger()


async def seed_influencers(tenant_id, count: int = 20) -> str:
    """Seed influencers with embeddings, return query embedding ID."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        query_vector = [1.0] + [0.0] * 1535
        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="test",
            dims=1536,
            norm="l2",
            vector=query_vector,
        )

        for i in range(count):
            similarity_offset = 0.05 * (i % 10)
            vec = [1.0 - similarity_offset, similarity_offset] + [0.0] * 1534

            bio_emb_id = await emb_repo.create_embedding(
                tenant_id=tenant_id,
                kind="bio",
                model="test",
                dims=1536,
                norm="l2",
                vector=vec,
            )

            await inf_repo.upsert_influencer(
                tenant_id=tenant_id,
                canonical_name=f"Test Influencer {i + 1}",
                primary_url=f"https://test.example.com/inf-{i + 1}-{tenant_id}",
                platform="substack",
                follower_count=1000 * (i + 1),
                bio_embedding_id=bio_emb_id,
                bio_text=f"Test bio for influencer {i + 1}",
            )

        await session.commit()

        return str(query_emb_id)


async def run_event_driven_flow(
    tenant_id,
    brief: CampaignBrief,
    clean_redis,
    budget: Budget,
    ledger: CostLedger | None,
    timeout_s: float = 30.0,
    max_poll_iterations: int = 800,
) -> DossierResult:
    """Run event-driven flow: start_run, Worker, await_completion."""
    redis = clean_redis
    bus = EventBus(redis)
    orchestrator = Orchestrator(
        bus=bus,
        poll_interval_seconds=0.05,
        max_poll_iterations=max_poll_iterations,  # 600 -> ~30s at 0.05s poll
    )
    handler_registry = build_handler_registry(budget=budget, ledger=ledger, bus=bus)
    worker = Worker(redis, bus, consumer_name="e2e-worker")

    run_id = await orchestrator.start_run(tenant_id=tenant_id, brief=brief)
    worker_task = asyncio.create_task(
        worker.run(handler_registry, budget=budget, ledger=ledger)
    )
    result = await orchestrator.await_completion(
        tenant_id=tenant_id, run_id=run_id, timeout_s=timeout_s
    )
    worker.stop()
    await worker_task
    return result


@pytest.mark.asyncio
async def test_orchestrator_e2e_creates_target_cards_and_drafts(
    tenant_id, in_memory_ledger, clean_redis
):
    """E2E test: event-driven orchestrator creates target cards and drafts."""
    query_embedding_id = await seed_influencers(tenant_id, count=20)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="E2E Test Campaign",
        description="Testing the full orchestrator pipeline",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    budget = Budget(
        max_dollars=10.0,
        max_provider_calls={"mock_discovery": 50, "mock_llm": 50},
    )

    result = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=budget,
        ledger=in_memory_ledger,
    )

    assert result.status == "completed", f"Run failed: {result.error_message}"
    assert result.target_cards_count >= 1, "Expected at least 1 target card"
    assert result.drafts_count >= 1, "Expected at least 1 draft"
    assert result.total_cost_dollars > 0, "Expected non-zero total cost when nodes record costs"
    assert "by_node" in result.cost_summary or "by_provider" in result.cost_summary, (
        "Expected cost_summary to contain by_node or by_provider"
    )

    async with db_session() as session:
        tc_repo = TargetCardRepo(session)
        draft_repo = DraftRepo(session)

        target_cards = await tc_repo.list_target_cards(tenant_id, result.campaign_id)
        drafts = await draft_repo.list_drafts(tenant_id, result.campaign_id)

        assert len(target_cards) >= 1
        assert len(drafts) >= 1

        for tc in target_cards:
            assert tc["tenant_id"] == tenant_id
            assert tc["campaign_id"] == result.campaign_id

        for draft in drafts:
            assert draft["tenant_id"] == tenant_id
            assert draft["campaign_id"] == result.campaign_id
            assert draft["status"] == "draft"


@pytest.mark.asyncio
async def test_orchestrator_e2e_records_costs(
    tenant_id, in_memory_ledger, clean_redis
):
    """E2E test: event-driven flow records cost entries."""
    query_embedding_id = await seed_influencers(tenant_id, count=5)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Cost Tracking Test",
        description="Testing cost recording",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    budget = Budget(max_dollars=5.0)

    result = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=budget,
        ledger=in_memory_ledger,
    )

    assert result.status == "completed"

    assert len(in_memory_ledger.entries) > 0, "Expected cost entries to be recorded"
    assert result.total_cost_dollars > 0, "Expected non-zero total_cost_dollars in result"
    assert "by_node" in result.cost_summary or "by_provider" in result.cost_summary, (
        "Expected cost_summary to contain by_node or by_provider"
    )

    providers = {e.provider for e in in_memory_ledger.entries}
    assert (
        "internal" in providers
        or "postgres" in providers
        or "mock_discovery" in providers
    )

    for entry in in_memory_ledger.entries:
        assert entry.tenant_id == tenant_id
        assert entry.trace_id is not None
        assert entry.dollars >= 0


@pytest.mark.asyncio
async def test_orchestrator_e2e_cost_log_output(tenant_id, caplog, clean_redis):
    """E2E test: verify cost logs are emitted to metismedia.cost logger."""
    query_embedding_id = await seed_influencers(tenant_id, count=3)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Log Test Campaign",
        description="Testing cost log output",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    ledger = JsonLogLedger()
    budget = Budget(max_dollars=5.0)

    with caplog.at_level(logging.INFO, logger="metismedia.cost"):
        result = await run_event_driven_flow(
            tenant_id=tenant_id,
            brief=brief,
            clean_redis=clean_redis,
            budget=budget,
            ledger=ledger,
        )

    assert result.status == "completed"

    cost_log_messages = [
        record.message
        for record in caplog.records
        if record.name == "metismedia.cost"
    ]

    assert len(cost_log_messages) > 0, "Expected cost log messages"

    for msg in cost_log_messages:
        assert "tenant_id" in msg or "provider" in msg or "dollars" in msg


@pytest.mark.asyncio
async def test_orchestrator_e2e_handles_no_influencers(
    tenant_id, in_memory_ledger, clean_redis
):
    """E2E test: event-driven flow handles case with no matching influencers."""
    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Empty Campaign",
        description="No influencers seeded",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
    )

    result = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=Budget(max_dollars=5.0),
        ledger=in_memory_ledger,
    )

    assert result.status == "completed"
    assert result.target_cards_count == 0
    assert result.drafts_count == 0


@pytest.mark.asyncio
async def test_orchestrator_e2e_deterministic_results(tenant_id, clean_redis):
    """E2E test: verify results are deterministic across runs."""
    query_embedding_id = await seed_influencers(tenant_id, count=10)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Determinism Test",
        description="Testing deterministic behavior",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    ledger1 = InMemoryLedger()
    result1 = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=Budget(max_dollars=5.0),
        ledger=ledger1,
    )

    tenant_id_2 = uuid4()
    query_embedding_id_2 = await seed_influencers(tenant_id_2, count=10)

    brief2 = CampaignBrief(
        tenant_id=tenant_id_2,
        name="Determinism Test",
        description="Testing deterministic behavior",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id_2},
    )

    ledger2 = InMemoryLedger()
    result2 = await run_event_driven_flow(
        tenant_id=tenant_id_2,
        brief=brief2,
        clean_redis=clean_redis,
        budget=Budget(max_dollars=5.0),
        ledger=ledger2,
    )

    assert result1.status == result2.status
    assert result1.target_cards_count == result2.target_cards_count
    assert result1.drafts_count == result2.drafts_count


@pytest.mark.asyncio
async def test_duplicate_publish_same_idem_key_does_not_double_execute(
    tenant_id, in_memory_ledger, clean_redis
):
    """Duplicate publish with same idempotency key is skipped; run stays correct."""
    query_embedding_id = await seed_influencers(tenant_id, count=1)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Idempotency Test",
        description="Testing duplicate event skip",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    budget = Budget(max_dollars=5.0)
    result = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=budget,
        ledger=in_memory_ledger,
    )

    assert result.status == "completed"
    assert result.target_cards_count == 1
    assert result.drafts_count == 1

    async with db_session() as session:
        tc_repo = TargetCardRepo(session)
        target_cards = await tc_repo.list_target_cards(tenant_id, result.campaign_id)
    assert len(target_cards) == 1
    influencer_id = str(target_cards[0]["influencer_id"])

    idem_key = make_idempotency_key(
        tenant_id=tenant_id,
        run_id=result.run_id,
        node=NodeName.G,
        event_name="node_g.input",
        step=f"finalize:{influencer_id}",
    )
    duplicate_envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.G,
        event_name="node_g.input",
        trace_id=result.trace_id or "",
        run_id=str(result.run_id),
        idempotency_key=idem_key,
        payload={"campaign_id": str(result.campaign_id), "influencer_id": influencer_id},
    )
    bus = EventBus(clean_redis)
    await bus.publish(duplicate_envelope)

    handler_registry = build_handler_registry(budget=budget, ledger=in_memory_ledger, bus=bus)
    worker = Worker(clean_redis, bus, consumer_name="idem-dup-test")
    await worker.run(handler_registry, stop_after=1, budget=budget, ledger=in_memory_ledger)

    async with db_session() as session:
        run_repo = RunRepo(session)
        row = await run_repo.get_by_id(tenant_id, result.run_id)
    assert row is not None
    rj = row.get("result_json")
    if isinstance(rj, str):
        rj = json.loads(rj) if rj else {}
    assert rj.get("target_cards_count") == 1, "Duplicate node_g should not double target cards"
    assert rj.get("drafts_count") == 1, "Duplicate node_g should not double drafts"


@pytest.mark.asyncio
async def test_budget_exceeded_fails_run_no_drafts(tenant_id, in_memory_ledger, clean_redis):
    """Budget(max_dollars=0.01) causes run to fail with Budget exceeded; no drafts created."""
    query_embedding_id = await seed_influencers(tenant_id, count=2)

    brief = CampaignBrief(
        tenant_id=tenant_id,
        name="Budget Cap Test",
        description="Run should fail when budget exceeded",
        polarity_intent=PolarityIntent.ALLIES,
        commercial_mode=CommercialMode.EARNED,
        finalized=True,
        slot_values={"query_embedding_id": query_embedding_id},
    )

    budget = Budget(max_dollars=0.01)

    result = await run_event_driven_flow(
        tenant_id=tenant_id,
        brief=brief,
        clean_redis=clean_redis,
        budget=budget,
        ledger=in_memory_ledger,
        timeout_s=45.0,
        max_poll_iterations=900,
    )

    async with db_session() as session:
        run_repo = RunRepo(session)
        row = await run_repo.get_by_id(tenant_id, result.run_id)
    assert row, "Run should exist"
    status = row.get("status") or result.status
    error_msg = row.get("error_message") or result.error_message or ""
    assert status == "failed", f"Run should be failed; got status={status}"
    assert "Budget exceeded" in error_msg, (
        f"Run should fail with Budget exceeded; got error_message={error_msg!r}"
    )

    async with db_session() as session:
        draft_repo = DraftRepo(session)
        drafts = await draft_repo.list_drafts(tenant_id, result.campaign_id)
    assert len(drafts) == 0, "Expected no drafts when run fails on budget"
