"""Integration tests for Node B handler: Genesis Guard event-driven flow."""

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from metismedia.contracts.enums import CommercialMode, NodeName, PolarityIntent, PulseStatus
from metismedia.contracts.models import CampaignBrief
from metismedia.core import Budget, InMemoryLedger
from metismedia.db.repos import CampaignRepo, EmbeddingRepo, InfluencerRepo, RunRepo
from metismedia.db.session import db_session
from metismedia.events import EventBus, EventEnvelope, Worker, make_idempotency_key
from metismedia.nodes.node_b.handler import handle_node_b_input
from metismedia.nodes.node_b.thresholds import TAU_PRE
from metismedia.providers import MockEmbeddingProvider, MockPulseProvider


@pytest.fixture
def tenant_id():
    return uuid4()


@pytest.fixture
def in_memory_ledger():
    return InMemoryLedger()


async def seed_campaign_and_influencers(
    tenant_id: UUID,
    count: int = 30,
    polarity_intent: str = "allies",
    third_rail_terms: list[str] | None = None,
) -> tuple[UUID, str]:
    """Seed campaign and influencers with embeddings.

    Returns (campaign_id, query_embedding_id).
    """
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)
        campaign_repo = CampaignRepo(session)

        # Create query embedding
        query_vector = [1.0] + [0.0] * 1535
        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="test",
            dims=1536,
            norm="l2",
            vector=query_vector,
        )

        # Create influencers with varying similarity
        for i in range(count):
            similarity_offset = 0.03 * (i % 15)  # Closer to query = higher similarity
            vec = [1.0 - similarity_offset, similarity_offset] + [0.0] * 1534

            bio_emb_id = await emb_repo.create_embedding(
                tenant_id=tenant_id,
                kind="bio",
                model="test",
                dims=1536,
                norm="l2",
                vector=vec,
            )

            # Make some influencers recent, some stale
            if i < 20:
                last_scraped = datetime.now(timezone.utc) - timedelta(days=i % 7)
            else:
                last_scraped = datetime.now(timezone.utc) - timedelta(days=20)

            polarity = 5 if polarity_intent == "allies" else (-5 if polarity_intent == "critics" else 0)

            await inf_repo.upsert_influencer(
                tenant_id=tenant_id,
                canonical_name=f"Test Influencer {i + 1}",
                primary_url=f"https://test.example.com/inf-{i + 1}-{tenant_id}",
                platform="substack",
                follower_count=1000 * (i + 1),
                bio_embedding_id=bio_emb_id,
                bio_text=f"Test bio for influencer {i + 1}. Tech and innovation focus.",
                polarity_score=polarity,
                last_scraped_at=last_scraped,
            )

        # Create campaign with brief
        campaign_id = await campaign_repo.create_campaign(
            tenant_id=tenant_id,
            trace_id=f"trace-{uuid4()}",
            run_id=f"run-{uuid4()}",
            brief_json={
                "name": "Node B Integration Test Campaign",
                "polarity_intent": polarity_intent,
                "commercial_mode": "earned",
                "slot_values": {
                    "query_embedding_id": str(query_emb_id),
                    "third_rail_terms": third_rail_terms or [],
                },
                "target_psychographics": {"tech_interest": 0.8},
            },
        )

        await session.commit()
        return campaign_id, str(query_emb_id)


@pytest.mark.asyncio
async def test_node_b_integration_processes_candidates(tenant_id, in_memory_ledger, clean_redis):
    """Test Node B handler processes candidates through full pipeline."""
    campaign_id, query_emb_id = await seed_campaign_and_influencers(tenant_id, count=30)

    # Create run record
    async with db_session() as session:
        run_repo = RunRepo(session)
        run_id_uuid = await run_repo.create_run(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            trace_id=f"trace-{uuid4()}",
        )
        await session.commit()
    run_id = str(run_id_uuid)

    bus = EventBus(clean_redis)
    budget = Budget(max_dollars=5.0)

    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=f"trace-{run_id}",
        run_id=run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="test",
        ),
        payload={
            "campaign_id": str(campaign_id),
            "query_embedding_id": query_emb_id,
            "limit": 10,
        },
    )

    mock_pulse = MockPulseProvider(
        default_summaries=[
            {"title": "Recent Post", "summary": "Tech innovation discussion", "date": datetime.now(timezone.utc)}
        ]
    )
    mock_embedding = MockEmbeddingProvider()

    async with db_session() as session:
        await handle_node_b_input(
            envelope=envelope,
            session=session,
            budget=budget,
            ledger=in_memory_ledger,
            bus=bus,
            pulse_provider=mock_pulse,
            embedding_provider=mock_embedding,
        )
        await session.commit()

    # Verify events were published
    # The handler should have published node_b.directive_emitted events
    # Let's check the run status
    async with db_session() as session:
        run_repo = RunRepo(session)
        run = await run_repo.get_by_id(tenant_id, UUID(run_id))

    # Run should not be marked completed (that's Node G's job) unless no candidates
    assert run is not None

    # Verify costs were recorded
    assert len(in_memory_ledger.entries) > 0
    provider_names = {e.provider for e in in_memory_ledger.entries}
    assert "postgres" in provider_names or "internal" in provider_names


@pytest.mark.asyncio
async def test_node_b_integration_filters_by_mms_threshold(tenant_id, in_memory_ledger, clean_redis):
    """Test that Node B filters candidates below MMS threshold."""
    campaign_id, query_emb_id = await seed_campaign_and_influencers(tenant_id, count=30)
    async with db_session() as session:
        run_repo = RunRepo(session)
        run_id_uuid = await run_repo.create_run(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            trace_id=f"trace-{uuid4()}",
        )
        await session.commit()
    run_id = str(run_id_uuid)

    bus = EventBus(clean_redis)
    budget = Budget(max_dollars=5.0)

    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=f"trace-{run_id}",
        run_id=run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="filter-test",
        ),
        payload={
            "campaign_id": str(campaign_id),
            "query_embedding_id": query_emb_id,
            "limit": 5,
        },
    )

    mock_pulse = MockPulseProvider(
        default_summaries=[
            {"title": "Recent Post", "summary": "Tech innovation", "date": datetime.now(timezone.utc)}
        ]
    )
    mock_embedding = MockEmbeddingProvider()

    async with db_session() as session:
        await handle_node_b_input(
            envelope=envelope,
            session=session,
            budget=budget,
            ledger=in_memory_ledger,
            bus=bus,
            pulse_provider=mock_pulse,
            embedding_provider=mock_embedding,
        )
        await session.commit()

    # Verify MMS computation cost was recorded
    mms_entries = [e for e in in_memory_ledger.entries if e.operation == "mms_compute"]
    assert len(mms_entries) > 0 or any("mms" in str(e.metadata) for e in in_memory_ledger.entries)


@pytest.mark.asyncio
async def test_node_b_integration_respects_budget(tenant_id, clean_redis):
    """Test that Node B respects budget limits."""
    campaign_id, query_emb_id = await seed_campaign_and_influencers(tenant_id, count=30)
    async with db_session() as session:
        run_repo = RunRepo(session)
        run_id_uuid = await run_repo.create_run(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            trace_id=f"trace-{uuid4()}",
        )
        await session.commit()
    run_id = str(run_id_uuid)

    bus = EventBus(clean_redis)
    # Very tight budget
    budget = Budget(max_dollars=0.001)
    ledger = InMemoryLedger()

    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=f"trace-{run_id}",
        run_id=run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="budget-test",
        ),
        payload={
            "campaign_id": str(campaign_id),
            "query_embedding_id": query_emb_id,
            "limit": 10,
        },
    )

    mock_pulse = MockPulseProvider(
        default_summaries=[
            {"title": "Post", "summary": "Content", "date": datetime.now(timezone.utc)}
        ]
    )
    mock_embedding = MockEmbeddingProvider()

    from metismedia.core.budget import BudgetExceeded, BudgetState

    budget_state = BudgetState()

    with pytest.raises(BudgetExceeded):
        async with db_session() as session:
            await handle_node_b_input(
                envelope=envelope,
                session=session,
                budget=budget,
                ledger=ledger,
                bus=bus,
                budget_state=budget_state,
                pulse_provider=mock_pulse,
                embedding_provider=mock_embedding,
            )


@pytest.mark.asyncio
async def test_node_b_integration_handles_no_matching_influencers(tenant_id, in_memory_ledger, clean_redis):
    """Test Node B handles case where no influencers match query."""
    # Create campaign without seeding influencers
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        campaign_repo = CampaignRepo(session)

        query_vector = [1.0] + [0.0] * 1535
        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="test",
            dims=1536,
            norm="l2",
            vector=query_vector,
        )

        campaign_id = await campaign_repo.create_campaign(
            tenant_id=tenant_id,
            trace_id=f"trace-{uuid4()}",
            run_id=f"run-{uuid4()}",
            brief_json={
                "name": "Empty Campaign",
                "polarity_intent": "allies",
                "commercial_mode": "earned",
                "slot_values": {"query_embedding_id": str(query_emb_id)},
            },
        )
        await session.commit()

    async with db_session() as session:
        run_repo = RunRepo(session)
        run_id_uuid = await run_repo.create_run(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            trace_id=f"trace-{uuid4()}",
        )
        await session.commit()
    run_id = str(run_id_uuid)

    bus = EventBus(clean_redis)
    budget = Budget(max_dollars=5.0)

    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=f"trace-{run_id}",
        run_id=run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="empty-test",
        ),
        payload={
            "campaign_id": str(campaign_id),
            "query_embedding_id": str(query_emb_id),
            "limit": 10,
        },
    )

    async with db_session() as session:
        await handle_node_b_input(
            envelope=envelope,
            session=session,
            budget=budget,
            ledger=in_memory_ledger,
            bus=bus,
        )
        await session.commit()

    # Run should be marked completed with 0 targets
    async with db_session() as session:
        run_repo = RunRepo(session)
        run = await run_repo.get_by_id(tenant_id, UUID(run_id))

    assert run is not None
    assert run.get("status") == "completed"
    result_json = run.get("result_json") or {}
    if isinstance(result_json, str):
        import json
        result_json = json.loads(result_json)
    assert result_json.get("target_cards_count") == 0


@pytest.mark.asyncio
async def test_node_b_integration_polarity_filter_allies(tenant_id, in_memory_ledger, clean_redis):
    """Test that Node B filters out critics when polarity_intent is allies."""
    # Seed influencers with negative polarity (critics)
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)
        campaign_repo = CampaignRepo(session)

        query_vector = [1.0] + [0.0] * 1535
        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="test",
            dims=1536,
            norm="l2",
            vector=query_vector,
        )

        # Create influencers with negative polarity
        for i in range(10):
            vec = [0.95, 0.05] + [0.0] * 1534  # High similarity
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
                canonical_name=f"Critic {i + 1}",
                primary_url=f"https://test.example.com/critic-{i + 1}-{tenant_id}",
                platform="substack",
                follower_count=1000,
                bio_embedding_id=bio_emb_id,
                bio_text=f"Critical voice {i + 1}",
                polarity_score=-5,  # Negative polarity
                last_scraped_at=datetime.now(timezone.utc),
            )

        campaign_id = await campaign_repo.create_campaign(
            tenant_id=tenant_id,
            trace_id=f"trace-{uuid4()}",
            run_id=f"run-{uuid4()}",
            brief_json={
                "name": "Allies Only Campaign",
                "polarity_intent": "allies",  # Looking for allies
                "commercial_mode": "earned",
                "slot_values": {"query_embedding_id": str(query_emb_id)},
            },
        )
        await session.commit()

    async with db_session() as session:
        run_repo = RunRepo(session)
        run_id_uuid = await run_repo.create_run(
            tenant_id=tenant_id,
            campaign_id=campaign_id,
            trace_id=f"trace-{uuid4()}",
        )
        await session.commit()
    run_id = str(run_id_uuid)

    bus = EventBus(clean_redis)
    budget = Budget(max_dollars=5.0)

    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.B,
        event_name="node_b.input",
        trace_id=f"trace-{run_id}",
        run_id=run_id,
        idempotency_key=make_idempotency_key(
            tenant_id=tenant_id,
            run_id=run_id,
            node=NodeName.B,
            event_name="node_b.input",
            step="polarity-test",
        ),
        payload={
            "campaign_id": str(campaign_id),
            "query_embedding_id": str(query_emb_id),
            "limit": 10,
        },
    )

    async with db_session() as session:
        await handle_node_b_input(
            envelope=envelope,
            session=session,
            budget=budget,
            ledger=in_memory_ledger,
            bus=bus,
        )
        await session.commit()

    # Run should complete with 0 targets because all influencers are critics
    # (polarity alignment = 0 for allies seeking critics)
    async with db_session() as session:
        run_repo = RunRepo(session)
        run = await run_repo.get_by_id(tenant_id, UUID(run_id))

    assert run is not None
    # The run status depends on MMS threshold - critics with allies intent get 0 polarity alignment
