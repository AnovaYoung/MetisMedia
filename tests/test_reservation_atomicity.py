"""Tests for atomic reservation operations under concurrency."""

import asyncio
from uuid import uuid4

import pytest

from metismedia.db.session import db_session, get_session_factory
from metismedia.db.repos import EmbeddingRepo, InfluencerRepo
from metismedia.db.queries.node_b import reserve_top_influencers_for_review


@pytest.fixture
def tenant_id():
    """Generate a unique tenant ID for test isolation."""
    return uuid4()


async def seed_influencers(tenant_id, count: int = 20):
    """Seed influencers with embeddings for testing."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        influencer_ids = []
        for i in range(count):
            vector = [float(i) / count] + [0.0] * 1535
            emb_id = await emb_repo.create_embedding(
                tenant_id=tenant_id,
                kind="bio",
                model="test",
                dims=1536,
                norm="l2",
                vector=vector,
            )

            inf_id = await inf_repo.upsert_influencer(
                tenant_id=tenant_id,
                canonical_name=f"Influencer {i}",
                primary_url=f"https://example.com/inf-{tenant_id}-{i}",
                bio_embedding_id=emb_id,
            )
            influencer_ids.append(inf_id)

        await session.commit()
        return influencer_ids


async def seed_query_embedding(tenant_id):
    """Create a query embedding for similarity search."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        vector = [0.5] + [0.0] * 1535
        emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id,
            kind="campaign",
            model="test",
            dims=1536,
            norm="l2",
            vector=vector,
        )
        await session.commit()
        return emb_id


async def reserve_task(tenant_id, query_embedding_id, limit: int = 5, task_id: int = 0):
    """Task that attempts to reserve influencers."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            reserved = await reserve_top_influencers_for_review(
                session=session,
                tenant_id=tenant_id,
                query_embedding_id=query_embedding_id,
                limit=limit,
                reason=f"task-{task_id}",
            )
        return [(r.influencer_id, r.reservation_id) for r in reserved]


@pytest.mark.asyncio
async def test_no_double_reservation(tenant_id):
    """Test that concurrent reservation attempts do not reserve the same influencer twice."""
    await seed_influencers(tenant_id, count=20)
    query_emb_id = await seed_query_embedding(tenant_id)

    results = await asyncio.gather(
        reserve_task(tenant_id, query_emb_id, limit=10, task_id=1),
        reserve_task(tenant_id, query_emb_id, limit=10, task_id=2),
    )

    all_influencer_ids = []
    for task_result in results:
        for inf_id, _ in task_result:
            all_influencer_ids.append(inf_id)

    unique_ids = set(all_influencer_ids)
    assert len(unique_ids) == len(all_influencer_ids), "Duplicate reservation detected!"

    total_reserved = sum(len(r) for r in results)
    assert total_reserved <= 20, "More reservations than available influencers"


@pytest.mark.asyncio
async def test_reservation_excludes_do_not_contact(tenant_id):
    """Test that do_not_contact influencers are excluded from reservation."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        from sqlalchemy import text

        vector = [0.5] + [0.0] * 1535
        emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="bio", model="test", dims=1536, norm="l2", vector=vector
        )

        inf_id = await inf_repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Blocked Influencer",
            primary_url=f"https://example.com/blocked-{tenant_id}",
            bio_embedding_id=emb_id,
        )

        await session.execute(
            text("UPDATE influencers SET do_not_contact = true WHERE id = :id"),
            {"id": inf_id},
        )

        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="campaign", model="test", dims=1536, norm="l2", vector=vector
        )

        await session.commit()

    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            reserved = await reserve_top_influencers_for_review(
                session=session,
                tenant_id=tenant_id,
                query_embedding_id=query_emb_id,
                limit=10,
            )

    reserved_ids = [r.influencer_id for r in reserved]
    assert inf_id not in reserved_ids, "do_not_contact influencer was reserved"


@pytest.mark.asyncio
async def test_reservation_excludes_cooling_off(tenant_id):
    """Test that cooling_off influencers are excluded from reservation."""
    from datetime import datetime, timedelta, timezone

    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        from sqlalchemy import text

        vector = [0.5] + [0.0] * 1535
        emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="bio", model="test", dims=1536, norm="l2", vector=vector
        )

        inf_id = await inf_repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Cooling Off Influencer",
            primary_url=f"https://example.com/cooling-{tenant_id}",
            bio_embedding_id=emb_id,
        )

        future = datetime.now(timezone.utc) + timedelta(days=7)
        await session.execute(
            text("UPDATE influencers SET cooling_off_until = :until WHERE id = :id"),
            {"id": inf_id, "until": future},
        )

        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="campaign", model="test", dims=1536, norm="l2", vector=vector
        )

        await session.commit()

    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            reserved = await reserve_top_influencers_for_review(
                session=session,
                tenant_id=tenant_id,
                query_embedding_id=query_emb_id,
                limit=10,
            )

    reserved_ids = [r.influencer_id for r in reserved]
    assert inf_id not in reserved_ids, "cooling_off influencer was reserved"
