"""Integration tests for repository layer."""

import asyncio
from uuid import uuid4

import pytest

from metismedia.db.session import db_session
from metismedia.db.repos import (
    CampaignRepo,
    EmbeddingRepo,
    InfluencerRepo,
    ReservationRepo,
)


@pytest.fixture
def tenant_id():
    """Generate a unique tenant ID for test isolation."""
    return uuid4()


@pytest.fixture
def other_tenant_id():
    """Generate a different tenant ID for isolation tests."""
    return uuid4()


@pytest.mark.asyncio
async def test_create_and_get_campaign(tenant_id):
    """Test creating and retrieving a campaign."""
    async with db_session() as session:
        repo = CampaignRepo(session)

        campaign_id = await repo.create_campaign(
            tenant_id=tenant_id,
            trace_id="trace-123",
            run_id="run-456",
            brief_json={"name": "Test Campaign", "goal": "engagement"},
        )
        await session.commit()

        campaign = await repo.get_campaign(tenant_id, campaign_id)

        assert campaign is not None
        assert campaign["id"] == campaign_id
        assert campaign["tenant_id"] == tenant_id
        assert campaign["trace_id"] == "trace-123"


@pytest.mark.asyncio
async def test_create_embedding_with_vector(tenant_id):
    """Test creating an embedding with a vector."""
    async with db_session() as session:
        repo = EmbeddingRepo(session)

        vector = [0.1] * 1536
        embedding_id = await repo.create_embedding(
            tenant_id=tenant_id,
            kind="bio",
            model="text-embedding-3-small",
            dims=1536,
            norm="l2",
            vector=vector,
        )
        await session.commit()

        meta = await repo.get_embedding_meta(tenant_id, embedding_id)

        assert meta is not None
        assert meta["id"] == embedding_id
        assert meta["kind"] == "bio"
        assert meta["embedding_model"] == "text-embedding-3-small"


@pytest.mark.asyncio
async def test_upsert_influencer(tenant_id):
    """Test upserting an influencer."""
    async with db_session() as session:
        repo = InfluencerRepo(session)

        influencer_id = await repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Test Influencer",
            primary_url="https://example.com/test",
            platform="substack",
            follower_count=1000,
        )
        await session.commit()

        influencer = await repo.get_by_id(tenant_id, influencer_id)

        assert influencer is not None
        assert influencer["canonical_name"] == "Test Influencer"
        assert influencer["follower_count"] == 1000


@pytest.mark.asyncio
async def test_vector_search_ordering(tenant_id):
    """Test that vector search returns results ordered by similarity."""
    async with db_session() as session:
        emb_repo = EmbeddingRepo(session)
        inf_repo = InfluencerRepo(session)

        base_vector = [1.0] + [0.0] * 1535
        similar_vector = [0.9, 0.1] + [0.0] * 1534
        dissimilar_vector = [0.0] * 1535 + [1.0]

        query_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="bio", model="test", dims=1536, norm="l2", vector=base_vector
        )
        similar_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="bio", model="test", dims=1536, norm="l2", vector=similar_vector
        )
        dissimilar_emb_id = await emb_repo.create_embedding(
            tenant_id=tenant_id, kind="bio", model="test", dims=1536, norm="l2", vector=dissimilar_vector
        )

        similar_inf_id = await inf_repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Similar Influencer",
            primary_url="https://example.com/similar",
            bio_embedding_id=similar_emb_id,
        )
        dissimilar_inf_id = await inf_repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Dissimilar Influencer",
            primary_url="https://example.com/dissimilar",
            bio_embedding_id=dissimilar_emb_id,
        )

        await session.commit()

        results = await inf_repo.vector_search_by_embedding_id(
            tenant_id=tenant_id,
            embedding_id=query_emb_id,
            kind="bio",
            limit=10,
        )

        assert len(results) == 2
        assert results[0][0] == similar_inf_id
        assert results[1][0] == dissimilar_inf_id
        assert results[0][1] > results[1][1]


@pytest.mark.asyncio
async def test_tenant_isolation(tenant_id, other_tenant_id):
    """Test that tenants cannot see each other's data."""
    async with db_session() as session:
        repo = CampaignRepo(session)

        campaign_id = await repo.create_campaign(
            tenant_id=tenant_id,
            trace_id="trace-abc",
            run_id="run-xyz",
            brief_json={"name": "Tenant A Campaign"},
        )
        await session.commit()

        own_campaign = await repo.get_campaign(tenant_id, campaign_id)
        assert own_campaign is not None

        other_campaign = await repo.get_campaign(other_tenant_id, campaign_id)
        assert other_campaign is None


@pytest.mark.asyncio
async def test_influencer_find_by_url(tenant_id):
    """Test finding influencer by primary URL."""
    async with db_session() as session:
        repo = InfluencerRepo(session)

        url = "https://example.com/unique-url"
        await repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="URL Test",
            primary_url=url,
        )
        await session.commit()

        found = await repo.find_by_primary_url(tenant_id, url)

        assert found is not None
        assert found["canonical_name"] == "URL Test"

        not_found = await repo.find_by_primary_url(tenant_id, "https://example.com/nonexistent")
        assert not_found is None


@pytest.mark.asyncio
async def test_clear_expired_reservations_tenant_isolation(tenant_id, other_tenant_id):
    """Test that clear_expired_reservations only deletes reservations for the specified tenant."""
    from datetime import datetime, timedelta, timezone

    async with db_session() as session:
        res_repo = ReservationRepo(session)
        inf_repo = InfluencerRepo(session)

        # Create influencers for both tenants
        inf_a = await inf_repo.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name="Tenant A Influencer",
            primary_url=f"https://example.com/tenant-a-{tenant_id}",
        )
        inf_b = await inf_repo.upsert_influencer(
            tenant_id=other_tenant_id,
            canonical_name="Tenant B Influencer",
            primary_url=f"https://example.com/tenant-b-{other_tenant_id}",
        )
        await session.commit()

        # Create expired reservations for both tenants
        past_time = datetime.now(timezone.utc) - timedelta(hours=1)
        res_a1 = await res_repo.create_reservation(
            tenant_id=tenant_id,
            influencer_id=inf_a,
            reserved_until=past_time,
            reason="expired-a-1",
        )
        res_a2 = await res_repo.create_reservation(
            tenant_id=tenant_id,
            influencer_id=inf_a,
            reserved_until=past_time,
            reason="expired-a-2",
        )
        res_b1 = await res_repo.create_reservation(
            tenant_id=other_tenant_id,
            influencer_id=inf_b,
            reserved_until=past_time,
            reason="expired-b-1",
        )
        await session.commit()

        # Verify all reservations exist
        assert await res_repo.get_by_id(tenant_id, res_a1) is not None
        assert await res_repo.get_by_id(tenant_id, res_a2) is not None
        assert await res_repo.get_by_id(other_tenant_id, res_b1) is not None

        # Clear expired reservations for tenant A only
        deleted_count = await res_repo.clear_expired_reservations(tenant_id=tenant_id)
        await session.commit()

        # Verify tenant A reservations are deleted
        assert deleted_count == 2
        assert await res_repo.get_by_id(tenant_id, res_a1) is None
        assert await res_repo.get_by_id(tenant_id, res_a2) is None

        # Verify tenant B reservation still exists
        assert await res_repo.get_by_id(other_tenant_id, res_b1) is not None
