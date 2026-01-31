"""Influencer repository with pgvector similarity search."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class InfluencerRepo(BaseRepo):
    """Repository for influencers table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_influencer(
        self,
        tenant_id: UUID,
        canonical_name: str,
        primary_url: str | None = None,
        platform: str | None = None,
        geography: str | None = None,
        follower_count: int | None = None,
        commercial_mode: str | None = None,
        polarity_score: int | None = None,
        bio_embedding_id: UUID | None = None,
        recent_embedding_id: UUID | None = None,
        bio_text: str | None = None,
        last_scraped_at: datetime | None = None,
        last_pulse_checked_at: datetime | None = None,
    ) -> UUID:
        """Upsert an influencer (insert or update on conflict)."""
        influencer_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO influencers (
                    id, tenant_id, canonical_name, primary_url, platform, geography,
                    follower_count, commercial_mode, polarity_score,
                    bio_embedding_id, recent_embedding_id, bio_text,
                    last_scraped_at, last_pulse_checked_at,
                    created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, :canonical_name, :primary_url, :platform, :geography,
                    :follower_count, :commercial_mode, :polarity_score,
                    :bio_embedding_id, :recent_embedding_id, :bio_text,
                    :last_scraped_at, :last_pulse_checked_at,
                    :created_at, :updated_at
                )
                ON CONFLICT (tenant_id, primary_url) WHERE primary_url IS NOT NULL
                DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    platform = COALESCE(EXCLUDED.platform, influencers.platform),
                    geography = COALESCE(EXCLUDED.geography, influencers.geography),
                    follower_count = COALESCE(EXCLUDED.follower_count, influencers.follower_count),
                    commercial_mode = COALESCE(EXCLUDED.commercial_mode, influencers.commercial_mode),
                    polarity_score = COALESCE(EXCLUDED.polarity_score, influencers.polarity_score),
                    bio_embedding_id = COALESCE(EXCLUDED.bio_embedding_id, influencers.bio_embedding_id),
                    recent_embedding_id = COALESCE(EXCLUDED.recent_embedding_id, influencers.recent_embedding_id),
                    bio_text = COALESCE(EXCLUDED.bio_text, influencers.bio_text),
                    last_scraped_at = COALESCE(EXCLUDED.last_scraped_at, influencers.last_scraped_at),
                    last_pulse_checked_at = COALESCE(EXCLUDED.last_pulse_checked_at, influencers.last_pulse_checked_at),
                    updated_at = EXCLUDED.updated_at
                RETURNING id
            """),
            {
                "id": influencer_id,
                "tenant_id": tenant_id,
                "canonical_name": canonical_name,
                "primary_url": primary_url,
                "platform": platform,
                "geography": geography,
                "follower_count": follower_count,
                "commercial_mode": commercial_mode,
                "polarity_score": polarity_score,
                "bio_embedding_id": bio_embedding_id,
                "recent_embedding_id": recent_embedding_id,
                "bio_text": bio_text,
                "last_scraped_at": last_scraped_at,
                "last_pulse_checked_at": last_pulse_checked_at,
                "created_at": now,
                "updated_at": now,
            },
        )
        result = await self.session.execute(
            text("""
                SELECT id FROM influencers
                WHERE tenant_id = :tenant_id AND primary_url = :primary_url
            """),
            {"tenant_id": tenant_id, "primary_url": primary_url},
        )
        row = result.fetchone()
        return row[0] if row else influencer_id

    async def find_by_primary_url(
        self,
        tenant_id: UUID,
        url: str,
    ) -> dict[str, Any] | None:
        """Find influencer by primary URL."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, canonical_name, primary_url, platform, geography,
                       follower_count, commercial_mode, polarity_score,
                       bio_embedding_id, recent_embedding_id, bio_text,
                       last_scraped_at, last_pulse_checked_at, do_not_contact, cooling_off_until,
                       created_at, updated_at
                FROM influencers
                WHERE tenant_id = :tenant_id AND primary_url = :url
            """),
            {"tenant_id": tenant_id, "url": url},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def vector_search_by_embedding_id(
        self,
        tenant_id: UUID,
        embedding_id: UUID,
        kind: str = "bio",
        limit: int = 10,
    ) -> list[tuple[UUID, float]]:
        """Search influencers by vector similarity.

        Returns list of (influencer_id, similarity) tuples ordered by similarity desc.
        Similarity is 1 - cosine_distance (so higher is more similar).
        """
        fk_column = "bio_embedding_id" if kind == "bio" else "recent_embedding_id"

        result = await self.session.execute(
            text(f"""
                WITH query_vec AS (
                    SELECT vector FROM embeddings WHERE id = :embedding_id AND tenant_id = :tenant_id
                )
                SELECT i.id, 1 - (e.vector <=> (SELECT vector FROM query_vec)) as similarity
                FROM influencers i
                JOIN embeddings e ON i.{fk_column} = e.id
                WHERE i.tenant_id = :tenant_id
                  AND e.tenant_id = :tenant_id
                  AND (SELECT vector FROM query_vec) IS NOT NULL
                ORDER BY e.vector <=> (SELECT vector FROM query_vec)
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "embedding_id": embedding_id, "limit": limit},
        )
        return [(row[0], row[1]) for row in result.fetchall()]

    async def update_last_scraped_at(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        scraped_at: datetime | None = None,
    ) -> bool:
        """Update last_scraped_at timestamp."""
        now = scraped_at or self.now()
        result = await self.session.execute(
            text("""
                UPDATE influencers
                SET last_scraped_at = :scraped_at, updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :influencer_id
            """),
            {
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "scraped_at": now,
                "updated_at": self.now(),
            },
        )
        return result.rowcount > 0

    async def update_last_pulse_checked_at(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        checked_at: datetime | None = None,
    ) -> bool:
        """Update last_pulse_checked_at timestamp."""
        now = checked_at or self.now()
        result = await self.session.execute(
            text("""
                UPDATE influencers
                SET last_pulse_checked_at = :checked_at, updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :influencer_id
            """),
            {
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "checked_at": now,
                "updated_at": self.now(),
            },
        )
        return result.rowcount > 0

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get influencer by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, canonical_name, primary_url, platform, geography,
                       follower_count, commercial_mode, polarity_score,
                       bio_embedding_id, recent_embedding_id, bio_text,
                       last_scraped_at, last_pulse_checked_at, do_not_contact, cooling_off_until,
                       created_at, updated_at
                FROM influencers
                WHERE tenant_id = :tenant_id AND id = :influencer_id
            """),
            {"tenant_id": tenant_id, "influencer_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create influencer (BaseRepo interface)."""
        return await self.upsert_influencer(
            tenant_id=tenant_id,
            canonical_name=data["canonical_name"],
            primary_url=data.get("primary_url"),
            platform=data.get("platform"),
            geography=data.get("geography"),
            follower_count=data.get("follower_count"),
            commercial_mode=data.get("commercial_mode"),
            polarity_score=data.get("polarity_score"),
            bio_embedding_id=data.get("bio_embedding_id"),
            recent_embedding_id=data.get("recent_embedding_id"),
            bio_text=data.get("bio_text"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update influencer fields."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE influencers
                SET canonical_name = COALESCE(:canonical_name, canonical_name),
                    platform = COALESCE(:platform, platform),
                    geography = COALESCE(:geography, geography),
                    follower_count = COALESCE(:follower_count, follower_count),
                    commercial_mode = COALESCE(:commercial_mode, commercial_mode),
                    polarity_score = COALESCE(:polarity_score, polarity_score),
                    bio_text = COALESCE(:bio_text, bio_text),
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :influencer_id
            """),
            {
                "tenant_id": tenant_id,
                "influencer_id": entity_id,
                "canonical_name": data.get("canonical_name"),
                "platform": data.get("platform"),
                "geography": data.get("geography"),
                "follower_count": data.get("follower_count"),
                "commercial_mode": data.get("commercial_mode"),
                "polarity_score": data.get("polarity_score"),
                "bio_text": data.get("bio_text"),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete influencer."""
        result = await self.session.execute(
            text("""
                DELETE FROM influencers
                WHERE tenant_id = :tenant_id AND id = :influencer_id
            """),
            {"tenant_id": tenant_id, "influencer_id": entity_id},
        )
        return result.rowcount > 0
