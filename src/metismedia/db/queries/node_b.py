"""Node B atomic operations for influencer reservation."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class ReservedInfluencer:
    """Result of a reservation operation."""

    influencer_id: UUID
    similarity: float
    reservation_id: UUID


async def reserve_top_influencers_for_review(
    session: AsyncSession,
    tenant_id: UUID,
    query_embedding_id: UUID,
    limit: int = 10,
    reservation_duration_minutes: int = 30,
    reason: str | None = None,
    kind: str = "bio",
) -> list[ReservedInfluencer]:
    """Atomically select and reserve top influencers by vector similarity.

    This function runs inside a single transaction and uses SELECT ... FOR UPDATE
    SKIP LOCKED to safely handle concurrent reservation attempts.

    Exclusions:
    - do_not_contact = true
    - cooling_off_until > now
    - active reservations (reserved_until > now)

    Args:
        session: AsyncSession (must be inside a transaction)
        tenant_id: Tenant ID for scoping
        query_embedding_id: ID of the embedding to use for similarity search
        limit: Maximum number of influencers to reserve
        reservation_duration_minutes: How long the reservation lasts
        reason: Optional reason for the reservation
        kind: Embedding kind to use ('bio' or 'recent')

    Returns:
        List of ReservedInfluencer with influencer_id, similarity, and reservation_id
    """
    now = datetime.now(timezone.utc)
    reserved_until = now + timedelta(minutes=reservation_duration_minutes)

    fk_column = "bio_embedding_id" if kind == "bio" else "recent_embedding_id"

    result = await session.execute(
        text(f"""
            WITH query_vec AS (
                SELECT vector FROM embeddings
                WHERE id = :query_embedding_id AND tenant_id = :tenant_id
            ),
            eligible AS (
                SELECT
                    i.id as influencer_id,
                    1 - (e.vector <=> (SELECT vector FROM query_vec)) as similarity
                FROM influencers i
                JOIN embeddings e ON i.{fk_column} = e.id
                WHERE i.tenant_id = :tenant_id
                  AND e.tenant_id = :tenant_id
                  AND i.do_not_contact = false
                  AND (i.cooling_off_until IS NULL OR i.cooling_off_until <= :now)
                  AND (SELECT vector FROM query_vec) IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM reservations r
                      WHERE r.tenant_id = :tenant_id
                        AND r.influencer_id = i.id
                        AND r.reserved_until > :now
                  )
                ORDER BY e.vector <=> (SELECT vector FROM query_vec)
                LIMIT :limit
                FOR UPDATE OF i SKIP LOCKED
            )
            SELECT influencer_id, similarity FROM eligible
        """),
        {
            "tenant_id": tenant_id,
            "query_embedding_id": query_embedding_id,
            "now": now,
            "limit": limit,
        },
    )

    selected = result.fetchall()

    if not selected:
        return []

    reserved: list[ReservedInfluencer] = []
    for influencer_id, similarity in selected:
        reservation_id = uuid4()
        await session.execute(
            text("""
                INSERT INTO reservations (id, tenant_id, influencer_id, reserved_until, reason, created_at, updated_at)
                VALUES (:id, :tenant_id, :influencer_id, :reserved_until, :reason, :now, :now)
            """),
            {
                "id": reservation_id,
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "reserved_until": reserved_until,
                "reason": reason,
                "now": now,
            },
        )
        reserved.append(
            ReservedInfluencer(
                influencer_id=influencer_id,
                similarity=similarity,
                reservation_id=reservation_id,
            )
        )

    return reserved
