"""Reservation repository."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class ReservationRepo(BaseRepo):
    """Repository for reservations table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_reservation(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        reserved_until: datetime,
        reason: str | None,
    ) -> UUID:
        """Create a new reservation."""
        reservation_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO reservations (id, tenant_id, influencer_id, reserved_until, reason, created_at, updated_at)
                VALUES (:id, :tenant_id, :influencer_id, :reserved_until, :reason, :created_at, :updated_at)
            """),
            {
                "id": reservation_id,
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "reserved_until": reserved_until,
                "reason": reason,
                "created_at": now,
                "updated_at": now,
            },
        )
        return reservation_id

    async def clear_expired_reservations(
        self, tenant_id: UUID, now: datetime | None = None
    ) -> int:
        """Delete expired reservations for a specific tenant.

        Args:
            tenant_id: Tenant identifier (required for isolation)
            now: Timestamp to compare against (defaults to current UTC time)

        Returns:
            Number of deleted reservations
        """
        if now is None:
            now = datetime.now(timezone.utc)
        result = await self.session.execute(
            text("""
                DELETE FROM reservations
                WHERE tenant_id = :tenant_id AND reserved_until < :now
            """),
            {"tenant_id": tenant_id, "now": now},
        )
        return result.rowcount

    async def list_active_reservations(
        self,
        tenant_id: UUID,
    ) -> list[dict[str, Any]]:
        """List active (non-expired) reservations for a tenant."""
        now = self.now()
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, reserved_until, reason, created_at, updated_at
                FROM reservations
                WHERE tenant_id = :tenant_id AND reserved_until >= :now
                ORDER BY reserved_until ASC
            """),
            {"tenant_id": tenant_id, "now": now},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def is_reserved(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
    ) -> bool:
        """Check if an influencer has an active reservation."""
        now = self.now()
        result = await self.session.execute(
            text("""
                SELECT 1 FROM reservations
                WHERE tenant_id = :tenant_id
                  AND influencer_id = :influencer_id
                  AND reserved_until >= :now
                LIMIT 1
            """),
            {"tenant_id": tenant_id, "influencer_id": influencer_id, "now": now},
        )
        return result.fetchone() is not None

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get reservation by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, reserved_until, reason, created_at, updated_at
                FROM reservations
                WHERE tenant_id = :tenant_id AND id = :reservation_id
            """),
            {"tenant_id": tenant_id, "reservation_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create reservation (BaseRepo interface)."""
        return await self.create_reservation(
            tenant_id=tenant_id,
            influencer_id=data["influencer_id"],
            reserved_until=data["reserved_until"],
            reason=data.get("reason"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update reservation."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE reservations
                SET reserved_until = COALESCE(:reserved_until, reserved_until),
                    reason = COALESCE(:reason, reason),
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :reservation_id
            """),
            {
                "tenant_id": tenant_id,
                "reservation_id": entity_id,
                "reserved_until": data.get("reserved_until"),
                "reason": data.get("reason"),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete reservation."""
        result = await self.session.execute(
            text("""
                DELETE FROM reservations
                WHERE tenant_id = :tenant_id AND id = :reservation_id
            """),
            {"tenant_id": tenant_id, "reservation_id": entity_id},
        )
        return result.rowcount > 0
