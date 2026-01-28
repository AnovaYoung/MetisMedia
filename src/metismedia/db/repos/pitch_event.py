"""Pitch event repository."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class PitchEventRepo(BaseRepo):
    """Repository for pitch_events table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_event(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        campaign_id: UUID,
        event_type: str | None,
        channel: str | None,
        occurred_at: datetime | None,
        metadata: dict[str, Any] | None,
    ) -> UUID:
        """Insert a new pitch event."""
        event_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO pitch_events (
                    id, tenant_id, influencer_id, campaign_id,
                    event_type, channel, occurred_at, metadata,
                    created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, :influencer_id, :campaign_id,
                    :event_type, :channel, :occurred_at, :metadata,
                    :created_at, :updated_at
                )
            """),
            {
                "id": event_id,
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "campaign_id": campaign_id,
                "event_type": event_type,
                "channel": channel,
                "occurred_at": occurred_at or now,
                "metadata": json.dumps(metadata) if metadata else None,
                "created_at": now,
                "updated_at": now,
            },
        )
        return event_id

    async def list_events_by_influencer(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List pitch events for an influencer."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, campaign_id,
                       event_type, channel, occurred_at, metadata,
                       created_at, updated_at
                FROM pitch_events
                WHERE tenant_id = :tenant_id AND influencer_id = :influencer_id
                ORDER BY occurred_at DESC
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "influencer_id": influencer_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def list_events_by_campaign(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List pitch events for a campaign."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, campaign_id,
                       event_type, channel, occurred_at, metadata,
                       created_at, updated_at
                FROM pitch_events
                WHERE tenant_id = :tenant_id AND campaign_id = :campaign_id
                ORDER BY occurred_at DESC
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "campaign_id": campaign_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get pitch event by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, campaign_id,
                       event_type, channel, occurred_at, metadata,
                       created_at, updated_at
                FROM pitch_events
                WHERE tenant_id = :tenant_id AND id = :event_id
            """),
            {"tenant_id": tenant_id, "event_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create pitch event (BaseRepo interface)."""
        return await self.insert_event(
            tenant_id=tenant_id,
            influencer_id=data["influencer_id"],
            campaign_id=data["campaign_id"],
            event_type=data.get("event_type"),
            channel=data.get("channel"),
            occurred_at=data.get("occurred_at"),
            metadata=data.get("metadata"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update pitch event (not typically used)."""
        return False

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete pitch event."""
        result = await self.session.execute(
            text("""
                DELETE FROM pitch_events
                WHERE tenant_id = :tenant_id AND id = :event_id
            """),
            {"tenant_id": tenant_id, "event_id": entity_id},
        )
        return result.rowcount > 0
