"""Target card repository."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class TargetCardRepo(BaseRepo):
    """Repository for target_cards table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_target_card(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        influencer_id: UUID,
        payload_json: dict[str, Any],
    ) -> UUID:
        """Insert a new target card."""
        card_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO target_cards (id, tenant_id, campaign_id, influencer_id, payload, created_at, updated_at)
                VALUES (:id, :tenant_id, :campaign_id, :influencer_id, :payload, :created_at, :updated_at)
                ON CONFLICT (tenant_id, campaign_id, influencer_id)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at
            """),
            {
                "id": card_id,
                "tenant_id": tenant_id,
                "campaign_id": campaign_id,
                "influencer_id": influencer_id,
                "payload": json.dumps(payload_json),
                "created_at": now,
                "updated_at": now,
            },
        )
        return card_id

    async def list_target_cards(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List target cards for a campaign."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, campaign_id, influencer_id, payload, created_at, updated_at
                FROM target_cards
                WHERE tenant_id = :tenant_id AND campaign_id = :campaign_id
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "campaign_id": campaign_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get target card by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, campaign_id, influencer_id, payload, created_at, updated_at
                FROM target_cards
                WHERE tenant_id = :tenant_id AND id = :card_id
            """),
            {"tenant_id": tenant_id, "card_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create target card (BaseRepo interface)."""
        return await self.insert_target_card(
            tenant_id=tenant_id,
            campaign_id=data["campaign_id"],
            influencer_id=data["influencer_id"],
            payload_json=data.get("payload", {}),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update target card payload."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE target_cards
                SET payload = :payload, updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :card_id
            """),
            {
                "tenant_id": tenant_id,
                "card_id": entity_id,
                "payload": json.dumps(data.get("payload", {})),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete target card."""
        result = await self.session.execute(
            text("""
                DELETE FROM target_cards
                WHERE tenant_id = :tenant_id AND id = :card_id
            """),
            {"tenant_id": tenant_id, "card_id": entity_id},
        )
        return result.rowcount > 0
