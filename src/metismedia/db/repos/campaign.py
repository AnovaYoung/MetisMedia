"""Campaign repository."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class CampaignRepo(BaseRepo):
    """Repository for campaigns table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_campaign(
        self,
        tenant_id: UUID,
        trace_id: str | None,
        run_id: str | None,
        brief_json: dict[str, Any] | None,
    ) -> UUID:
        """Create a new campaign."""
        campaign_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO campaigns (id, tenant_id, trace_id, run_id, brief, created_at, updated_at)
                VALUES (:id, :tenant_id, :trace_id, :run_id, :brief, :created_at, :updated_at)
            """),
            {
                "id": campaign_id,
                "tenant_id": tenant_id,
                "trace_id": trace_id,
                "run_id": run_id,
                "brief": json.dumps(brief_json) if brief_json else None,
                "created_at": now,
                "updated_at": now,
            },
        )
        return campaign_id

    async def get_campaign(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
    ) -> dict[str, Any] | None:
        """Get a campaign by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, trace_id, run_id, brief, created_at, updated_at
                FROM campaigns
                WHERE tenant_id = :tenant_id AND id = :campaign_id
            """),
            {"tenant_id": tenant_id, "campaign_id": campaign_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get campaign by ID (BaseRepo interface)."""
        return await self.get_campaign(tenant_id, entity_id)

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create campaign (BaseRepo interface)."""
        return await self.create_campaign(
            tenant_id=tenant_id,
            trace_id=data.get("trace_id"),
            run_id=data.get("run_id"),
            brief_json=data.get("brief"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update campaign."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE campaigns
                SET brief = :brief, updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :campaign_id
            """),
            {
                "tenant_id": tenant_id,
                "campaign_id": entity_id,
                "brief": json.dumps(data.get("brief")) if data.get("brief") else None,
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete campaign."""
        result = await self.session.execute(
            text("""
                DELETE FROM campaigns
                WHERE tenant_id = :tenant_id AND id = :campaign_id
            """),
            {"tenant_id": tenant_id, "campaign_id": entity_id},
        )
        return result.rowcount > 0
