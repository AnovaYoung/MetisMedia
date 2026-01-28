"""Draft repository."""

from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class DraftRepo(BaseRepo):
    """Repository for drafts table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_draft(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        influencer_id: UUID,
        channel: str | None,
        subject: str | None,
        body: str | None,
        status: str | None,
    ) -> UUID:
        """Insert a new draft."""
        draft_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO drafts (
                    id, tenant_id, campaign_id, influencer_id,
                    channel, subject, body, status, created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, :campaign_id, :influencer_id,
                    :channel, :subject, :body, :status, :created_at, :updated_at
                )
            """),
            {
                "id": draft_id,
                "tenant_id": tenant_id,
                "campaign_id": campaign_id,
                "influencer_id": influencer_id,
                "channel": channel,
                "subject": subject,
                "body": body,
                "status": status,
                "created_at": now,
                "updated_at": now,
            },
        )
        return draft_id

    async def list_drafts(
        self,
        tenant_id: UUID,
        campaign_id: UUID,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List drafts for a campaign."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, campaign_id, influencer_id,
                       channel, subject, body, status, created_at, updated_at
                FROM drafts
                WHERE tenant_id = :tenant_id AND campaign_id = :campaign_id
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "campaign_id": campaign_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get draft by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, campaign_id, influencer_id,
                       channel, subject, body, status, created_at, updated_at
                FROM drafts
                WHERE tenant_id = :tenant_id AND id = :draft_id
            """),
            {"tenant_id": tenant_id, "draft_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create draft (BaseRepo interface)."""
        return await self.insert_draft(
            tenant_id=tenant_id,
            campaign_id=data["campaign_id"],
            influencer_id=data["influencer_id"],
            channel=data.get("channel"),
            subject=data.get("subject"),
            body=data.get("body"),
            status=data.get("status"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update draft."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE drafts
                SET channel = COALESCE(:channel, channel),
                    subject = COALESCE(:subject, subject),
                    body = COALESCE(:body, body),
                    status = COALESCE(:status, status),
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :draft_id
            """),
            {
                "tenant_id": tenant_id,
                "draft_id": entity_id,
                "channel": data.get("channel"),
                "subject": data.get("subject"),
                "body": data.get("body"),
                "status": data.get("status"),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete draft."""
        result = await self.session.execute(
            text("""
                DELETE FROM drafts
                WHERE tenant_id = :tenant_id AND id = :draft_id
            """),
            {"tenant_id": tenant_id, "draft_id": entity_id},
        )
        return result.rowcount > 0
