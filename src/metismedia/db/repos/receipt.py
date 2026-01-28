"""Receipt repository."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class ReceiptRepo(BaseRepo):
    """Repository for receipts table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_receipt(
        self,
        tenant_id: UUID,
        influencer_id: UUID | None,
        type_: str | None,
        url: str | None,
        excerpt: str | None,
        occurred_at: datetime | None,
        source_platform: str | None,
        confidence: float | None,
        provenance_json: dict[str, Any],
    ) -> UUID:
        """Insert a new receipt."""
        receipt_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO receipts (
                    id, tenant_id, influencer_id, type, url, excerpt,
                    occurred_at, source_platform, confidence, provenance,
                    created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, :influencer_id, :type, :url, :excerpt,
                    :occurred_at, :source_platform, :confidence, :provenance,
                    :created_at, :updated_at
                )
            """),
            {
                "id": receipt_id,
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "type": type_,
                "url": url,
                "excerpt": excerpt,
                "occurred_at": occurred_at,
                "source_platform": source_platform,
                "confidence": confidence,
                "provenance": json.dumps(provenance_json),
                "created_at": now,
                "updated_at": now,
            },
        )
        return receipt_id

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get receipt by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, type, url, excerpt,
                       occurred_at, source_platform, confidence, provenance,
                       created_at, updated_at
                FROM receipts
                WHERE tenant_id = :tenant_id AND id = :receipt_id
            """),
            {"tenant_id": tenant_id, "receipt_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def list_by_influencer(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List receipts for an influencer."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, type, url, excerpt,
                       occurred_at, source_platform, confidence, provenance,
                       created_at, updated_at
                FROM receipts
                WHERE tenant_id = :tenant_id AND influencer_id = :influencer_id
                ORDER BY occurred_at DESC NULLS LAST
                LIMIT :limit
            """),
            {"tenant_id": tenant_id, "influencer_id": influencer_id, "limit": limit},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create receipt (BaseRepo interface)."""
        return await self.insert_receipt(
            tenant_id=tenant_id,
            influencer_id=data.get("influencer_id"),
            type_=data.get("type"),
            url=data.get("url"),
            excerpt=data.get("excerpt"),
            occurred_at=data.get("occurred_at"),
            source_platform=data.get("source_platform"),
            confidence=data.get("confidence"),
            provenance_json=data.get("provenance", {}),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update receipt (not typically used)."""
        return False

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete receipt."""
        result = await self.session.execute(
            text("""
                DELETE FROM receipts
                WHERE tenant_id = :tenant_id AND id = :receipt_id
            """),
            {"tenant_id": tenant_id, "receipt_id": entity_id},
        )
        return result.rowcount > 0
