"""Contact method repository."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class ContactRepo(BaseRepo):
    """Repository for contact_methods table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_contact_method(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
        method: str,
        value: str,
        confidence: float | None,
        verified: bool | None,
        provenance_json: dict[str, Any] | None,
    ) -> UUID:
        """Insert a new contact method."""
        contact_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO contact_methods (
                    id, tenant_id, influencer_id, method, value,
                    confidence, verified, provenance, created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, :influencer_id, :method, :value,
                    :confidence, :verified, :provenance, :created_at, :updated_at
                )
                ON CONFLICT (tenant_id, influencer_id, method, value)
                DO UPDATE SET
                    confidence = COALESCE(EXCLUDED.confidence, contact_methods.confidence),
                    verified = COALESCE(EXCLUDED.verified, contact_methods.verified),
                    provenance = COALESCE(EXCLUDED.provenance, contact_methods.provenance),
                    updated_at = EXCLUDED.updated_at
            """),
            {
                "id": contact_id,
                "tenant_id": tenant_id,
                "influencer_id": influencer_id,
                "method": method,
                "value": value,
                "confidence": confidence,
                "verified": verified,
                "provenance": json.dumps(provenance_json) if provenance_json else None,
                "created_at": now,
                "updated_at": now,
            },
        )
        return contact_id

    async def list_contact_methods(
        self,
        tenant_id: UUID,
        influencer_id: UUID,
    ) -> list[dict[str, Any]]:
        """List contact methods for an influencer."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, method, value,
                       confidence, verified, provenance, created_at, updated_at
                FROM contact_methods
                WHERE tenant_id = :tenant_id AND influencer_id = :influencer_id
                ORDER BY confidence DESC NULLS LAST
            """),
            {"tenant_id": tenant_id, "influencer_id": influencer_id},
        )
        return [dict(row) for row in result.mappings().fetchall()]

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get contact method by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, influencer_id, method, value,
                       confidence, verified, provenance, created_at, updated_at
                FROM contact_methods
                WHERE tenant_id = :tenant_id AND id = :contact_id
            """),
            {"tenant_id": tenant_id, "contact_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create contact method (BaseRepo interface)."""
        return await self.insert_contact_method(
            tenant_id=tenant_id,
            influencer_id=data["influencer_id"],
            method=data["method"],
            value=data["value"],
            confidence=data.get("confidence"),
            verified=data.get("verified"),
            provenance_json=data.get("provenance"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update contact method."""
        now = self.now()
        result = await self.session.execute(
            text("""
                UPDATE contact_methods
                SET confidence = COALESCE(:confidence, confidence),
                    verified = COALESCE(:verified, verified),
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :contact_id
            """),
            {
                "tenant_id": tenant_id,
                "contact_id": entity_id,
                "confidence": data.get("confidence"),
                "verified": data.get("verified"),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete contact method."""
        result = await self.session.execute(
            text("""
                DELETE FROM contact_methods
                WHERE tenant_id = :tenant_id AND id = :contact_id
            """),
            {"tenant_id": tenant_id, "contact_id": entity_id},
        )
        return result.rowcount > 0
