"""Briefing session repository for Node A."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo
from metismedia.providers.node_a_provider import compute_missing_slots


class BriefingSessionRepo(BaseRepo):
    """Repository for briefing_sessions table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_session(
        self,
        tenant_id: UUID,
        initial_slots: dict[str, Any] | None = None,
        initial_confidences: dict[str, float] | None = None,
    ) -> UUID:
        """Create a new briefing session. missing_slots is derived from slots+confidences."""
        session_id = self.generate_uuid()
        now = self.now()
        slots = initial_slots or {}
        confidences = initial_confidences or {}
        missing_slots = compute_missing_slots(slots, confidences)

        await self.session.execute(
            text("""
                INSERT INTO briefing_sessions (
                    id, tenant_id, status, slots_json, confidences_json,
                    messages_json, missing_slots, created_at, updated_at
                )
                VALUES (
                    :id, :tenant_id, 'active', :slots_json, :confidences_json,
                    :messages_json, :missing_slots, :created_at, :updated_at
                )
            """),
            {
                "id": session_id,
                "tenant_id": tenant_id,
                "slots_json": json.dumps(slots),
                "confidences_json": json.dumps(confidences),
                "messages_json": json.dumps([]),
                "missing_slots": json.dumps(missing_slots),
                "created_at": now,
                "updated_at": now,
            },
        )
        return session_id

    async def get_session(
        self,
        tenant_id: UUID,
        session_id: UUID,
    ) -> dict[str, Any] | None:
        """Get a briefing session by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, status, slots_json, confidences_json,
                       messages_json, missing_slots, run_id, campaign_id,
                       created_at, updated_at
                FROM briefing_sessions
                WHERE tenant_id = :tenant_id AND id = :session_id
            """),
            {"tenant_id": tenant_id, "session_id": session_id},
        )
        row = result.mappings().fetchone()
        if not row:
            return None

        data = dict(row)
        if isinstance(data.get("slots_json"), str):
            data["slots_json"] = json.loads(data["slots_json"])
        if isinstance(data.get("confidences_json"), str):
            data["confidences_json"] = json.loads(data["confidences_json"])
        if isinstance(data.get("messages_json"), str):
            data["messages_json"] = json.loads(data["messages_json"])
        if isinstance(data.get("missing_slots"), str):
            data["missing_slots"] = json.loads(data["missing_slots"])
        return data

    async def update_slots(
        self,
        tenant_id: UUID,
        session_id: UUID,
        slots: dict[str, Any],
        confidences: dict[str, float],
    ) -> bool:
        """Update slots and confidences for a session. missing_slots is derived from slots+confidences."""
        now = self.now()
        missing_slots = compute_missing_slots(slots, confidences)
        result = await self.session.execute(
            text("""
                UPDATE briefing_sessions
                SET slots_json = :slots_json,
                    confidences_json = :confidences_json,
                    missing_slots = :missing_slots,
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :session_id AND status = 'active'
            """),
            {
                "tenant_id": tenant_id,
                "session_id": session_id,
                "slots_json": json.dumps(slots),
                "confidences_json": json.dumps(confidences),
                "missing_slots": json.dumps(missing_slots),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def add_message(
        self,
        tenant_id: UUID,
        session_id: UUID,
        role: str,
        content: str,
    ) -> bool:
        """Add a message to the session history."""
        now = self.now()

        session_data = await self.get_session(tenant_id, session_id)
        if not session_data:
            return False

        messages = session_data.get("messages_json", [])
        messages.append({
            "role": role,
            "content": content,
            "timestamp": now.isoformat(),
        })

        result = await self.session.execute(
            text("""
                UPDATE briefing_sessions
                SET messages_json = :messages_json, updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :session_id
            """),
            {
                "tenant_id": tenant_id,
                "session_id": session_id,
                "messages_json": json.dumps(messages),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def finalize_session(
        self,
        tenant_id: UUID,
        session_id: UUID,
        run_id: UUID,
        campaign_id: UUID,
    ) -> bool:
        """Mark session as finalized and link to run/campaign. Defensively recompute missing_slots from slots+confidences."""
        now = self.now()
        row = await self.get_session(tenant_id, session_id)
        if not row:
            return False
        slots = row.get("slots_json") or {}
        confidences = row.get("confidences_json") or {}
        missing_slots = compute_missing_slots(slots, confidences)
        result = await self.session.execute(
            text("""
                UPDATE briefing_sessions
                SET status = 'finalized',
                    run_id = :run_id,
                    campaign_id = :campaign_id,
                    missing_slots = :missing_slots,
                    updated_at = :updated_at
                WHERE tenant_id = :tenant_id AND id = :session_id AND status = 'active'
            """),
            {
                "tenant_id": tenant_id,
                "session_id": session_id,
                "run_id": run_id,
                "campaign_id": campaign_id,
                "missing_slots": json.dumps(missing_slots),
                "updated_at": now,
            },
        )
        return result.rowcount > 0

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get session by ID (BaseRepo interface)."""
        return await self.get_session(tenant_id, entity_id)

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create session (BaseRepo interface). missing_slots is derived from slots+confidences."""
        return await self.create_session(
            tenant_id=tenant_id,
            initial_slots=data.get("slots"),
            initial_confidences=data.get("confidences"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update session (BaseRepo interface). missing_slots is derived from slots+confidences."""
        return await self.update_slots(
            tenant_id=tenant_id,
            session_id=entity_id,
            slots=data.get("slots", {}),
            confidences=data.get("confidences", {}),
        )

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete session."""
        result = await self.session.execute(
            text("""
                DELETE FROM briefing_sessions
                WHERE tenant_id = :tenant_id AND id = :session_id
            """),
            {"tenant_id": tenant_id, "session_id": entity_id},
        )
        return result.rowcount > 0
