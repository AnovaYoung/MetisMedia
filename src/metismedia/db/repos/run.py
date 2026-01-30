"""Run repository for orchestrator tracking."""

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class RunRepo(BaseRepo):
    """Repository for runs table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_run(
        self,
        tenant_id: UUID,
        trace_id: str,
        campaign_id: UUID | None = None,
        status: str = "pending",
    ) -> UUID:
        """Create a new run record."""
        run_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO runs (id, tenant_id, campaign_id, trace_id, status, created_at, updated_at)
                VALUES (:id, :tenant_id, :campaign_id, :trace_id, :status, :created_at, :updated_at)
            """),
            {
                "id": run_id,
                "tenant_id": tenant_id,
                "campaign_id": campaign_id,
                "trace_id": trace_id,
                "status": status,
                "created_at": now,
                "updated_at": now,
            },
        )
        return run_id

    async def update_status(
        self,
        tenant_id: UUID,
        run_id: UUID,
        status: str,
        error_message: str | None = None,
        result_json: dict[str, Any] | None = None,
    ) -> bool:
        """Update run status."""
        now = self.now()
        started_at_clause = ""
        completed_at_clause = ""

        if status == "running":
            started_at_clause = ", started_at = :now"
        elif status in ("completed", "failed"):
            completed_at_clause = ", completed_at = :now"

        result = await self.session.execute(
            text(f"""
                UPDATE runs
                SET status = :status,
                    error_message = :error_message,
                    result_json = :result_json,
                    updated_at = :now
                    {started_at_clause}
                    {completed_at_clause}
                WHERE tenant_id = :tenant_id AND id = :run_id
            """),
            {
                "tenant_id": tenant_id,
                "run_id": run_id,
                "status": status,
                "error_message": error_message,
                "result_json": json.dumps(result_json) if result_json else None,
                "now": now,
            },
        )
        return result.rowcount > 0

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get run by ID."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, campaign_id, trace_id, status,
                       started_at, completed_at, error_message, result_json,
                       created_at, updated_at
                FROM runs
                WHERE tenant_id = :tenant_id AND id = :run_id
            """),
            {"tenant_id": tenant_id, "run_id": entity_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create run (BaseRepo interface)."""
        return await self.create_run(
            tenant_id=tenant_id,
            trace_id=data["trace_id"],
            campaign_id=data.get("campaign_id"),
            status=data.get("status", "pending"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update run (BaseRepo interface)."""
        return await self.update_status(
            tenant_id=tenant_id,
            run_id=entity_id,
            status=data.get("status", "pending"),
            error_message=data.get("error_message"),
            result_json=data.get("result_json"),
        )

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete run."""
        result = await self.session.execute(
            text("""
                DELETE FROM runs
                WHERE tenant_id = :tenant_id AND id = :run_id
            """),
            {"tenant_id": tenant_id, "run_id": entity_id},
        )
        return result.rowcount > 0

    async def link_campaign(
        self,
        tenant_id: UUID,
        run_id: UUID,
        campaign_id: UUID,
    ) -> bool:
        """Link a campaign to a run."""
        result = await self.session.execute(
            text("""
                UPDATE runs
                SET campaign_id = :campaign_id, updated_at = :now
                WHERE tenant_id = :tenant_id AND id = :run_id
            """),
            {
                "tenant_id": tenant_id,
                "run_id": run_id,
                "campaign_id": campaign_id,
                "now": self.now(),
            },
        )
        return result.rowcount > 0
