"""Embedding repository."""

from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.db.repos.base import BaseRepo


class EmbeddingRepo(BaseRepo):
    """Repository for embeddings table."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_embedding(
        self,
        tenant_id: UUID,
        kind: str,
        model: str | None,
        dims: int | None,
        norm: str | None,
        vector: list[float] | None,
    ) -> UUID:
        """Create a new embedding."""
        embedding_id = self.generate_uuid()
        now = self.now()

        await self.session.execute(
            text("""
                INSERT INTO embeddings (id, tenant_id, kind, embedding_model, embedding_dims, embedding_norm, vector, created_at, updated_at)
                VALUES (:id, :tenant_id, :kind, :model, :dims, :norm, :vector, :created_at, :updated_at)
            """),
            {
                "id": embedding_id,
                "tenant_id": tenant_id,
                "kind": kind,
                "model": model,
                "dims": dims,
                "norm": norm,
                "vector": str(vector) if vector else None,
                "created_at": now,
                "updated_at": now,
            },
        )
        return embedding_id

    async def get_embedding_meta(
        self,
        tenant_id: UUID,
        embedding_id: UUID,
    ) -> dict[str, Any] | None:
        """Get embedding metadata (without vector)."""
        result = await self.session.execute(
            text("""
                SELECT id, tenant_id, kind, embedding_model, embedding_dims, embedding_norm, created_at, updated_at
                FROM embeddings
                WHERE tenant_id = :tenant_id AND id = :embedding_id
            """),
            {"tenant_id": tenant_id, "embedding_id": embedding_id},
        )
        row = result.mappings().fetchone()
        return dict(row) if row else None

    async def get_vector(
        self,
        tenant_id: UUID,
        embedding_id: UUID,
    ) -> list[float] | None:
        """Get embedding vector."""
        result = await self.session.execute(
            text("""
                SELECT vector::text
                FROM embeddings
                WHERE tenant_id = :tenant_id AND id = :embedding_id
            """),
            {"tenant_id": tenant_id, "embedding_id": embedding_id},
        )
        row = result.fetchone()
        if row and row[0]:
            vec_str = row[0].strip("[]")
            return [float(x) for x in vec_str.split(",")] if vec_str else None
        return None

    async def get_by_id(self, tenant_id: UUID, entity_id: UUID) -> dict[str, Any] | None:
        """Get embedding by ID (BaseRepo interface)."""
        return await self.get_embedding_meta(tenant_id, entity_id)

    async def create(self, tenant_id: UUID, data: dict[str, Any]) -> UUID:
        """Create embedding (BaseRepo interface)."""
        return await self.create_embedding(
            tenant_id=tenant_id,
            kind=data["kind"],
            model=data.get("model"),
            dims=data.get("dims"),
            norm=data.get("norm"),
            vector=data.get("vector"),
        )

    async def update(self, tenant_id: UUID, entity_id: UUID, data: dict[str, Any]) -> bool:
        """Update embedding (not typically used)."""
        return False

    async def delete(self, tenant_id: UUID, entity_id: UUID) -> bool:
        """Delete embedding."""
        result = await self.session.execute(
            text("""
                DELETE FROM embeddings
                WHERE tenant_id = :tenant_id AND id = :embedding_id
            """),
            {"tenant_id": tenant_id, "embedding_id": entity_id},
        )
        return result.rowcount > 0
