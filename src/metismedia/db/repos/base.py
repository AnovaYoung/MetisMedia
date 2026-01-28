"""Base repository with tenant isolation invariants."""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from metismedia.db.types import TenantId


class BaseRepo(ABC):
    """Base repository class with tenant isolation guarantees.

    Invariants:
    - Every query MUST include a tenant_id filter to prevent cross-tenant data access
    - No cross-tenant joins are allowed without explicit approval and audit logging
    - All repository methods MUST accept tenant_id as the first parameter after self
    - UUID generation: Use app-side uuid4() for new entities; db-side generation
      (e.g., gen_random_uuid()) may be used for audit fields but must be documented

    Design decisions:
    - tenant_id is required at the repository level, not the connection level
    - This ensures explicit tenant context in every operation
    - Prevents accidental cross-tenant queries from missing tenant_id in WHERE clauses
    """

    @staticmethod
    def now() -> datetime:
        """Get current UTC timestamp.

        Returns:
            Current UTC datetime. Use this instead of datetime.utcnow() for consistency.
        """
        return datetime.now(timezone.utc)

    @staticmethod
    def generate_uuid() -> UUID:
        """Generate a new UUID.

        Returns:
            A new UUID4. Use app-side generation for entity IDs.
            DB-side generation (e.g., gen_random_uuid()) may be used for
            audit fields but must be explicitly documented.
        """
        return uuid4()

    @abstractmethod
    def get_by_id(self, tenant_id: TenantId, entity_id: UUID) -> Any:
        """Get entity by ID (must include tenant_id filter).

        Args:
            tenant_id: Tenant identifier (required for isolation)
            entity_id: Entity identifier

        Returns:
            Entity instance or None if not found

        Invariant:
            Query MUST include: WHERE tenant_id = :tenant_id AND id = :entity_id
        """
        raise NotImplementedError

    @abstractmethod
    def create(self, tenant_id: TenantId, data: dict[str, Any]) -> Any:
        """Create new entity (must include tenant_id).

        Args:
            tenant_id: Tenant identifier (required for isolation)
            data: Entity data dictionary

        Returns:
            Created entity instance

        Invariant:
            INSERT MUST include tenant_id in the data/values
        """
        raise NotImplementedError

    @abstractmethod
    def update(self, tenant_id: TenantId, entity_id: UUID, data: dict[str, Any]) -> Any:
        """Update entity (must include tenant_id filter).

        Args:
            tenant_id: Tenant identifier (required for isolation)
            entity_id: Entity identifier
            data: Update data dictionary

        Returns:
            Updated entity instance or None if not found

        Invariant:
            UPDATE MUST include: WHERE tenant_id = :tenant_id AND id = :entity_id
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, tenant_id: TenantId, entity_id: UUID) -> bool:
        """Delete entity (must include tenant_id filter).

        Args:
            tenant_id: Tenant identifier (required for isolation)
            entity_id: Entity identifier

        Returns:
            True if deleted, False if not found

        Invariant:
            DELETE MUST include: WHERE tenant_id = :tenant_id AND id = :entity_id
        """
        raise NotImplementedError
