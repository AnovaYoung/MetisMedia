"""Deterministic idempotency keys for orchestrator/handler-published events."""

from uuid import UUID

from metismedia.contracts.enums import NodeName


def make_idempotency_key(
    *,
    tenant_id: UUID,
    run_id: UUID | str,
    node: NodeName,
    event_name: str,
    step: str,
) -> str:
    """Build deterministic idempotency key so duplicate publishes do not re-execute nodes.

    Format: "{tenant_id}:{run_id}:{node.value}:{event_name}:{step}"
    """
    run = str(run_id)
    return f"{tenant_id}:{run}:{node.value}:{event_name}:{step}"
