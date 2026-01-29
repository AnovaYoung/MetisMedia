"""Cost ledger for recording provider and node costs."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, Field

from metismedia.contracts.enums import NodeName

COST_LOGGER_NAME = "metismedia.cost"


def compute_cost(unit_cost: float, quantity: float) -> float:
    """Compute total cost from unit cost and quantity."""
    return round(unit_cost * quantity, 6)


class CostLedger(Protocol):
    """Protocol for recording cost entries."""

    def record(self, entry: "CostEntry") -> None:
        """Record a cost entry."""
        ...


class CostEntry(BaseModel):
    """Single cost entry for the ledger."""

    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tenant_id: UUID
    trace_id: str
    run_id: str
    node: NodeName
    provider: str
    operation: str
    unit_cost: float = Field(ge=0)
    quantity: float = Field(ge=0)
    dollars: float = Field(ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


class JsonLogLedger:
    """Ledger implementation that writes one JSON line to logger metismedia.cost."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(COST_LOGGER_NAME)

    def record(self, entry: CostEntry) -> None:
        """Record entry as a single JSON line to the cost logger."""
        payload = {
            "occurred_at": entry.occurred_at.isoformat(),
            "tenant_id": str(entry.tenant_id),
            "trace_id": entry.trace_id,
            "run_id": entry.run_id,
            "node": entry.node.value,
            "provider": entry.provider,
            "operation": entry.operation,
            "unit_cost": entry.unit_cost,
            "quantity": entry.quantity,
            "dollars": entry.dollars,
            "metadata": entry.metadata,
        }
        self._logger.info(json.dumps(payload))
