"""Node runtime wrapper with timeout and budget enforcement."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from metismedia.contracts.enums import NodeName
from metismedia.core import Budget, BudgetState, CostEntry, CostLedger, compute_cost
from metismedia.events.envelope import EventEnvelope

logger = logging.getLogger(__name__)

T = TypeVar("T")


class NodeTimeoutError(Exception):
    """Raised when a node exceeds its time budget."""

    def __init__(self, node: NodeName, timeout_seconds: float) -> None:
        super().__init__(f"Node {node.value} timed out after {timeout_seconds}s")
        self.node = node
        self.timeout_seconds = timeout_seconds


class NodeRuntime:
    """Runtime wrapper for node handlers with timeout and cost tracking."""

    def __init__(
        self,
        node: NodeName,
        budget: Budget | None = None,
        budget_state: BudgetState | None = None,
        ledger: CostLedger | None = None,
        default_timeout_seconds: float = 60.0,
    ) -> None:
        self.node = node
        self.budget = budget
        self.budget_state = budget_state or BudgetState()
        self.ledger = ledger
        self.default_timeout_seconds = default_timeout_seconds

    def get_timeout_seconds(self) -> float:
        """Get timeout for this node from budget or default."""
        if self.budget and self.node.value in self.budget.max_node_seconds:
            return self.budget.max_node_seconds[self.node.value]
        return self.default_timeout_seconds

    async def run_with_timeout(
        self,
        coro: Awaitable[T],
    ) -> T:
        """Run a coroutine with timeout enforcement."""
        timeout = self.get_timeout_seconds()
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError as e:
            raise NodeTimeoutError(self.node, timeout) from e

    def record_cost(
        self,
        envelope: EventEnvelope,
        provider: str,
        operation: str,
        unit_cost: float,
        quantity: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a cost entry to the ledger."""
        if self.ledger is None:
            return

        dollars = compute_cost(unit_cost, quantity)
        entry = CostEntry(
            tenant_id=envelope.tenant_id,
            trace_id=envelope.trace_id,
            run_id=envelope.run_id,
            node=self.node,
            provider=provider,
            operation=operation,
            unit_cost=unit_cost,
            quantity=quantity,
            dollars=dollars,
            metadata=metadata or {},
        )
        self.ledger.record(entry)

        self.budget_state.dollars_spent += dollars
        if provider:
            self.budget_state.provider_calls[provider] = (
                self.budget_state.provider_calls.get(provider, 0) + 1
            )


NodeHandler = Callable[
    [EventEnvelope, NodeRuntime, Any],
    Awaitable[list[EventEnvelope]],
]
