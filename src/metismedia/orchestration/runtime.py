"""Node runtime protocol and budget/time enforcement helper."""

import asyncio
from collections.abc import Awaitable
from typing import Protocol, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.core.budget import Budget, BudgetState, budget_guard
from metismedia.core.ledger import CostLedger
from metismedia.events.envelope import EventEnvelope

T = TypeVar("T")


class NodeRuntime(Protocol):
    """Protocol for node runtime: run with envelope, session, budget, ledger."""

    async def run(
        self,
        envelope: EventEnvelope,
        *,
        session: AsyncSession,
        budget: Budget,
        ledger: CostLedger,
    ) -> None:
        """Run node logic for the given envelope.

        Args:
            envelope: Event envelope for this run
            session: DB session
            budget: Budget limits
            ledger: Cost ledger for recording
        """
        ...


async def enforce_budget_and_time(
    budget: Budget,
    state: BudgetState,
    node: str | None,
    coro: Awaitable[T],
    cost_delta: float = 0,
    provider: str | None = None,
    calls_delta: int = 0,
) -> T:
    """Run coroutine under budget check and optional node time limit.

    Uses budget_guard for dollars/providers; enforces max_node_seconds
    via asyncio timeout when node has a limit.

    Args:
        budget: Budget limits
        state: Current budget state
        node: Node name (for max_node_seconds lookup)
        coro: Coroutine to run
        cost_delta: Cost delta for budget_guard
        provider: Provider name for call cap check
        calls_delta: Calls delta for budget_guard

    Returns:
        Result of coro
    """
    budget_guard(
        budget,
        state,
        cost_delta=cost_delta,
        provider=provider,
        calls_delta=calls_delta,
        node=node,
    )
    timeout_s = budget.max_node_seconds.get(node) if node else None
    if timeout_s is not None and timeout_s > 0:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    return await coro
