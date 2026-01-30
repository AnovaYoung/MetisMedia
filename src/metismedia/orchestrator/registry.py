"""Handler registry for wiring node handlers to the event worker."""

import logging
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from metismedia.contracts.enums import NodeName
from metismedia.core import Budget, BudgetState, CostLedger, JsonLogLedger
from metismedia.db.session import db_session
from metismedia.events.envelope import EventEnvelope
from metismedia.orchestrator.nodes import NODE_HANDLERS
from metismedia.orchestrator.runtime import NodeRuntime

logger = logging.getLogger(__name__)


def build_worker_handler_registry(
    budget: Budget | None = None,
    budget_state: BudgetState | None = None,
    ledger: CostLedger | None = None,
) -> dict[str, Any]:
    """Build a handler registry mapping event names to worker handlers.

    This creates async handlers that can be passed to Worker.run().
    """
    budget = budget or Budget(max_dollars=5.0)
    budget_state = budget_state or BudgetState()
    ledger = ledger or JsonLogLedger()

    async def make_handler(node: NodeName, event_name: str):
        """Create a handler for a specific node/event combination."""
        node_handler = NODE_HANDLERS.get(node)
        if node_handler is None:
            return None

        async def handler(envelope: EventEnvelope) -> None:
            runtime = NodeRuntime(
                node=node,
                budget=budget,
                budget_state=budget_state,
                ledger=ledger,
            )

            async with db_session() as session:
                await runtime.run_with_timeout(
                    node_handler(envelope, runtime, session)
                )
                await session.commit()

        return handler

    registry: dict[str, Any] = {}

    event_mappings = [
        (NodeName.A, "node_a.brief_finalized"),
        (NodeName.A, "node_a.input"),
        (NodeName.B, "node_b.directive_emitted"),
        (NodeName.B, "node_b.input"),
        (NodeName.C, "node_c.batch_complete"),
        (NodeName.C, "node_c.input"),
        (NodeName.D, "node_d.profile_ready"),
        (NodeName.D, "node_d.input"),
        (NodeName.E, "node_e.contact_ready"),
        (NodeName.E, "node_e.input"),
        (NodeName.F, "node_f.draft_ready"),
        (NodeName.F, "node_f.input"),
        (NodeName.G, "node_g.input"),
    ]

    import asyncio

    for node, event_name in event_mappings:
        handler = asyncio.get_event_loop().run_until_complete(
            make_handler(node, event_name)
        )
        if handler:
            registry[event_name] = handler

    return registry


def build_sync_handler_registry(
    budget: Budget | None = None,
    budget_state: BudgetState | None = None,
    ledger: CostLedger | None = None,
) -> dict[str, Any]:
    """Build handler registry without async event loop.

    Returns factory functions that create handlers when called.
    """
    budget = budget or Budget(max_dollars=5.0)
    budget_state = budget_state or BudgetState()
    ledger = ledger or JsonLogLedger()

    def create_handler(node: NodeName):
        """Create a handler for a specific node."""
        node_handler = NODE_HANDLERS.get(node)
        if node_handler is None:
            return None

        async def handler(envelope: EventEnvelope) -> None:
            runtime = NodeRuntime(
                node=node,
                budget=budget,
                budget_state=budget_state,
                ledger=ledger,
            )

            async with db_session() as session:
                await runtime.run_with_timeout(
                    node_handler(envelope, runtime, session)
                )
                await session.commit()

        return handler

    registry: dict[str, Any] = {}

    event_mappings = [
        (NodeName.A, "node_a.brief_finalized"),
        (NodeName.A, "node_a.input"),
        (NodeName.B, "node_b.directive_emitted"),
        (NodeName.B, "node_b.input"),
        (NodeName.C, "node_c.batch_complete"),
        (NodeName.C, "node_c.input"),
        (NodeName.D, "node_d.profile_ready"),
        (NodeName.D, "node_d.input"),
        (NodeName.E, "node_e.contact_ready"),
        (NodeName.E, "node_e.input"),
        (NodeName.F, "node_f.draft_ready"),
        (NodeName.F, "node_f.input"),
        (NodeName.G, "node_g.input"),
    ]

    for node, event_name in event_mappings:
        handler = create_handler(node)
        if handler:
            registry[event_name] = handler

    return registry
