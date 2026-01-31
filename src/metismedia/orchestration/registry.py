"""Build Worker handler registry from orchestration handlers."""

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from metismedia.core.budget import Budget
from metismedia.core.ledger import CostLedger
from metismedia.db.session import db_session
from metismedia.events.bus import EventBus
from metismedia.events.envelope import EventEnvelope
from metismedia.providers import EmbeddingProvider, MockEmbeddingProvider, MockPulseProvider, PulseProvider

from metismedia.nodes.node_b.handler import handle_node_b_input as real_handle_node_b_input
from metismedia.orchestration.handlers import HANDLER_MAP


def _make_wrapper(
    _handler: Any,
    _event_name: str,
    _budget: Budget,
    _ledger: CostLedger | None,
    _bus: EventBus,
    _pulse_provider: PulseProvider,
    _embedding_provider: EmbeddingProvider,
) -> Callable[..., Awaitable[None]]:
    async def wrapper(envelope: EventEnvelope, **kwargs: Any) -> None:
        extra = {k: v for k, v in kwargs.items() if k not in ("ledger", "budget", "bus")}
        if _event_name == "node_b.input":
            if "pulse_provider" not in extra:
                extra["pulse_provider"] = _pulse_provider
            if "embedding_provider" not in extra:
                extra["embedding_provider"] = _embedding_provider
        async with db_session() as session:
            await _handler(
                envelope,
                session=session,
                budget=_budget,
                ledger=_ledger,
                bus=_bus,
                **extra,
            )
            await session.commit()

    return wrapper


def build_handler_registry(
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
    pulse_provider: PulseProvider | None = None,
    embedding_provider: EmbeddingProvider | None = None,
) -> dict[str, Callable[..., Awaitable[None]]]:
    """Build handler_registry for Worker: event_name -> async handler(envelope).

    node_b.input is always routed to metismedia.nodes.node_b.handler.handle_node_b_input.
    Other events use HANDLER_MAP (orchestration handlers). Each handler runs inside
    db_session() and commits.
    """
    if pulse_provider is None:
        pulse_provider = MockPulseProvider(
            default_summaries=[
                {
                    "title": "Recent Post",
                    "summary": "Tech innovation and industry insights",
                    "date": datetime.now(timezone.utc),
                }
            ]
        )
    if embedding_provider is None:
        embedding_provider = MockEmbeddingProvider()

    registry: dict[str, Callable[..., Awaitable[None]]] = {}

    for event_name, real_handler in HANDLER_MAP.items():
        if event_name == "node_b.input":
            continue
        registry[event_name] = _make_wrapper(
            real_handler, event_name, budget, ledger, bus, pulse_provider, embedding_provider
        )

    registry["node_b.input"] = _make_wrapper(
        real_handle_node_b_input,
        "node_b.input",
        budget,
        ledger,
        bus,
        pulse_provider,
        embedding_provider,
    )

    return registry
