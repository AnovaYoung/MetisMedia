"""Build Worker handler registry from orchestration handlers."""

from collections.abc import Awaitable, Callable
from typing import Any

from metismedia.core.budget import Budget
from metismedia.core.ledger import CostLedger
from metismedia.db.session import db_session
from metismedia.events.bus import EventBus
from metismedia.events.envelope import EventEnvelope

from metismedia.orchestration.handlers import HANDLER_MAP


def build_handler_registry(
    budget: Budget,
    ledger: CostLedger | None,
    bus: EventBus,
) -> dict[str, Callable[..., Awaitable[None]]]:
    """Build handler_registry for Worker: event_name -> async handler(envelope).

    Each handler opens a db_session, calls the orchestration handler with
    (envelope, session, budget, ledger, bus), and commits.
    """
    registry: dict[str, Callable[..., Awaitable[None]]] = {}

    for event_name, real_handler in HANDLER_MAP.items():
        def make_wrapper(
            _handler: Any,
            _budget: Budget,
            _ledger: CostLedger | None,
            _bus: EventBus,
        ) -> Callable[..., Awaitable[None]]:
            async def wrapper(envelope: EventEnvelope, **kwargs: Any) -> None:
                # Worker may pass budget_state (and legacy ledger); use registry's
                # budget/ledger/bus to avoid duplicate keyword arguments.
                extra = {k: v for k, v in kwargs.items() if k not in ("ledger", "budget", "bus")}
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

        registry[event_name] = make_wrapper(real_handler, budget, ledger, bus)

    return registry
