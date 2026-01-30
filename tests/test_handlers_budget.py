"""Tests for handler _record_cost budget enforcement."""

from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.core.budget import Budget, BudgetExceeded, BudgetState
from metismedia.events.envelope import EventEnvelope
from metismedia.orchestration.handlers import _record_cost


def test_record_cost_raises_budget_exceeded_when_over_limit() -> None:
    """_record_cost with budget/budget_state raises BudgetExceeded when cost would exceed."""
    tenant_id = uuid4()
    envelope = EventEnvelope(
        tenant_id=tenant_id,
        node=NodeName.C,
        event_name="node_c.input",
        trace_id="trace-1",
        run_id="run-1",
        idempotency_key="key-1",
        payload={},
    )
    budget = Budget(max_dollars=0.01)
    state = BudgetState()

    _record_cost(
        envelope,
        ledger=None,
        node=NodeName.C,
        provider="mock_discovery",
        operation="scrape",
        unit_cost=0.005,
        quantity=1.0,
        budget=budget,
        budget_state=state,
    )
    assert state.dollars_spent == 0.005

    with pytest.raises(BudgetExceeded, match="Budget exceeded"):
        _record_cost(
            envelope,
            ledger=None,
            node=NodeName.C,
            provider="mock_discovery",
            operation="scrape",
            unit_cost=0.02,
            quantity=1.0,
            budget=budget,
            budget_state=state,
        )
    assert state.dollars_spent == 0.005
