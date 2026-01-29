"""Tests for budget guard and budget models."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from metismedia.core.budget import Budget, BudgetExceeded, BudgetState, budget_guard


class TestBudgetModel:
    """Test Budget and BudgetState models."""

    def test_budget_creation(self) -> None:
        """Test creating a Budget with caps."""
        budget = Budget(
            max_dollars=50.0,
            max_provider_calls={"firecrawl": 50, "exa": 25},
            max_node_seconds={"B": 2.0, "C": 30.0},
        )
        assert budget.max_dollars == 50.0
        assert budget.max_provider_calls["firecrawl"] == 50
        assert budget.max_node_seconds["B"] == 2.0

    def test_budget_state_creation(self) -> None:
        """Test creating a BudgetState."""
        started = datetime.now(timezone.utc)
        state = BudgetState(dollars_spent=0, provider_calls={}, started_at=started)
        assert state.dollars_spent == 0
        assert state.provider_calls == {}
        assert state.started_at == started

    def test_budget_state_defaults(self) -> None:
        """Test BudgetState default values."""
        state = BudgetState()
        assert state.dollars_spent == 0
        assert state.provider_calls == {}
        assert state.started_at is not None


class TestBudgetGuardDollars:
    """Test budget_guard blocks when cost exceeds max_dollars."""

    def test_allows_within_limit(self) -> None:
        """Test that cost within limit does not raise."""
        budget = Budget(max_dollars=10.0)
        state = BudgetState(dollars_spent=5.0)
        budget_guard(budget, state, cost_delta=3.0)
        budget_guard(budget, state, cost_delta=5.0)

    def test_blocks_when_exceeds_max_dollars(self) -> None:
        """Test that cost exceeding max_dollars raises BudgetExceeded."""
        budget = Budget(max_dollars=10.0)
        state = BudgetState(dollars_spent=5.0)
        with pytest.raises(BudgetExceeded) as exc_info:
            budget_guard(budget, state, cost_delta=6.0)
        assert exc_info.value.limit_type == "max_dollars"
        assert "max_dollars" in str(exc_info.value)

    def test_blocks_at_exactly_max_dollars_with_positive_delta(self) -> None:
        """Test that delta that would push over limit raises."""
        budget = Budget(max_dollars=10.0)
        state = BudgetState(dollars_spent=10.0)
        with pytest.raises(BudgetExceeded):
            budget_guard(budget, state, cost_delta=0.01)

    def test_allows_exactly_max_dollars(self) -> None:
        """Test that spending exactly max_dollars is allowed."""
        budget = Budget(max_dollars=10.0)
        state = BudgetState(dollars_spent=0.0)
        budget_guard(budget, state, cost_delta=10.0)


class TestBudgetGuardProviderCalls:
    """Test provider call caps are enforced."""

    def test_allows_within_provider_cap(self) -> None:
        """Test that calls within cap do not raise."""
        budget = Budget(max_dollars=100.0, max_provider_calls={"firecrawl": 50})
        state = BudgetState(provider_calls={"firecrawl": 10})
        budget_guard(budget, state, provider="firecrawl", calls_delta=20)

    def test_blocks_when_provider_cap_exceeded(self) -> None:
        """Test that exceeding provider call cap raises BudgetExceeded."""
        budget = Budget(max_dollars=100.0, max_provider_calls={"firecrawl": 50})
        state = BudgetState(provider_calls={"firecrawl": 45})
        with pytest.raises(BudgetExceeded) as exc_info:
            budget_guard(budget, state, provider="firecrawl", calls_delta=10)
        assert exc_info.value.limit_type == "max_provider_calls"
        assert "firecrawl" in str(exc_info.value)

    def test_unknown_provider_no_cap(self) -> None:
        """Test that provider not in budget has no cap."""
        budget = Budget(max_dollars=100.0, max_provider_calls={"firecrawl": 50})
        state = BudgetState()
        budget_guard(budget, state, provider="exa", calls_delta=1000)

    def test_allows_exactly_at_cap(self) -> None:
        """Test that exactly at cap is allowed."""
        budget = Budget(max_dollars=100.0, max_provider_calls={"firecrawl": 50})
        state = BudgetState(provider_calls={"firecrawl": 50})
        budget_guard(budget, state, provider="firecrawl", calls_delta=0)


class TestBudgetGuardValidation:
    """Test budget_guard input validation."""

    def test_negative_cost_delta_raises(self) -> None:
        """Test that negative cost_delta raises ValueError."""
        budget = Budget(max_dollars=10.0)
        state = BudgetState()
        with pytest.raises(ValueError, match="cost_delta"):
            budget_guard(budget, state, cost_delta=-1.0)

    def test_negative_calls_delta_raises(self) -> None:
        """Test that negative calls_delta raises ValueError."""
        budget = Budget(max_dollars=100.0, max_provider_calls={"firecrawl": 50})
        state = BudgetState()
        with pytest.raises(ValueError, match="calls_delta"):
            budget_guard(budget, state, provider="firecrawl", calls_delta=-1)
