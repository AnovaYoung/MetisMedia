"""Budget models and guard for cost and provider caps."""

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class BudgetExceeded(ValueError):
    """Raised when a budget limit would be exceeded."""

    def __init__(self, message: str, limit_type: str) -> None:
        super().__init__(message)
        self.limit_type = limit_type


class Budget(BaseModel):
    """Budget limits for a run or campaign."""

    max_dollars: float = Field(ge=0)
    max_provider_calls: dict[str, int] = Field(default_factory=dict)
    max_node_seconds: dict[str, float] = Field(default_factory=dict)

    model_config = {"extra": "forbid"}


class BudgetState(BaseModel):
    """Current spend state against a budget."""

    dollars_spent: float = Field(default=0, ge=0)
    provider_calls: dict[str, int] = Field(default_factory=dict)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "forbid"}


def budget_guard(
    budget: Budget,
    state: BudgetState,
    cost_delta: float = 0,
    provider: str | None = None,
    calls_delta: int = 0,
    node: str | None = None,
) -> None:
    """Check that applying the deltas would not exceed budget limits.

    Raises BudgetExceeded if any limit would be exceeded.
    Node time caps are supported by the model but not enforced here
    (deferred to Module 6 runtime).

    Args:
        budget: Budget limits
        state: Current state
        cost_delta: Additional dollars to spend
        provider: Provider name for call cap check
        calls_delta: Additional calls for that provider
        node: Node name (for future node_seconds enforcement)
    """
    _ = node
    if cost_delta < 0:
        raise ValueError("cost_delta must be >= 0")
    if calls_delta < 0:
        raise ValueError("calls_delta must be >= 0")

    new_dollars = state.dollars_spent + cost_delta
    if new_dollars > budget.max_dollars:
        raise BudgetExceeded(
            f"Budget exceeded: {new_dollars:.4f} > {budget.max_dollars} max_dollars",
            limit_type="max_dollars",
        )

    if provider is not None and calls_delta > 0:
        cap = budget.max_provider_calls.get(provider)
        if cap is not None:
            current = state.provider_calls.get(provider, 0)
            new_calls = current + calls_delta
            if new_calls > cap:
                raise BudgetExceeded(
                    f"Provider call cap exceeded: {provider} would be {new_calls} > {cap}",
                    limit_type="max_provider_calls",
                )
