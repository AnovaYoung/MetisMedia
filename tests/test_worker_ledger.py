"""Tests for Worker with budget/ledger integration."""

from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.core import Budget, CostEntry, CostLedger, compute_cost
from metismedia.events import EventBus, EventEnvelope, Worker


class InMemoryLedger:
    """Simple in-memory ledger for testing."""

    def __init__(self) -> None:
        self.entries: list[CostEntry] = []

    def record(self, entry: CostEntry) -> None:
        self.entries.append(entry)


@pytest.fixture
def tenant_id():
    return uuid4()


@pytest.fixture
def budget():
    return Budget(max_dollars=10.0, max_provider_calls={"test": 100})


@pytest.fixture
def ledger():
    return InMemoryLedger()


class LedgerAwareHandler:
    """Handler that accepts and uses a ledger."""

    def __init__(self) -> None:
        self.call_count = 0
        self.envelopes: list[EventEnvelope] = []
        self.recorded_entries: list[CostEntry] = []

    async def __call__(
        self, envelope: EventEnvelope, ledger: CostLedger | None = None
    ) -> None:
        self.call_count += 1
        self.envelopes.append(envelope)

        if ledger is not None:
            entry = CostEntry(
                tenant_id=envelope.tenant_id,
                trace_id=envelope.trace_id,
                run_id=envelope.run_id,
                node=envelope.node,
                provider="test_provider",
                operation="test_op",
                unit_cost=0.001,
                quantity=1.0,
                dollars=compute_cost(0.001, 1.0),
            )
            ledger.record(entry)
            self.recorded_entries.append(entry)


class SimpleHandler:
    """Handler with only envelope param (backward compatibility)."""

    def __init__(self) -> None:
        self.call_count = 0
        self.envelopes: list[EventEnvelope] = []

    async def __call__(self, envelope: EventEnvelope) -> None:
        self.call_count += 1
        self.envelopes.append(envelope)


@pytest.mark.asyncio
async def test_worker_with_ledger_records_cost(clean_redis, tenant_id, budget, ledger):
    """Worker passes ledger to handler that can record CostEntry."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="ledger-test-1")

    envelope = EventEnvelope(
        event_name="test.ledger",
        trace_id="trace-ledger-1",
        run_id="run-ledger-1",
        idempotency_key="ledger-test-1",
        tenant_id=tenant_id,
        node=NodeName.A,
        payload={"data": "test"},
    )

    await bus.publish(envelope)

    handler = LedgerAwareHandler()
    registry = {"test.ledger": handler}

    processed = await worker.run(
        registry, stop_after=1, budget=budget, ledger=ledger
    )

    assert processed == 1
    assert handler.call_count == 1
    assert len(ledger.entries) == 1

    entry = ledger.entries[0]
    assert entry.tenant_id == tenant_id
    assert entry.trace_id == "trace-ledger-1"
    assert entry.run_id == "run-ledger-1"
    assert entry.provider == "test_provider"
    assert entry.dollars == compute_cost(0.001, 1.0)


@pytest.mark.asyncio
async def test_worker_backward_compatibility_simple_handler(
    clean_redis, tenant_id, budget, ledger
):
    """Worker works with handlers that only accept envelope (no ledger)."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="compat-test-1")

    envelope = EventEnvelope(
        event_name="test.simple",
        trace_id="trace-simple-1",
        run_id="run-simple-1",
        idempotency_key="compat-test-1",
        tenant_id=tenant_id,
        node=NodeName.B,
        payload={"compat": True},
    )

    await bus.publish(envelope)

    handler = SimpleHandler()
    registry = {"test.simple": handler}

    processed = await worker.run(
        registry, stop_after=1, budget=budget, ledger=ledger
    )

    assert processed == 1
    assert handler.call_count == 1
    assert handler.envelopes[0].payload == {"compat": True}
    assert len(ledger.entries) == 0  # Simple handler doesn't record


@pytest.mark.asyncio
async def test_worker_without_ledger_still_works(clean_redis, tenant_id):
    """Worker works without budget/ledger (both None)."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="no-ledger-test-1")

    envelope = EventEnvelope(
        event_name="test.noop",
        trace_id="trace-noop-1",
        run_id="run-noop-1",
        idempotency_key="noop-test-1",
        tenant_id=tenant_id,
        node=NodeName.C,
        payload={},
    )

    await bus.publish(envelope)

    handler = SimpleHandler()
    registry = {"test.noop": handler}

    processed = await worker.run(registry, stop_after=1)

    assert processed == 1
    assert handler.call_count == 1


@pytest.mark.asyncio
async def test_ledger_aware_handler_without_ledger(clean_redis, tenant_id):
    """Ledger-aware handler works when no ledger is provided."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="no-ledger-aware-1")

    envelope = EventEnvelope(
        event_name="test.aware",
        trace_id="trace-aware-1",
        run_id="run-aware-1",
        idempotency_key="aware-test-1",
        tenant_id=tenant_id,
        node=NodeName.D,
        payload={},
    )

    await bus.publish(envelope)

    handler = LedgerAwareHandler()
    registry = {"test.aware": handler}

    processed = await worker.run(registry, stop_after=1)

    assert processed == 1
    assert handler.call_count == 1
    assert len(handler.recorded_entries) == 0  # No ledger, nothing recorded
