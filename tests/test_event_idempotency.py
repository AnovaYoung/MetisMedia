"""Integration tests for event idempotency."""

from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.events import EventBus, EventEnvelope, Worker
from metismedia.events.handlers import SpyHandler
from metismedia.events.idempotency import mark_processed


@pytest.fixture
def tenant_id():
    """Generate a tenant ID for tests."""
    return uuid4()


@pytest.mark.asyncio
async def test_idempotent_event_not_reprocessed(clean_redis, tenant_id):
    """Test that event with same idempotency_key is not processed twice."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-idem-1")

    envelope1 = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-idem-1",
        run_id="run-idem-1",
        idempotency_key="idem-test-same-key",
        tenant_id=tenant_id,
        node=NodeName.A,
        payload={"first": True},
    )
    await bus.publish(envelope1)

    spy = SpyHandler()
    handler_registry = {"test.ok": spy}

    await worker.run(handler_registry, stop_after=1)

    assert spy.call_count == 1

    envelope2 = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-idem-2",
        run_id="run-idem-2",
        idempotency_key="idem-test-same-key",
        tenant_id=tenant_id,
        node=NodeName.A,
        payload={"second": True},
    )
    await bus.publish(envelope2)

    await worker.run(handler_registry, stop_after=1)

    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_different_idempotency_keys_both_processed(clean_redis, tenant_id):
    """Test that events with different idempotency keys are both processed."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-idem-2")

    envelope1 = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-idem-diff-1",
        run_id="run-idem-diff-1",
        idempotency_key="idem-key-alpha",
        tenant_id=tenant_id,
        node=NodeName.B,
        payload={"key": "alpha"},
    )
    await bus.publish(envelope1)

    envelope2 = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-idem-diff-2",
        run_id="run-idem-diff-2",
        idempotency_key="idem-key-beta",
        tenant_id=tenant_id,
        node=NodeName.B,
        payload={"key": "beta"},
    )
    await bus.publish(envelope2)

    spy = SpyHandler()
    handler_registry = {"test.ok": spy}

    await worker.run(handler_registry, stop_after=2)

    assert spy.call_count == 2


@pytest.mark.asyncio
async def test_pre_marked_event_skipped(clean_redis, tenant_id):
    """Test that pre-marked event is skipped without calling handler."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-idem-3")

    envelope = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-pre-marked",
        run_id="run-pre-marked",
        idempotency_key="pre-marked-key",
        tenant_id=tenant_id,
        node=NodeName.C,
        payload={"pre_marked": True},
    )

    await mark_processed(redis, envelope)

    await bus.publish(envelope)

    spy = SpyHandler()
    handler_registry = {"test.ok": spy}

    await worker.run(handler_registry, stop_after=1)

    assert spy.call_count == 0
