"""Integration tests for event bus roundtrip."""

from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.events import EventBus, EventEnvelope, Worker
from metismedia.events.handlers import SpyHandler
from metismedia.events.idempotency import build_idem_key


@pytest.fixture
def tenant_id():
    """Generate a tenant ID for tests."""
    return uuid4()


@pytest.mark.asyncio
async def test_publish_and_consume_roundtrip(clean_redis, tenant_id):
    """Test publishing an event and consuming it successfully."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-1")

    envelope = EventEnvelope(
        event_name="test.ok",
        trace_id="trace-roundtrip-1",
        run_id="run-roundtrip-1",
        idempotency_key="roundtrip-test-1",
        tenant_id=tenant_id,
        node=NodeName.A,
        payload={"message": "hello"},
    )

    message_id = await bus.publish(envelope)
    assert message_id is not None
    assert "-" in message_id

    spy = SpyHandler()
    handler_registry = {"test.ok": spy}

    processed = await worker.run(handler_registry, stop_after=1)

    assert processed == 1
    assert spy.call_count == 1
    assert spy.envelopes[0].idempotency_key == "roundtrip-test-1"
    assert spy.envelopes[0].payload == {"message": "hello"}

    idem_key = build_idem_key(envelope)
    exists = await redis.exists(idem_key)
    assert exists == 1


@pytest.mark.asyncio
async def test_publish_multiple_events(clean_redis, tenant_id):
    """Test publishing and consuming multiple events."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-2")

    for i in range(3):
        envelope = EventEnvelope(
            event_name="test.ok",
            trace_id=f"trace-multi-{i}",
            run_id=f"run-multi-{i}",
            idempotency_key=f"multi-test-{i}",
            tenant_id=tenant_id,
            node=NodeName.B,
            payload={"index": i},
        )
        await bus.publish(envelope)

    spy = SpyHandler()
    handler_registry = {"test.ok": spy}

    processed = await worker.run(handler_registry, stop_after=3)

    assert processed == 3
    assert spy.call_count == 3
