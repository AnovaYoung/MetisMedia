"""Integration tests for event retry and dead letter queue."""

import json
from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.events import EventBus, EventEnvelope, Worker
from metismedia.events.constants import MAX_RETRIES, STREAM_DLQ, STREAM_MAIN
from metismedia.events.handlers import handler_always_fail, make_handler_flaky


@pytest.fixture
def tenant_id():
    """Generate a tenant ID for tests."""
    return uuid4()


@pytest.fixture
def short_backoff(monkeypatch):
    """Reduce backoff times for faster tests."""
    import metismedia.events.worker as worker_module

    monkeypatch.setattr(worker_module, "BACKOFF_BASE_SECONDS", 0.01)
    monkeypatch.setattr(worker_module, "BACKOFF_JITTER_MAX", 0.001)


@pytest.mark.asyncio
async def test_event_moves_to_dlq_after_max_retries(clean_redis, short_backoff, tenant_id):
    """Test that event moves to DLQ after max retries exceeded."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-dlq")

    envelope = EventEnvelope(
        event_name="test.always_fail",
        trace_id="trace-dlq-1",
        run_id="run-dlq-1",
        idempotency_key="dlq-test-1",
        tenant_id=tenant_id,
        node=NodeName.C,
        payload={"will_fail": True},
    )

    await bus.publish(envelope)

    handler_registry = {"test.always_fail": handler_always_fail}

    total_messages = MAX_RETRIES
    processed = await worker.run(handler_registry, stop_after=total_messages)

    assert processed == MAX_RETRIES

    dlq_messages = await redis.xrange(STREAM_DLQ)
    assert len(dlq_messages) == 1

    dlq_message_id, dlq_data = dlq_messages[0]
    data = {k.decode(): v.decode() for k, v in dlq_data.items()}

    assert data["idempotency_key"] == "dlq-test-1"
    assert data["event_name"] == "test.always_fail"
    assert "error" in data
    assert "Always fails" in data["error"]
    assert int(data["attempt"]) == MAX_RETRIES


@pytest.mark.asyncio
async def test_flaky_handler_succeeds_after_retries(clean_redis, short_backoff, tenant_id):
    """Test that flaky handler eventually succeeds after retries."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-flaky")

    envelope = EventEnvelope(
        event_name="test.flaky",
        trace_id="trace-flaky-1",
        run_id="run-flaky-1",
        idempotency_key="flaky-test-1",
        tenant_id=tenant_id,
        node=NodeName.D,
        payload={"flaky": True},
    )

    await bus.publish(envelope)

    flaky_handler = make_handler_flaky(fail_until_attempt=3)
    handler_registry = {"test.flaky": flaky_handler}

    processed = await worker.run(handler_registry, stop_after=4)

    dlq_messages = await redis.xrange(STREAM_DLQ)
    assert len(dlq_messages) == 0


@pytest.mark.asyncio
async def test_retry_increments_attempt_counter(clean_redis, short_backoff, tenant_id):
    """Test that retry increments the attempt counter."""
    redis = clean_redis

    bus = EventBus(redis)
    worker = Worker(redis, bus, consumer_name="test-consumer-attempt")

    envelope = EventEnvelope(
        event_name="test.always_fail",
        trace_id="trace-attempt-1",
        run_id="run-attempt-1",
        idempotency_key="attempt-test-1",
        tenant_id=tenant_id,
        node=NodeName.E,
        payload={},
        attempt=0,
    )

    await bus.publish(envelope)

    handler_registry = {"test.always_fail": handler_always_fail}

    await worker.run(handler_registry, stop_after=2)

    messages = await redis.xrange(STREAM_MAIN)

    attempt_values = []
    for msg_id, msg_data in messages:
        data = {k.decode(): v.decode() for k, v in msg_data.items()}
        if data.get("idempotency_key") == "attempt-test-1":
            attempt_values.append(int(data.get("attempt", 0)))

    assert 1 in attempt_values or 2 in attempt_values
