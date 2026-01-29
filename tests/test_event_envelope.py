"""Tests for event envelope serialization and validation."""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from metismedia.contracts.enums import NodeName
from metismedia.events.constants import EVENT_CAMPAIGN_CREATED, EVENT_NODE_STARTED
from metismedia.events.envelope import EventEnvelope
from metismedia.events.idempotency import build_idem_key


class TestEventEnvelope:
    """Test EventEnvelope model."""

    def test_event_envelope_creation(self) -> None:
        """Test creating a valid EventEnvelope."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
        )
        assert envelope.event_name == EVENT_CAMPAIGN_CREATED
        assert envelope.trace_id == "trace-123"
        assert envelope.run_id == "run-456"
        assert envelope.idempotency_key == "key-789"
        assert envelope.tenant_id == tenant_id
        assert envelope.node == NodeName.A
        assert envelope.attempt == 0
        assert isinstance(envelope.event_id, UUID)
        assert isinstance(envelope.occurred_at, datetime)

    def test_event_envelope_with_node(self) -> None:
        """Test EventEnvelope with node."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_NODE_STARTED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
        )
        assert envelope.node == NodeName.A
        assert envelope.tenant_id == tenant_id

    def test_event_envelope_with_tenant_id(self) -> None:
        """Test EventEnvelope with tenant_id."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
        )
        assert envelope.tenant_id == tenant_id

    def test_event_envelope_with_payload(self) -> None:
        """Test EventEnvelope with payload."""
        tenant_id = uuid4()
        payload = {"campaign_id": str(uuid4()), "name": "Test Campaign"}
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
            payload=payload,
        )
        assert envelope.payload == payload

    def test_event_envelope_forbids_extra(self) -> None:
        """Test EventEnvelope forbids extra fields."""
        tenant_id = uuid4()
        with pytest.raises(ValidationError) as exc_info:
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                run_id="run-456",
                idempotency_key="key-789",
                tenant_id=tenant_id,
                node=NodeName.A,
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)

    def test_event_envelope_required_fields(self) -> None:
        """Test EventEnvelope requires essential fields."""
        tenant_id = uuid4()
        # Missing event_name
        with pytest.raises(ValidationError):
            EventEnvelope(
                trace_id="trace-123",
                run_id="run-456",
                idempotency_key="key-789",
                tenant_id=tenant_id,
                node=NodeName.A,
            )

        # Missing trace_id
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                run_id="run-456",
                idempotency_key="key-789",
                tenant_id=tenant_id,
                node=NodeName.A,
            )

        # Missing run_id
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                idempotency_key="key-789",
                tenant_id=tenant_id,
                node=NodeName.A,
            )

        # Missing idempotency_key
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                run_id="run-456",
                tenant_id=tenant_id,
                node=NodeName.A,
            )

        # Missing tenant_id
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                run_id="run-456",
                idempotency_key="key-789",
                node=NodeName.A,
            )

        # Missing node
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                run_id="run-456",
                idempotency_key="key-789",
                tenant_id=tenant_id,
            )

    def test_event_envelope_attempt_validation(self) -> None:
        """Test EventEnvelope attempt field validation."""
        tenant_id = uuid4()
        # Negative attempt should fail
        with pytest.raises(ValidationError):
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id="trace-123",
                run_id="run-456",
                idempotency_key="key-789",
                tenant_id=tenant_id,
                node=NodeName.A,
                attempt=-1,
            )

        # Valid attempt
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
            attempt=3,
        )
        assert envelope.attempt == 3


class TestEventEnvelopeSerialization:
    """Test EventEnvelope serialization."""

    def test_as_redis_fields(self) -> None:
        """Test as_redis_fields() returns all string values."""
        tenant_id = uuid4()
        event_id = uuid4()
        envelope = EventEnvelope(
            event_id=event_id,
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.B,
            payload={"test": "data"},
            attempt=2,
        )

        fields = envelope.as_redis_fields()

        assert isinstance(fields, dict)
        assert all(isinstance(v, str) for v in fields.values())
        assert fields["event_id"] == str(event_id)
        assert fields["tenant_id"] == str(tenant_id)
        assert fields["node"] == NodeName.B.value
        assert fields["event_name"] == EVENT_CAMPAIGN_CREATED
        assert fields["trace_id"] == "trace-123"
        assert fields["run_id"] == "run-456"
        assert fields["idempotency_key"] == "key-789"
        assert fields["attempt"] == "2"

        # Verify payload is JSON serialized
        payload_dict = json.loads(fields["payload"])
        assert payload_dict == {"test": "data"}

    def test_as_redis_fields_required_values(self) -> None:
        """Test as_redis_fields() with required tenant_id and node."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.C,
        )

        fields = envelope.as_redis_fields()

        assert fields["tenant_id"] == str(tenant_id)
        assert fields["node"] == NodeName.C.value

    def test_json_round_trip(self) -> None:
        """Test EventEnvelope JSON serialization round-trip."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
            payload={"campaign_id": "123"},
        )

        json_str = envelope.model_dump_json()
        parsed = EventEnvelope.model_validate_json(json_str)

        assert parsed.event_name == envelope.event_name
        assert parsed.trace_id == envelope.trace_id
        assert parsed.run_id == envelope.run_id
        assert parsed.idempotency_key == envelope.idempotency_key
        assert parsed.tenant_id == envelope.tenant_id
        assert parsed.node == envelope.node
        assert parsed.payload == envelope.payload


class TestIdempotencyHelpers:
    """Test idempotency helper functions."""

    def test_build_idem_key(self) -> None:
        """Test build_idem_key() function."""
        tenant_id = uuid4()
        envelope = EventEnvelope(
            event_name=EVENT_NODE_STARTED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.C,
        )

        key = build_idem_key(envelope)
        assert key == "idem:C:key-789"

    def test_build_idem_key_with_different_nodes(self) -> None:
        """Test build_idem_key() with different nodes."""
        tenant_id = uuid4()
        envelope_a = EventEnvelope(
            event_name=EVENT_NODE_STARTED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.A,
        )
        envelope_b = EventEnvelope(
            event_name=EVENT_NODE_STARTED,
            trace_id="trace-123",
            run_id="run-456",
            idempotency_key="key-789",
            tenant_id=tenant_id,
            node=NodeName.B,
        )

        key_a = build_idem_key(envelope_a)
        key_b = build_idem_key(envelope_b)
        assert key_a == "idem:A:key-789"
        assert key_b == "idem:B:key-789"
        assert key_a != key_b
