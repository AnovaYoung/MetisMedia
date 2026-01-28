"""Core tests for canonical contracts - validation and serialization."""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from metismedia.contracts.enums import (
    CacheStatus,
    CommercialMode,
    NodeName,
    Platform,
    PolarityIntent,
    PulseStatus,
    ReceiptType,
)
from metismedia.contracts.events import (
    EVENT_CAMPAIGN_CREATED,
    EVENT_NODE_STARTED,
    EventEnvelope,
)
from metismedia.contracts.models import (
    CampaignBrief,
    CampaignQuery,
    CostEstimate,
    DirectiveObject,
    InfluencerEntity,
    RawCandidate,
    Receipt,
    TargetCard,
)
from metismedia.contracts.reasons import ReasonCode

# Note: ReasonCode.SUCCESS doesn't exist in the new stable codes
# Using a valid code for testing


class TestEnumsStability:
    """Test enum stability and serialization."""

    def test_polarity_intent_values(self) -> None:
        """Test PolarityIntent enum values."""
        assert PolarityIntent.ALLIES.value == "allies"
        assert PolarityIntent.CRITICS.value == "critics"
        assert PolarityIntent.WATCHLIST.value == "watchlist"

    def test_commercial_mode_values(self) -> None:
        """Test CommercialMode enum values."""
        assert CommercialMode.EARNED.value == "earned"
        assert CommercialMode.PAID.value == "paid"
        assert CommercialMode.HYBRID.value == "hybrid"
        assert CommercialMode.UNKNOWN.value == "unknown"

    def test_cache_status_values(self) -> None:
        """Test CacheStatus enum values."""
        assert CacheStatus.CACHE_HIT.value == "cache_hit"
        assert CacheStatus.PARTIAL_HIT.value == "partial_hit"
        assert CacheStatus.CACHE_MISS.value == "cache_miss"

    def test_pulse_status_values(self) -> None:
        """Test PulseStatus enum values."""
        assert PulseStatus.PASS.value == "pass"
        assert PulseStatus.FAIL.value == "fail"
        assert PulseStatus.INCONCLUSIVE.value == "inconclusive"

    def test_receipt_type_values(self) -> None:
        """Test ReceiptType enum values."""
        assert ReceiptType.SOCIAL.value == "social"
        assert ReceiptType.CREATOR.value == "creator"
        assert ReceiptType.THREAD.value == "thread"
        assert ReceiptType.AUDIO.value == "audio"

    def test_node_name_values(self) -> None:
        """Test NodeName enum values."""
        assert NodeName.A.value == "A"
        assert NodeName.B.value == "B"
        assert NodeName.G.value == "G"

    def test_platform_values(self) -> None:
        """Test Platform enum values."""
        assert Platform.X.value == "x"
        assert Platform.BLUESKY.value == "bluesky"
        assert Platform.REDDIT.value == "reddit"
        assert Platform.OTHER.value == "other"


class TestReasonCodes:
    """Test reason codes stability."""

    def test_safety_reason_codes(self) -> None:
        """Test safety reason codes."""
        assert ReasonCode.SAFETY_BURNOUT.value == "safety_burnout"
        assert ReasonCode.SAFETY_COOLDOWN.value == "safety_cooldown"
        assert ReasonCode.SAFETY_OPT_OUT.value == "safety_opt_out"

    def test_filter_reason_codes(self) -> None:
        """Test filter reason codes."""
        assert ReasonCode.THIRD_RAIL_MATCH.value == "third_rail_match"
        assert ReasonCode.PLATFORM_MISMATCH.value == "platform_mismatch"
        assert ReasonCode.GEO_MISMATCH.value == "geo_mismatch"

    def test_budget_reason_codes(self) -> None:
        """Test budget reason codes."""
        assert ReasonCode.BUDGET_EXHAUSTED.value == "budget_exhausted"
        assert ReasonCode.TIME_BUDGET_EXHAUSTED.value == "time_budget_exhausted"


class TestSerializationRoundTrip:
    """Test serialization round-trip for models."""

    def test_campaign_brief_round_trip(self) -> None:
        """Test CampaignBrief serialization round-trip."""
        brief = CampaignBrief(
            name="Test Campaign",
            description="Test description",
            polarity_intent=PolarityIntent.ALLIES,
            commercial_mode=CommercialMode.PAID,
        )
        data = brief.model_dump()
        json_str = brief.model_dump_json()
        parsed = CampaignBrief.model_validate_json(json_str)
        assert parsed.name == brief.name
        assert parsed.polarity_intent == brief.polarity_intent

    def test_receipt_round_trip(self) -> None:
        """Test Receipt serialization round-trip."""
        receipt = Receipt(
            receipt_type=ReceiptType.SOCIAL,
            platform=Platform.X,
            url="https://example.com/post/1",
        )
        json_str = receipt.model_dump_json()
        parsed = Receipt.model_validate_json(json_str)
        assert parsed.url == receipt.url
        assert parsed.receipt_type == receipt.receipt_type

    def test_target_card_round_trip(self) -> None:
        """Test TargetCard serialization round-trip."""
        card = TargetCard(
            influencer_id=uuid4(),
            campaign_id=uuid4(),
            polarity_score=0.85,
        )
        json_str = card.model_dump_json()
        parsed = TargetCard.model_validate_json(json_str)
        assert parsed.polarity_score == card.polarity_score


class TestExtraForbidden:
    """Test that all models forbid extra fields."""

    def test_campaign_brief_forbids_extra(self) -> None:
        """Test CampaignBrief forbids extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            CampaignBrief(
                name="Test",
                description="Test",
                polarity_intent=PolarityIntent.ALLIES,
                commercial_mode=CommercialMode.PAID,
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)

    def test_receipt_forbids_extra(self) -> None:
        """Test Receipt forbids extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            Receipt(
                receipt_type=ReceiptType.SOCIAL,
                platform=Platform.X,
                url="https://example.com",
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)

    def test_raw_candidate_forbids_extra(self) -> None:
        """Test RawCandidate forbids extra fields."""
        receipt = Receipt(
            receipt_type=ReceiptType.SOCIAL,
            platform=Platform.X,
            url="https://example.com",
        )
        with pytest.raises(ValidationError) as exc_info:
            RawCandidate(
                receipts=[receipt],
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)


class TestRawCandidateReceiptsRequired:
    """Test that RawCandidate requires non-empty receipts."""

    def test_raw_candidate_receipts_not_empty(self) -> None:
        """Test RawCandidate cannot have empty receipts."""
        with pytest.raises(ValidationError) as exc_info:
            RawCandidate(receipts=[])
        error_str = str(exc_info.value).lower()
        assert "receipts" in error_str
        assert (
            "at least 1" in error_str
            or "too_short" in error_str
            or "min_length" in error_str
        )

    def test_raw_candidate_with_receipts(self) -> None:
        """Test RawCandidate with valid receipts."""
        receipt = Receipt(
            receipt_type=ReceiptType.SOCIAL,
            platform=Platform.X,
            url="https://example.com/post/1",
        )
        candidate = RawCandidate(receipts=[receipt])
        assert len(candidate.receipts) == 1
        assert candidate.receipts[0].url == receipt.url


class TestEventEnvelope:
    """Test EventEnvelope requirements."""

    def test_event_envelope_requires_trace_id(self) -> None:
        """Test EventEnvelope requires trace_id."""
        with pytest.raises(ValidationError) as exc_info:
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id=None,  # type: ignore
                idempotency_key="test-key",
            )
        assert "trace_id" in str(exc_info.value).lower()

    def test_event_envelope_requires_idempotency_key(self) -> None:
        """Test EventEnvelope requires idempotency_key."""
        with pytest.raises(ValidationError) as exc_info:
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id=uuid4(),
                idempotency_key="",
            )
        assert "idempotency_key" in str(exc_info.value).lower()

    def test_event_envelope_valid(self) -> None:
        """Test EventEnvelope with valid fields."""
        envelope = EventEnvelope(
            event_name=EVENT_NODE_STARTED,
            trace_id=uuid4(),
            idempotency_key="test-key-123",
            node="B",
        )
        assert envelope.trace_id is not None
        assert envelope.idempotency_key == "test-key-123"
        assert envelope.event_name == EVENT_NODE_STARTED

    def test_event_envelope_serialization(self) -> None:
        """Test EventEnvelope serialization."""
        envelope = EventEnvelope(
            event_name=EVENT_CAMPAIGN_CREATED,
            trace_id=uuid4(),
            idempotency_key="test-key",
        )
        json_str = envelope.model_dump_json()
        parsed = EventEnvelope.model_validate_json(json_str)
        assert parsed.trace_id == envelope.trace_id
        assert parsed.idempotency_key == envelope.idempotency_key

    def test_event_envelope_forbids_extra(self) -> None:
        """Test EventEnvelope forbids extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            EventEnvelope(
                event_name=EVENT_CAMPAIGN_CREATED,
                trace_id=uuid4(),
                idempotency_key="test-key",
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)


class TestProvenanceFields:
    """Test provenance fields across models."""

    def test_models_have_provenance(self) -> None:
        """Test that models have provenance fields."""
        brief = CampaignBrief(
            name="Test",
            description="Test",
            polarity_intent=PolarityIntent.ALLIES,
            commercial_mode=CommercialMode.PAID,
        )
        assert isinstance(brief.trace_id, UUID)
        assert isinstance(brief.run_id, UUID)
        assert isinstance(brief.provenance, dict)
        assert isinstance(brief.created_at, datetime)

    def test_receipt_has_provenance(self) -> None:
        """Test Receipt has provenance."""
        receipt = Receipt(
            receipt_type=ReceiptType.SOCIAL,
            platform=Platform.X,
            url="https://example.com",
        )
        assert isinstance(receipt.trace_id, UUID)
        assert isinstance(receipt.provenance, dict)

    def test_raw_candidate_has_provenance(self) -> None:
        """Test RawCandidate has provenance."""
        receipt = Receipt(
            receipt_type=ReceiptType.SOCIAL,
            platform=Platform.X,
            url="https://example.com",
        )
        candidate = RawCandidate(receipts=[receipt])
        assert isinstance(candidate.trace_id, UUID)
        assert isinstance(candidate.provenance, dict)


class TestDirectiveObject:
    """Test DirectiveObject model."""

    def test_directive_object_creation(self) -> None:
        """Test creating a valid DirectiveObject."""
        directive = DirectiveObject(
            campaign_id=uuid4(),
            action="proceed",
            reason_codes=[ReasonCode.MMS_BELOW_CACHE],
            cache_status=CacheStatus.CACHE_HIT,
            pulse_status=PulseStatus.PASS,
        )
        assert directive.action == "proceed"
        assert len(directive.reason_codes) == 1
        assert directive.cache_status == CacheStatus.CACHE_HIT

    def test_directive_object_action_validation(self) -> None:
        """Test DirectiveObject action validation."""
        with pytest.raises(ValidationError):
            DirectiveObject(
                campaign_id=uuid4(),
                action="invalid_action",
            )

    def test_directive_object_with_cost_estimate(self) -> None:
        """Test DirectiveObject with cost estimate."""
        cost_est = CostEstimate(
            operation="node_b",
            estimated_cost=0.05,
        )
        directive = DirectiveObject(
            campaign_id=uuid4(),
            action="proceed",
            cost_estimate=cost_est,
        )
        assert directive.cost_estimate is not None
        assert directive.cost_estimate.estimated_cost == 0.05
