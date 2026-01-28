"""Unit tests for contract models - validation and serialization."""

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from metismedia.app.contracts.enums import (
    CommercialMode,
    NodeStatus,
    Platform,
    PolarityTarget,
    ReasonCode,
    ReceiptType,
)
from metismedia.app.contracts.models import (
    CampaignBrief,
    CampaignQuery,
    ContactBundle,
    ContactMethod,
    DiscoveryBatch,
    DiscoveryDirective,
    DraftPackage,
    DraftRecord,
    InfluencerEntity,
    InfluencerPlatform,
    NodeBDirective,
    Receipt,
    TargetCard,
)


class TestEnums:
    """Test enum serialization."""

    def test_polarity_target_serialization(self) -> None:
        """Test PolarityTarget enum serialization."""
        assert PolarityTarget.POSITIVE.value == "positive"
        assert PolarityTarget.NEGATIVE.value == "negative"
        assert PolarityTarget.NEUTRAL.value == "neutral"

    def test_commercial_mode_serialization(self) -> None:
        """Test CommercialMode enum serialization."""
        assert CommercialMode.PAID.value == "paid"
        assert CommercialMode.GIFTED.value == "gifted"
        assert CommercialMode.COLLABORATION.value == "collaboration"

    def test_platform_serialization(self) -> None:
        """Test Platform enum serialization."""
        assert Platform.INSTAGRAM.value == "instagram"
        assert Platform.TIKTOK.value == "tiktok"
        assert Platform.REDDIT.value == "reddit"

    def test_reason_code_serialization(self) -> None:
        """Test ReasonCode enum serialization."""
        assert ReasonCode.SAFETY_BLOCK.value == "safety_block"
        assert ReasonCode.SUCCESS.value == "success"


class TestCampaignBrief:
    """Test CampaignBrief model."""

    def test_campaign_brief_creation(self) -> None:
        """Test creating a valid CampaignBrief."""
        brief = CampaignBrief(
            name="Test Campaign",
            description="Test description",
            polarity_target=PolarityTarget.POSITIVE,
            commercial_mode=CommercialMode.PAID,
        )
        assert brief.name == "Test Campaign"
        assert brief.polarity_target == PolarityTarget.POSITIVE
        assert brief.commercial_mode == CommercialMode.PAID
        assert isinstance(brief.campaign_id, UUID)
        assert isinstance(brief.correlation_id, UUID)
        assert isinstance(brief.run_id, UUID)

    def test_campaign_brief_serialization(self) -> None:
        """Test CampaignBrief JSON serialization."""
        brief = CampaignBrief(
            name="Test Campaign",
            description="Test description",
            polarity_target=PolarityTarget.POSITIVE,
            commercial_mode=CommercialMode.PAID,
        )
        data = brief.model_dump()
        assert "name" in data
        assert "polarity_target" in data
        assert data["polarity_target"] == "positive"
        json_str = brief.model_dump_json()
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["name"] == "Test Campaign"

    def test_campaign_brief_forbid_extra(self) -> None:
        """Test CampaignBrief forbids extra fields."""
        with pytest.raises(ValidationError) as exc_info:
            CampaignBrief(
                name="Test",
                description="Test",
                polarity_target=PolarityTarget.POSITIVE,
                commercial_mode=CommercialMode.PAID,
                extra_field="not allowed",
            )
        assert "extra_field" in str(exc_info.value)


class TestCampaignQuery:
    """Test CampaignQuery model."""

    def test_campaign_query_creation(self) -> None:
        """Test creating a valid CampaignQuery."""
        campaign_id = uuid4()
        query = CampaignQuery(
            campaign_id=campaign_id,
            search_terms=["term1", "term2"],
            max_results=50,
        )
        assert query.campaign_id == campaign_id
        assert len(query.search_terms) == 2
        assert query.max_results == 50

    def test_campaign_query_serialization(self) -> None:
        """Test CampaignQuery JSON serialization."""
        query = CampaignQuery(campaign_id=uuid4(), search_terms=["test"])
        data = query.model_dump()
        assert "campaign_id" in data
        assert "search_terms" in data


class TestInfluencerEntity:
    """Test InfluencerEntity model."""

    def test_influencer_entity_creation(self) -> None:
        """Test creating a valid InfluencerEntity."""
        entity = InfluencerEntity(
            canonical_name="Test Influencer",
            bio="Test bio",
            verified=True,
        )
        assert entity.canonical_name == "Test Influencer"
        assert entity.bio == "Test bio"
        assert entity.verified is True

    def test_influencer_entity_with_embeddings(self) -> None:
        """Test InfluencerEntity with embeddings."""
        entity = InfluencerEntity(
            canonical_name="Test",
            bio_embedding=[0.1, 0.2, 0.3],
            recent_embedding=[0.4, 0.5, 0.6],
        )
        assert len(entity.bio_embedding) == 3
        assert len(entity.recent_embedding) == 3


class TestInfluencerPlatform:
    """Test InfluencerPlatform model."""

    def test_influencer_platform_creation(self) -> None:
        """Test creating a valid InfluencerPlatform."""
        influencer_id = uuid4()
        platform = InfluencerPlatform(
            influencer_id=influencer_id,
            platform=Platform.INSTAGRAM,
            handle="test_handle",
        )
        assert platform.influencer_id == influencer_id
        assert platform.platform == Platform.INSTAGRAM
        assert platform.handle == "test_handle"

    def test_influencer_platform_handle_validation(self) -> None:
        """Test InfluencerPlatform handle validation."""
        with pytest.raises(ValidationError):
            InfluencerPlatform(
                influencer_id=uuid4(),
                platform=Platform.INSTAGRAM,
                handle="",
            )


class TestReceipt:
    """Test Receipt model."""

    def test_receipt_creation(self) -> None:
        """Test creating a valid Receipt."""
        receipt = Receipt(
            receipt_type=ReceiptType.POST,
            platform=Platform.INSTAGRAM,
            url="https://example.com/post/1",
        )
        assert receipt.receipt_type == ReceiptType.POST
        assert receipt.platform == Platform.INSTAGRAM
        assert receipt.url == "https://example.com/post/1"

    def test_receipt_url_validation(self) -> None:
        """Test Receipt URL validation."""
        with pytest.raises(ValidationError):
            Receipt(
                receipt_type=ReceiptType.POST,
                platform=Platform.INSTAGRAM,
                url="",
            )

    def test_receipt_serialization(self) -> None:
        """Test Receipt JSON serialization."""
        receipt = Receipt(
            receipt_type=ReceiptType.POST,
            platform=Platform.INSTAGRAM,
            url="https://example.com/post/1",
            published_at=datetime.now(timezone.utc),
        )
        data = receipt.model_dump()
        assert "receipt_type" in data
        assert "published_at" in data


class TestTargetCard:
    """Test TargetCard model."""

    def test_target_card_creation(self) -> None:
        """Test creating a valid TargetCard."""
        influencer_id = uuid4()
        campaign_id = uuid4()
        receipt_id = uuid4()
        card = TargetCard(
            influencer_id=influencer_id,
            campaign_id=campaign_id,
            evidence_receipt_ids=[receipt_id],
            polarity_score=0.85,
        )
        assert card.influencer_id == influencer_id
        assert card.campaign_id == campaign_id
        assert len(card.evidence_receipt_ids) == 1
        assert card.polarity_score == 0.85

    def test_target_card_with_reason_codes(self) -> None:
        """Test TargetCard with reason codes."""
        card = TargetCard(
            influencer_id=uuid4(),
            campaign_id=uuid4(),
            reason_codes=[ReasonCode.INSUFFICIENT_EVIDENCE, ReasonCode.UNKNOWN_FIELD],
        )
        assert len(card.reason_codes) == 2


class TestContactMethod:
    """Test ContactMethod model."""

    def test_contact_method_creation(self) -> None:
        """Test creating a valid ContactMethod."""
        contact = ContactMethod(
            influencer_id=uuid4(),
            method_type="email",
            value="test@example.com",
        )
        assert contact.method_type == "email"
        assert contact.value == "test@example.com"
        assert contact.verified is False

    def test_contact_method_validation(self) -> None:
        """Test ContactMethod method_type validation."""
        with pytest.raises(ValidationError):
            ContactMethod(
                influencer_id=uuid4(),
                method_type="invalid",
                value="test",
            )


class TestContactBundle:
    """Test ContactBundle model."""

    def test_contact_bundle_creation(self) -> None:
        """Test creating a valid ContactBundle."""
        influencer_id = uuid4()
        contact1 = ContactMethod(
            influencer_id=influencer_id,
            method_type="email",
            value="test@example.com",
        )
        bundle = ContactBundle(
            influencer_id=influencer_id,
            contact_methods=[contact1],
        )
        assert len(bundle.contact_methods) == 1
        assert bundle.completeness_score == 0.0


class TestDraftRecord:
    """Test DraftRecord model."""

    def test_draft_record_creation(self) -> None:
        """Test creating a valid DraftRecord."""
        draft = DraftRecord(
            target_card_id=uuid4(),
            commercial_mode=CommercialMode.PAID,
            variant="standard",
            content="Test draft content",
        )
        assert draft.commercial_mode == CommercialMode.PAID
        assert draft.variant == "standard"
        assert draft.content == "Test draft content"

    def test_draft_record_with_receipts(self) -> None:
        """Test DraftRecord with included receipt IDs."""
        receipt_id = uuid4()
        draft = DraftRecord(
            target_card_id=uuid4(),
            commercial_mode=CommercialMode.PAID,
            variant="standard",
            content="Test",
            included_receipt_ids=[receipt_id],
        )
        assert len(draft.included_receipt_ids) == 1


class TestDraftPackage:
    """Test DraftPackage model."""

    def test_draft_package_creation(self) -> None:
        """Test creating a valid DraftPackage."""
        package = DraftPackage(
            target_card_id=uuid4(),
            campaign_id=uuid4(),
            status=NodeStatus.COMPLETED,
        )
        assert package.status == NodeStatus.COMPLETED
        assert len(package.drafts) == 0

    def test_draft_package_with_drafts(self) -> None:
        """Test DraftPackage with draft records."""
        draft = DraftRecord(
            target_card_id=uuid4(),
            commercial_mode=CommercialMode.PAID,
            variant="standard",
            content="Test",
        )
        package = DraftPackage(
            target_card_id=uuid4(),
            campaign_id=uuid4(),
            drafts=[draft],
        )
        assert len(package.drafts) == 1


class TestNodeBDirective:
    """Test NodeBDirective model."""

    def test_node_b_directive_creation(self) -> None:
        """Test creating a valid NodeBDirective."""
        directive = NodeBDirective(
            campaign_id=uuid4(),
            influencer_id=uuid4(),
            action="proceed",
            reason_codes=[ReasonCode.SUCCESS],
        )
        assert directive.action == "proceed"
        assert len(directive.reason_codes) == 1

    def test_node_b_directive_action_validation(self) -> None:
        """Test NodeBDirective action validation."""
        with pytest.raises(ValidationError):
            NodeBDirective(
                campaign_id=uuid4(),
                influencer_id=uuid4(),
                action="invalid_action",
            )


class TestDiscoveryDirective:
    """Test DiscoveryDirective model."""

    def test_discovery_directive_creation(self) -> None:
        """Test creating a valid DiscoveryDirective."""
        directive = DiscoveryDirective(
            campaign_id=uuid4(),
            squad="beta",
            max_results=100,
        )
        assert directive.squad == "beta"
        assert directive.max_results == 100


class TestDiscoveryBatch:
    """Test DiscoveryBatch model."""

    def test_discovery_batch_creation(self) -> None:
        """Test creating a valid DiscoveryBatch."""
        batch = DiscoveryBatch(
            directive_id=uuid4(),
            status=NodeStatus.COMPLETED,
        )
        assert batch.status == NodeStatus.COMPLETED
        assert len(batch.receipts) == 0

    def test_discovery_batch_with_receipts(self) -> None:
        """Test DiscoveryBatch with receipts."""
        receipt = Receipt(
            receipt_type=ReceiptType.POST,
            platform=Platform.INSTAGRAM,
            url="https://example.com/post/1",
        )
        batch = DiscoveryBatch(
            directive_id=uuid4(),
            receipts=[receipt],
        )
        assert len(batch.receipts) == 1

    def test_discovery_batch_serialization(self) -> None:
        """Test DiscoveryBatch JSON serialization."""
        batch = DiscoveryBatch(
            directive_id=uuid4(),
            status=NodeStatus.PENDING,
            cost_actual=10.5,
        )
        data = batch.model_dump()
        assert "status" in data
        assert "cost_actual" in data
        assert data["cost_actual"] == 10.5


class TestModelProvenance:
    """Test provenance fields across models."""

    def test_all_models_have_provenance(self) -> None:
        """Test that all models have provenance fields."""
        models = [
            CampaignBrief(
                name="Test",
                description="Test",
                polarity_target=PolarityTarget.POSITIVE,
                commercial_mode=CommercialMode.PAID,
            ),
            CampaignQuery(campaign_id=uuid4()),
            InfluencerEntity(canonical_name="Test"),
        ]
        for model in models:
            assert isinstance(model.correlation_id, UUID)
            assert isinstance(model.run_id, UUID)
            assert isinstance(model.provenance, dict)
            assert isinstance(model.created_at, datetime)
            assert isinstance(model.updated_at, datetime)


class TestModelForbidExtra:
    """Test that all models forbid extra fields."""

    def test_models_forbid_extra_fields(self) -> None:
        """Test that models reject extra fields."""
        with pytest.raises(ValidationError):
            CampaignBrief(
                name="Test",
                description="Test",
                polarity_target=PolarityTarget.POSITIVE,
                commercial_mode=CommercialMode.PAID,
                extra_field="not allowed",
            )

        with pytest.raises(ValidationError):
            Receipt(
                receipt_type=ReceiptType.POST,
                platform=Platform.INSTAGRAM,
                url="https://example.com",
                extra_field="not allowed",
            )
