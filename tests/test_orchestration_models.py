"""Minimal unit tests for orchestration run models serialization."""

from uuid import uuid4

import pytest

from metismedia.orchestration.run_models import DossierResult, RunRecord, RunStatus


class TestRunStatus:
    """Test RunStatus enum serialization."""

    def test_run_status_values(self) -> None:
        """Test RunStatus enum values."""
        assert RunStatus.CREATED.value == "created"
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.SUCCEEDED.value == "succeeded"
        assert RunStatus.FAILED.value == "failed"

    def test_run_status_json_round_trip(self) -> None:
        """Test RunStatus serializes in Pydantic model."""
        record = RunRecord(tenant_id=uuid4(), trace_id=uuid4(), status=RunStatus.RUNNING)
        data = record.model_dump()
        assert data["status"] == "running"
        parsed = RunRecord.model_validate_json(record.model_dump_json())
        assert parsed.status == RunStatus.RUNNING


class TestDossierResult:
    """Test DossierResult serialization."""

    def test_dossier_result_creation(self) -> None:
        """Test creating a DossierResult."""
        run_id = uuid4()
        campaign_id = uuid4()
        result = DossierResult(
            run_id=run_id,
            campaign_id=campaign_id,
            targets_count=5,
            drafts_count=3,
            cost_summary={"dollars": 1.5},
            notes=["note1"],
        )
        assert result.run_id == run_id
        assert result.targets_count == 5
        assert result.cost_summary == {"dollars": 1.5}
        assert result.notes == ["note1"]

    def test_dossier_result_serialization_round_trip(self) -> None:
        """Test DossierResult JSON serialization round-trip."""
        run_id = uuid4()
        campaign_id = uuid4()
        result = DossierResult(
            run_id=run_id,
            campaign_id=campaign_id,
            targets_count=2,
            drafts_count=1,
            cost_summary={"provider_a": 0.5},
            notes=[],
        )
        json_str = result.model_dump_json()
        parsed = DossierResult.model_validate_json(json_str)
        assert parsed.run_id == result.run_id
        assert parsed.campaign_id == result.campaign_id
        assert parsed.targets_count == result.targets_count
        assert parsed.drafts_count == result.drafts_count
        assert parsed.cost_summary == result.cost_summary
        assert parsed.notes == result.notes
