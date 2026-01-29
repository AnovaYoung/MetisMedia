"""Tests for cost ledger and JsonLogLedger."""

import json
import logging
from uuid import uuid4

import pytest

from metismedia.contracts.enums import NodeName
from metismedia.core.ledger import (
    COST_LOGGER_NAME,
    CostEntry,
    JsonLogLedger,
    compute_cost,
)


class TestComputeCost:
    """Test compute_cost helper."""

    def test_compute_cost(self) -> None:
        """Test compute_cost returns unit_cost * quantity rounded."""
        assert compute_cost(0.01, 100) == 1.0
        assert compute_cost(0.1, 3) == 0.3
        assert compute_cost(1.5, 2) == 3.0


class TestCostEntry:
    """Test CostEntry model."""

    def test_cost_entry_creation(self) -> None:
        """Test creating a CostEntry."""
        tenant_id = uuid4()
        entry = CostEntry(
            tenant_id=tenant_id,
            trace_id="trace-1",
            run_id="run-1",
            node=NodeName.B,
            provider="firecrawl",
            operation="scrape",
            unit_cost=0.01,
            quantity=10.0,
            dollars=0.1,
        )
        assert entry.tenant_id == tenant_id
        assert entry.provider == "firecrawl"
        assert entry.dollars == 0.1
        assert entry.node == NodeName.B


class TestJsonLogLedger:
    """Test JsonLogLedger records required fields (caplog)."""

    def test_json_log_ledger_records_dict_with_required_fields(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that JsonLogLedger records a JSON line with required fields."""
        caplog.set_level(logging.INFO)
        logger = logging.getLogger(COST_LOGGER_NAME)
        ledger = JsonLogLedger(logger=logger)

        tenant_id = uuid4()
        entry = CostEntry(
            tenant_id=tenant_id,
            trace_id="trace-1",
            run_id="run-1",
            node=NodeName.C,
            provider="exa",
            operation="search",
            unit_cost=0.02,
            quantity=5.0,
            dollars=0.1,
            metadata={"query": "test"},
        )

        ledger.record(entry)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.name == COST_LOGGER_NAME
        assert record.levelname == "INFO"
        payload = json.loads(record.message)
        assert payload["tenant_id"] == str(tenant_id)
        assert payload["trace_id"] == "trace-1"
        assert payload["run_id"] == "run-1"
        assert payload["node"] == NodeName.C.value
        assert payload["provider"] == "exa"
        assert payload["operation"] == "search"
        assert payload["unit_cost"] == 0.02
        assert payload["quantity"] == 5.0
        assert payload["dollars"] == 0.1
        assert payload["metadata"] == {"query": "test"}
        assert "occurred_at" in payload
