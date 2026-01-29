"""Tests for settings helper."""

import pytest

from metismedia.settings import Settings, get_settings


def test_get_default_budget_provider_call_caps_invalid_json_logs_warning_and_returns_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When JSON parse fails, log warning (truncated to 200 chars) and return {}."""
    caplog.set_level("WARNING")
    settings = Settings(default_budget_provider_call_caps="not valid json")
    result = settings.get_default_budget_provider_call_caps()
    assert result == {}
    assert "metismedia.settings" in [r.name for r in caplog.records]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "not valid json" in warnings[0].message
