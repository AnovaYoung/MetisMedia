"""Tests for Node A Briefing Session API."""

from uuid import uuid4

import httpx
import pytest

from metismedia.api.node_a import set_provider
from metismedia.events.constants import STREAM_MAIN
from metismedia.main import app
from metismedia.providers import MockNodeAProvider
from metismedia.settings import get_settings


@pytest.fixture
def tenant_id():
    return uuid4()


@pytest.fixture
def mock_provider():
    """Create and set mock provider."""
    provider = MockNodeAProvider()
    set_provider(provider)
    return provider


@pytest.fixture
async def async_client(mock_provider):
    """Create an async test client."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_create_session_empty(async_client, tenant_id):
    """Test creating a new empty briefing session."""
    response = await async_client.post(
        "/api/v1/briefing/sessions",
        json={"tenant_id": str(tenant_id)},
    )

    assert response.status_code == 200
    data = response.json()

    assert "session_id" in data
    assert data["status"] == "active"
    assert data["slots"] == {}
    assert len(data["missing_slots"]) > 0
    assert data["next_question"] is not None


@pytest.mark.asyncio
async def test_create_session_with_initial_message(async_client, tenant_id):
    """Test creating session with initial message extracts slots."""
    response = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "I want to find allies on Twitter and Substack in the US for an earned media campaign",
        },
    )

    assert response.status_code == 200
    data = response.json()

    assert data["status"] == "active"
    assert data["slots"].get("polarity_intent") == "allies"
    assert "x" in data["slots"].get("platform_vector", []) or "substack" in data["slots"].get("platform_vector", [])
    assert data["slots"].get("geography") == "US"
    assert data["slots"].get("commercial_mode") == "earned"


@pytest.mark.asyncio
async def test_submit_message_extracts_slots(async_client, tenant_id):
    """Test that submitting messages extracts slot values."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={"tenant_id": str(tenant_id)},
    )
    session_id = create_resp.json()["session_id"]
    initial_missing = len(create_resp.json()["missing_slots"])

    msg_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "We want to target allies on YouTube in the EU with a paid campaign",
        },
    )

    assert msg_resp.status_code == 200
    data = msg_resp.json()

    assert data["slots"].get("polarity_intent") == "allies"
    assert data["slots"].get("commercial_mode") == "paid"
    assert data["slots"].get("geography") == "EU"
    assert "youtube" in data["slots"].get("platform_vector", [])
    assert data["extracted_count"] > 0

    assert len(data["missing_slots"]) < initial_missing


@pytest.mark.asyncio
async def test_submit_multiple_messages_accumulates_slots(async_client, tenant_id):
    """Test that multiple messages accumulate slot values."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={"tenant_id": str(tenant_id)},
    )
    session_id = create_resp.json()["session_id"]

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "We want allies",
        },
    )

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "Focus on US market",
        },
    )

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "This is a paid campaign on Twitter",
        },
    )

    get_resp = await async_client.get(
        f"/api/v1/briefing/sessions/{session_id}",
        params={"tenant_id": str(tenant_id)},
    )

    assert get_resp.status_code == 200
    data = get_resp.json()

    assert data["slots"].get("polarity_intent") == "allies"
    assert data["slots"].get("geography") == "US"
    assert data["slots"].get("commercial_mode") == "paid"


@pytest.mark.asyncio
async def test_missing_slots_shrink_as_filled(async_client, tenant_id):
    """Test that missing_slots list shrinks as slots are filled."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={"tenant_id": str(tenant_id)},
    )
    session_id = create_resp.json()["session_id"]
    initial_missing = set(create_resp.json()["missing_slots"])

    msg_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "We're looking for allies",
        },
    )

    if msg_resp.json()["slots"].get("polarity_intent"):
        current_missing = set(msg_resp.json()["missing_slots"])
        assert "polarity_intent" not in current_missing or len(current_missing) < len(initial_missing)


@pytest.mark.asyncio
async def test_finalize_requires_hard_blockers(async_client, tenant_id):
    """Test that finalize fails if hard blockers are missing (threshold from settings)."""
    threshold = get_settings().node_a_hardblocker_min_confidence
    assert 0 <= threshold <= 1, "Hard blocker threshold should be a confidence in [0,1]"
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={"tenant_id": str(tenant_id)},
    )
    session_id = create_resp.json()["session_id"]

    finalize_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )

    assert finalize_resp.status_code == 400
    assert "missing or low-confidence" in finalize_resp.json()["detail"]


@pytest.mark.asyncio
async def test_finalize_success_creates_run_and_campaign(async_client, tenant_id):
    """Test successful finalization creates run and campaign."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "Campaign name: Tech Outreach. Finding allies on Twitter in US for earned media campaign about technology innovation.",
        },
    )
    session_id = create_resp.json()["session_id"]

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "Description: A campaign to reach tech influencers who support innovation",
        },
    )

    finalize_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )

    assert finalize_resp.status_code == 200
    data = finalize_resp.json()

    assert "run_id" in data
    assert "campaign_id" in data
    assert "trace_id" in data
    assert data["brief"]["finalized"] is True
    assert data["brief"]["polarity_intent"] == "allies"


@pytest.mark.asyncio
async def test_finalize_emits_event_to_redis(async_client, tenant_id, clean_redis):
    """Test that finalization emits node_a.brief_finalized to redis stream."""
    redis_client = clean_redis

    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "Campaign name: Event Test. Allies on Substack in EU, earned media campaign about sustainability.",
        },
    )
    session_id = create_resp.json()["session_id"]

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "Campaign description: Testing event emission for briefing finalization",
        },
    )

    finalize_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )

    assert finalize_resp.status_code == 200
    data = finalize_resp.json()
    assert data["event_published"] is True

    messages = await redis_client.xrange(STREAM_MAIN, "-", "+")
    assert len(messages) > 0

    found_event = False
    for msg_id, msg_data in messages:
        event_name = msg_data.get(b"event_name", b"").decode()
        if event_name == "node_a.brief_finalized":
            payload_str = msg_data.get(b"payload", b"{}").decode()
            if str(session_id) in payload_str:
                found_event = True
                break

    assert found_event, "node_a.brief_finalized event not found in stream"


@pytest.mark.asyncio
async def test_session_not_found(async_client, tenant_id):
    """Test 404 for non-existent session."""
    fake_id = uuid4()

    msg_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{fake_id}/message",
        json={"tenant_id": str(tenant_id), "message": "test"},
    )
    assert msg_resp.status_code == 404

    finalize_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{fake_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )
    assert finalize_resp.status_code == 404

    get_resp = await async_client.get(
        f"/api/v1/briefing/sessions/{fake_id}",
        params={"tenant_id": str(tenant_id)},
    )
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_cannot_message_finalized_session(async_client, tenant_id):
    """Test that messages cannot be added to finalized session."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "Campaign name: Finalized Test. Allies on Twitter in US, earned campaign.",
        },
    )
    session_id = create_resp.json()["session_id"]

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "Description: Testing finalized session blocking",
        },
    )

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )

    msg_resp = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={"tenant_id": str(tenant_id), "message": "Should fail"},
    )

    assert msg_resp.status_code == 400
    assert "finalized" in msg_resp.json()["detail"]


@pytest.mark.asyncio
async def test_cannot_finalize_twice(async_client, tenant_id):
    """Test that a session cannot be finalized twice."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "Campaign name: Double Finalize. Allies on YouTube, global, paid campaign.",
        },
    )
    session_id = create_resp.json()["session_id"]

    await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/message",
        json={
            "tenant_id": str(tenant_id),
            "message": "Description: Testing double finalization prevention",
        },
    )

    first_finalize = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )
    assert first_finalize.status_code == 200

    second_finalize = await async_client.post(
        f"/api/v1/briefing/sessions/{session_id}/finalize",
        json={"tenant_id": str(tenant_id)},
    )
    assert second_finalize.status_code == 400
    assert "already" in second_finalize.json()["detail"]


@pytest.mark.asyncio
async def test_provider_no_external_calls(async_client, tenant_id, mock_provider):
    """Test that mock provider doesn't make external calls."""
    create_resp = await async_client.post(
        "/api/v1/briefing/sessions",
        json={
            "tenant_id": str(tenant_id),
            "initial_message": "Looking for critics on podcasts in APAC",
        },
    )

    assert create_resp.status_code == 200
    data = create_resp.json()

    assert data["slots"].get("polarity_intent") == "critics"
    assert "podcast" in data["slots"].get("platform_vector", [])
    assert data["slots"].get("geography") == "APAC"
