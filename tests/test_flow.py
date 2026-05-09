import json

import pytest
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


@pytest.fixture(autouse=True)
def _enable_legacy_features(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("ENABLE_KNOWLEDGE_SYSTEM", "true")


def _login_admin() -> dict[str, str]:
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    token = res.json()["token"]
    return {"Authorization": f"Bearer {token}"}


def _parse_ndjson_events(response_text: str) -> list[dict]:
    return [json.loads(line) for line in response_text.splitlines() if line.strip()]


def _run_mock_iteration(headers: dict[str, str], message: str = "list all flights") -> tuple[list[dict], str, str]:
    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": message,
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "iteration_complete")
    return events, complete_event["data"]["session_id"], complete_event["data"]["proposal_id"]


def test_full_iterative_flow():
    headers = _login_admin()

    events, session_id, proposal_id = _run_mock_iteration(headers)

    event_types = [event["type"] for event in events]
    assert "thought" in event_types
    assert "result" in event_types
    assert "data" in event_types
    assert "iteration_complete" in event_types

    res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "test-skill"},
    )
    assert res.status_code == 200
    assert res.json()["skill"]["name"] == "test-skill"

    res = client.post(
        "/api/chat/feedback",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "feedback": "test feedback",
            "is_business_knowledge": True,
        },
    )
    assert res.status_code == 200
    assert res.json()["type"] == "business_knowledge"

    res = client.get(f"/api/chat/history?session_id={session_id}", headers=headers)
    assert res.status_code == 200
    assert len(res.json()["iterations"]) == 1


def test_table_limits():
    headers = _login_admin()

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "test",
            "selected_tables": ["t1", "t2", "t3", "t4", "t5", "t6"],
        },
    )
    assert res.status_code == 400
    assert "5" in res.json()["detail"]


def test_table_authorization():
    res = client.post("/api/auth/login", json={"provider": "oauth", "oauth_token": "oauth_marketing_bob"})
    assert res.status_code == 200
    headers = {"Authorization": f"Bearer {res.json()['token']}"}

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "test",
            "selected_tables": ["tutorial_flights", "secret_finance_table"],
        },
    )
    assert res.status_code == 403

