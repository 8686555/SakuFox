import json

import pytest
from fastapi.testclient import TestClient

import app.main as main_module
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


def test_auto_analyze_defaults_to_ppt_report_format():
    headers = _login_admin()

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze default ppt",
            "provider": "mock",
        },
    )

    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")

    history = client.get(
        f"/api/chat/history?session_id={complete_event['data']['session_id']}",
        headers=headers,
    )
    assert history.status_code == 200
    saved = history.json()["iterations"][-1]
    assert saved["report_meta"]["report_format"] == "ppt"


def test_report_chat_revises_and_persists_html(monkeypatch):
    headers = _login_admin()
    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze then revise",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    complete_event = next(event for event in _parse_ndjson_events(res.text) if event["type"] == "analysis_complete")
    iteration_id = complete_event["data"]["iteration_id"]

    def fake_revise_report_html_document(**kwargs):
        assert kwargs["instruction"] == "改成更紧凑的管理层页"
        assert kwargs["current_html"]
        return {
            "title": "Revised Deck",
            "summary": "revised summary",
            "assistant_message": "已改成更紧凑的管理层页",
            "html_document": "<!doctype html><html><body><h1>revised html</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## revised",
        }

    monkeypatch.setattr(main_module, "revise_report_html_document", fake_revise_report_html_document)

    chat = client.post(
        f"/api/reports/iterations/{iteration_id}/chat",
        headers=headers,
        json={"message": "改成更紧凑的管理层页"},
    )

    assert chat.status_code == 200
    payload = chat.json()
    assert payload["assistant_message"] == "已改成更紧凑的管理层页"
    assert "revised html" in payload["html_document"]

    report = client.get(f"/api/reports/iterations/{iteration_id}", headers=headers)
    assert report.status_code == 200
    saved = report.json()
    assert "revised html" in saved["final_report_html"]
    assert saved["report_meta"]["report_chat_history"][-1]["content"] == "已改成更紧凑的管理层页"


def test_session_html_summary_uses_current_session_history(monkeypatch):
    headers = _login_admin()
    _, session_id, _ = _run_mock_iteration(headers, message="session summary source")
    captured = {}

    def fake_summarize_session_history_as_html(**kwargs):
        captured["session_id"] = kwargs["session_id"]
        captured["iterations"] = kwargs["iterations"]
        return {
            "title": "Session HTML",
            "summary": "session summary",
            "assistant_message": "已总结当前会话",
            "html_document": "<!doctype html><html><body><h1>session html</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## session",
        }

    monkeypatch.setattr(main_module, "summarize_session_history_as_html", fake_summarize_session_history_as_html)

    res = client.post(
        "/api/chat/session-html-summary",
        headers=headers,
        json={"session_id": session_id},
    )

    assert res.status_code == 200
    payload = res.json()
    assert payload["report_url"].startswith("/web/report.html?iteration_id=")
    assert "session html" in payload["html_document"]
    assert captured["session_id"] == session_id
    assert [item["session_id"] for item in captured["iterations"]] == [session_id]

    report = client.get(f"/api/reports/iterations/{payload['iteration_id']}", headers=headers)
    assert report.status_code == 200
    assert "session html" in report.json()["final_report_html"]


def test_session_html_summary_rejects_missing_complete_html(monkeypatch):
    headers = _login_admin()
    _, session_id, _ = _run_mock_iteration(headers, message="blank summary source")

    def fake_summarize_session_history_as_html(**kwargs):
        return {
            "title": "Blank HTML",
            "summary": "blank summary",
            "assistant_message": "bad html",
            "html_document": "",
            "chart_bindings": [],
        }

    monkeypatch.setattr(main_module, "summarize_session_history_as_html", fake_summarize_session_history_as_html)

    res = client.post(
        "/api/chat/session-html-summary",
        headers=headers,
        json={"session_id": session_id},
    )

    assert res.status_code == 502
    assert "complete HTML document" in res.json()["detail"]


def test_report_chat_rejects_missing_complete_html(monkeypatch):
    headers = _login_admin()
    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze then bad revise",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    complete_event = next(event for event in _parse_ndjson_events(res.text) if event["type"] == "analysis_complete")
    iteration_id = complete_event["data"]["iteration_id"]

    def fake_revise_report_html_document(**kwargs):
        return {
            "title": "Broken",
            "summary": "",
            "assistant_message": "broken",
            "html_document": "",
            "chart_bindings": [],
        }

    monkeypatch.setattr(main_module, "revise_report_html_document", fake_revise_report_html_document)

    chat = client.post(
        f"/api/reports/iterations/{iteration_id}/chat",
        headers=headers,
        json={"message": "改一下"},
    )

    assert chat.status_code == 502
    assert "complete HTML document" in chat.json()["detail"]

