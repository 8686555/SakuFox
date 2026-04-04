import json

import app.main as main_module
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


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


def test_mount_skill_context_and_sandbox_payload(monkeypatch):
    headers = _login_admin()

    client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": []},
    )

    _, session_id, proposal_id = _run_mock_iteration(headers, message="build a reusable skill")

    res = client.post(
        "/api/skills/save",
        headers=headers,
        json={
            "proposal_id": proposal_id,
            "name": "mounted-skill",
            "knowledge": ["rule-a", "rule-a", "rule-b"],
        },
    )
    assert res.status_code == 200
    skill_id = res.json()["skill"]["skill_id"]

    res = client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": [skill_id, skill_id, ""]},
    )
    assert res.status_code == 200
    assert res.json()["skills"] == [skill_id]

    res = client.get("/api/sandboxes", headers=headers)
    assert res.status_code == 200
    sandbox = next(item for item in res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    assert "knowledge_bases" in sandbox
    assert "mounted_skills" in sandbox
    assert sandbox["mounted_skills"] == [skill_id]

    captured: dict[str, list[str]] = {}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["business_knowledge"] = business_knowledge
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "message": "verify mounted skill context",
            "provider": "mock",
        },
    )
    assert res.status_code == 200

    business_knowledge = captured["business_knowledge"]
    assert "[mounted-skill]: rule-a" in business_knowledge
    assert "[mounted-skill]: rule-b" in business_knowledge
    assert business_knowledge.count("[mounted-skill]: rule-a") == 1


def test_mount_unknown_skill_returns_400():
    headers = _login_admin()

    res = client.post(
        "/api/sandboxes/sb_flights_overview/skills",
        headers=headers,
        json={"skills": ["sk_missing"]},
    )
    assert res.status_code == 400
    assert "Skills not found" in res.json()["detail"]


def test_auto_analyze_stops_when_model_stops_using_tools_and_persists_report(monkeypatch):
    headers = _login_admin()
    call_state = {"count": 0}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        call_state["count"] += 1
        if call_state["count"] == 1:
            yield {
                "type": "thought",
                "content": "round one",
            }
            yield {
                "type": "result",
                "data": {
                    "steps": [{"tool": "sql", "code": "SELECT * FROM tutorial_flights LIMIT 1"}],
                    "conclusions": [{"text": "first conclusion", "confidence": 0.8}],
                    "hypotheses": [{"id": "h1", "text": "verify next"}],
                    "action_items": ["keep digging"],
                    "tools_used": ["execute_select_sql"],
                    "explanation": "first round",
                    "final_report_outline": ["summary"],
                },
            }
        else:
            yield {
                "type": "result",
                "data": {
                    "steps": [],
                    "conclusions": [{"text": "final conclusion", "confidence": 0.9}],
                    "hypotheses": [{"id": "h2", "text": "follow-up"}],
                    "action_items": ["ship report"],
                    "tools_used": [],
                    "explanation": "final round",
                    "final_report_outline": ["final"],
                },
            }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "done",
            "html_document": "<!doctype html><html><body><h1>done</h1><div data-chart-id=\"chart_1\"></div></body></html>",
            "chart_bindings": [{"chart_id": "chart_1", "option": {"xAxis": {}, "yAxis": {}, "series": []}, "height": 320}],
            "legacy_markdown": "## Executive Summary\n- done",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- done")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze flights",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    event_types = [event["type"] for event in events]
    assert "loop_status" in event_types
    assert "loop_round" in event_types
    assert "report" in event_types
    assert "analysis_complete" in event_types

    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["rounds_completed"] == 2
    assert complete_event["data"]["max_rounds_hit"] is False
    assert complete_event["data"]["report_url"].startswith("/web/report.html?iteration_id=")
    assert complete_event["data"]["report_title"] == "Auto Report"

    report_event = next(event for event in events if event["type"] == "report")
    assert report_event["data"]["title"] == "Auto Report"
    assert report_event["data"]["summary"] == "done"

    history = client.get(
        f"/api/chat/history?session_id={complete_event['data']['session_id']}",
        headers=headers,
    )
    assert history.status_code == 200
    saved = history.json()["iterations"][0]
    assert saved["mode"] == "auto_analysis"
    assert saved["final_report_md"].startswith("## Executive Summary")
    assert saved["report_title"] == "Auto Report"
    assert "data-chart-id" in saved["final_report_html"]
    assert saved["final_report_summary"] == "done"
    assert len(saved["final_report_chart_bindings"]) == 1
    assert len(saved["loop_rounds"]) == 2

    report_res = client.get(
        f"/api/reports/iterations/{complete_event['data']['iteration_id']}",
        headers=headers,
    )
    assert report_res.status_code == 200
    payload = report_res.json()
    assert payload["report_title"] == "Auto Report"
    assert payload["final_report_summary"] == "done"
    assert len(payload["final_report_chart_bindings"]) == 1


def test_auto_analyze_marks_max_rounds_hit(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [{"tool": "sql", "code": "SELECT * FROM tutorial_flights LIMIT 1"}],
                "conclusions": [{"text": "still working", "confidence": 0.6}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": ["execute_select_sql"],
                "explanation": "keep going",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "partial",
            "html_document": "<!doctype html><html><body><h1>partial</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- partial",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- partial")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "auto analyze cap",
            "provider": "mock",
            "max_rounds": 1,
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert complete_event["data"]["max_rounds_hit"] is True
    assert complete_event["data"]["stop_reason"] == "max_rounds_reached"


def test_auto_analyze_injects_session_patches_and_skill_save_works(monkeypatch):
    headers = _login_admin()
    _, session_id, _ = _run_mock_iteration(headers, message="seed session")

    feedback = client.post(
        "/api/chat/feedback",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "feedback": "session patch rule",
            "is_business_knowledge": False,
        },
    )
    assert feedback.status_code == 200

    captured: dict[str, list[str]] = {}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["business_knowledge"] = business_knowledge
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Report",
            "summary": "session aware",
            "html_document": "<!doctype html><html><body><h1>session aware</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- session aware",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- session aware")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": session_id,
            "message": "auto analyze with patch",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete_event = next(event for event in events if event["type"] == "analysis_complete")
    assert "[Session Patch]: session patch rule" in captured["business_knowledge"]

    save_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": complete_event["data"]["proposal_id"], "name": "auto-analysis-skill"},
    )
    assert save_res.status_code == 200
    saved_skill = save_res.json()["skill"]
    assert saved_skill["name"] == "auto-analysis-skill"


def test_auto_analyze_no_tool_call_with_direct_report_does_not_surface_parse_error(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "JSON parse failed", "confidence": 0.0}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "bad json",
                "final_report_outline": [],
                "direct_report": "## Executive Summary\n- final report",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "finish with direct report",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    report_event = next(event for event in events if event["type"] == "report")
    assert report_event["data"]["summary"].startswith("## Executive Summary")
    loop_round = next(event for event in events if event["type"] == "loop_round")
    assert loop_round["data"]["result"]["conclusions"] == []


def test_auto_analyze_allows_empty_message_and_returns_report_url(monkeypatch):
    headers = _login_admin()

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Empty",
            "summary": "empty start",
            "html_document": "<!doctype html><html><body><h1>empty start</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- empty",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- empty")

    res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete = next(event for event in events if event["type"] == "analysis_complete")
    assert complete["data"]["report_url"].startswith("/web/report.html?iteration_id=")
    assert complete["data"]["report_title"] == "Auto Empty"


def test_iterate_receives_latest_auto_report_summary_in_history(monkeypatch):
    headers = _login_admin()
    captured: dict[str, list[dict]] = {}

    def fake_run_analysis_iteration_auto(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "auto done", "confidence": 0.8}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "done",
                "final_report_outline": [],
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration_auto)
    monkeypatch.setattr(
        main_module,
        "generate_auto_analysis_report_bundle",
        lambda **kwargs: {
            "title": "Auto Context",
            "summary": "context summary",
            "html_document": "<!doctype html><html><body><h1>context</h1></body></html>",
            "chart_bindings": [],
            "legacy_markdown": "## Executive Summary\n- context",
        },
    )
    monkeypatch.setattr(main_module, "generate_auto_analysis_report", lambda **kwargs: "## Executive Summary\n- context")

    auto_res = client.post(
        "/api/chat/auto-analyze",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "first auto",
            "provider": "mock",
        },
    )
    assert auto_res.status_code == 200
    auto_events = _parse_ndjson_events(auto_res.text)
    auto_complete = next(event for event in auto_events if event["type"] == "analysis_complete")

    def fake_run_analysis_iteration_manual(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        captured["iteration_history"] = iteration_history
        yield {
            "type": "result",
            "data": {
                "steps": [],
                "conclusions": [{"text": "manual done", "confidence": 0.7}],
                "hypotheses": [],
                "action_items": [],
                "tools_used": [],
                "explanation": "manual",
            },
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration_manual)
    iterate_res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "session_id": auto_complete["data"]["session_id"],
            "message": "continue based on report",
            "provider": "mock",
        },
    )
    assert iterate_res.status_code == 200
    history = captured["iteration_history"]
    assert any(
        str(item.get("report_title", "")) == "Auto Context"
        and str(item.get("final_report_summary", "")) == "context summary"
        for item in history
    )
