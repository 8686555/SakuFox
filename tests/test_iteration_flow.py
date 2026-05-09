import json

import app.main as main_module
import pandas as pd
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


def test_iterate_uses_post_execution_synthesis(monkeypatch):
    headers = _login_admin()
    captured = {"saw_rows": False}

    def fake_run_analysis_iteration(*, message, sandbox, iteration_history, business_knowledge, provider=None, model=None):
        yield {
            "type": "result",
            "data": {
                "steps": [
                    {
                        "tool": "sql",
                        "code": "SELECT department, SUM(cost) AS total_cost FROM tutorial_flights GROUP BY department ORDER BY total_cost DESC LIMIT 3",
                    }
                ],
                "tools_used": ["execute_select_sql"],
                "conclusions": [{"text": "planner conclusion should be replaced", "confidence": 0.2}],
                "hypotheses": [],
                "action_items": [],
                "direct_answer": "planner answer should be replaced",
                "explanation": "planner only",
                "goal": "find highest cost department",
                "observation_focus": "",
                "continue_reason": "",
                "stop_if": "",
                "finalize": False,
            },
        }

    def fake_synthesize_iteration_result(*, message, sandbox, iteration_history, business_knowledge, planned_result, execution_result, incremental=True, provider=None, model=None):
        rows = execution_result.get("rows") or []
        assert rows
        captured["saw_rows"] = True
        top_department = rows[0]["department"]
        return {
            "steps": [],
            "tools_used": [],
            "conclusions": [{"text": f"{top_department} 成本最高", "confidence": 1.0}],
            "hypotheses": [],
            "action_items": [f"继续分析 {top_department} 的成本驱动因素"],
            "direct_answer": f"成本最高的部门是 {top_department}",
            "explanation": "post execution synthesis",
            "final_report_outline": [],
            "direct_report": "",
            "goal": planned_result.get("goal", ""),
            "observation_focus": "",
            "continue_reason": "",
            "stop_if": "",
            "finalize": True,
        }

    monkeypatch.setattr(main_module, "run_analysis_iteration", fake_run_analysis_iteration)
    monkeypatch.setattr(main_module, "synthesize_iteration_result", fake_synthesize_iteration_result)

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "哪个部门成本最高",
            "provider": "openai",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    result_events = [event for event in events if event["type"] == "result"]
    assert result_events
    final_result = result_events[-1]["data"]
    assert captured["saw_rows"] is True
    assert "planner answer should be replaced" not in json.dumps(final_result, ensure_ascii=False)
    assert "成本最高的部门是" in final_result["direct_answer"]
    assert final_result["conclusions"][0]["text"].endswith("成本最高")

