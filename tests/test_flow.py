import json
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_full_iterative_flow():
    # 1. Login
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    token = res.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Iterate (Mock LLM)
    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "查一下所有航班",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    lines = [line for line in res.text.split("\n") if line.strip()]
    events = [json.loads(line) for line in lines]

    # Should contain thought -> result -> data -> iteration_complete
    types = [e["type"] for e in events]
    assert "thought" in types
    assert "result" in types
    assert "data" in types
    assert "iteration_complete" in types

    # Extract IDs for next steps
    comp_event = next(e for e in events if e["type"] == "iteration_complete")
    session_id = comp_event["data"]["session_id"]
    proposal_id = comp_event["data"]["proposal_id"]
    result_count = comp_event["data"]["result_count"]
    
    # 3. Save skill
    res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "测试技能"},
    )
    assert res.status_code == 200
    assert res.json()["skill"]["name"] == "测试技能"

    # 4. Feedback / Business Knowledge
    res = client.post(
        "/api/chat/feedback",
        headers=headers,
        json={"sandbox_id": "sb_flights_overview", "session_id": session_id, "feedback": "这是一个测试反馈", "is_business_knowledge": True},
    )
    assert res.status_code == 200
    assert res.json()["type"] == "business_knowledge"

    # 5. Iteration History
    res = client.get(f"/api/chat/history?session_id={session_id}", headers=headers)
    assert res.status_code == 200
    assert len(res.json()["iterations"]) == 1


def test_table_limits():
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    token = res.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}

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
    assert "最多可选择 5 张表" in res.json()["detail"]


def test_table_authorization():
    # Login as bob (marketing) who shouldn't have access to unconfigured tables
    res = client.post("/api/auth/login", json={"provider": "oauth", "oauth_token": "oauth_marketing_bob"})
    token = res.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}

    res = client.post(
        "/api/chat/iterate",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "test",
            "selected_tables": ["tutorial_flights", "secret_finance_table"],
        },
    )
    # mock config shouldn't throw 403 because secret_finance_table is not in sandbox at all, so it's checked against allowed_sandbox_tables
    # In _resolve_selected_tables, it checks if it's in sandbox available tables First.
    assert res.status_code == 403
    assert "无权选择表" in res.json()["detail"]
