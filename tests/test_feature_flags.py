import json

from fastapi.testclient import TestClient

import app.main as main_module
import app.skills as skills_module
from app.config import load_config
from app.main import app


client = TestClient(app)


def _parse_ndjson_events(response_text: str) -> list[dict]:
    return [json.loads(line) for line in response_text.splitlines() if line.strip()]


def test_auth_disabled_allows_anonymous_core_access(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "false")
    monkeypatch.delenv("AUTH_TYPE", raising=False)
    monkeypatch.delenv("ENABLE_KNOWLEDGE_SYSTEM", raising=False)

    me = client.get("/api/me")
    assert me.status_code == 200
    payload = me.json()
    assert payload["features"] == {"auth_system": False, "knowledge_system": False}
    assert payload["user"]["username"] == "anonymous"
    assert "Admin" in payload["user"]["roles"]

    assert client.get("/api/sandboxes").status_code == 200
    assert client.get("/api/tables").status_code == 200
    assert client.get("/api/chat/sessions").status_code == 200


def test_auth_disabled_lists_legacy_skills_without_groups(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "false")

    class FakeStore:
        skills = {
            "skill_legacy_no_groups": {
                "owner_id": "u_anonymous",
                "owner_name": "Legacy",
                "name": "legacy skill",
                "description": "",
                "tags": [],
                "layers": {},
                "sql_template": "",
                "inherited_tables": [],
                "created_at": "2026-01-01T00:00:00+00:00",
            },
            "skill_other_no_groups": {
                "owner_id": "u_someone_else",
                "owner_name": "Legacy",
                "name": "other legacy skill",
                "description": "",
                "tags": [],
                "layers": {},
                "sql_template": "",
                "inherited_tables": [],
                "created_at": "2026-01-01T00:00:00+00:00",
            },
        }

    monkeypatch.setattr(skills_module, "store", FakeStore())

    res = client.get("/api/skills")
    assert res.status_code == 200
    ids = {item["skill_id"] for item in res.json()["skills"]}
    assert "skill_legacy_no_groups" in ids
    assert "skill_other_no_groups" not in ids


def test_default_knowledge_disabled_blocks_pages_and_api(monkeypatch):
    monkeypatch.delenv("ENABLE_KNOWLEDGE_SYSTEM", raising=False)

    assert client.get("/knowledge-index").status_code == 404
    assert client.get("/web/knowledge.html").status_code == 404
    assert client.get("/api/knowledge/assets").status_code == 404
    assert client.get("/api/knowledge_bases").status_code == 404
    assert client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        json={"knowledge_bases": []},
    ).status_code == 404


def test_analysis_does_not_query_knowledge_when_disabled(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "false")
    monkeypatch.delenv("ENABLE_KNOWLEDGE_SYSTEM", raising=False)

    def fail_search(*args, **kwargs):
        raise AssertionError("knowledge index should not be queried when disabled")

    monkeypatch.setattr(main_module.store, "search_knowledge_index", fail_search)
    res = client.post(
        "/api/chat/iterate",
        json={
            "sandbox_id": "sb_flights_overview",
            "message": "list all flights",
            "provider": "mock",
        },
    )
    assert res.status_code == 200
    events = _parse_ndjson_events(res.text)
    complete = next(event for event in events if event["type"] == "iteration_complete")
    assert complete["data"]["knowledge_sources"] == []


def test_iteration_prompt_knowledge_tools_follow_feature_flag(monkeypatch):
    monkeypatch.delenv("ENABLE_KNOWLEDGE_SYSTEM", raising=False)
    disabled_config = load_config()
    assert disabled_config.enable_knowledge_system is False
    assert "query_knowledge_index" not in disabled_config.iteration_system_prompt
    assert "read_knowledge_asset" not in disabled_config.iteration_system_prompt

    monkeypatch.setenv("ENABLE_KNOWLEDGE_SYSTEM", "true")
    enabled_config = load_config()
    assert enabled_config.enable_knowledge_system is True
    assert "query_knowledge_index" in enabled_config.iteration_system_prompt
    assert "read_knowledge_asset" in enabled_config.iteration_system_prompt
