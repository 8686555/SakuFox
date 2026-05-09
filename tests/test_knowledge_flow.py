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


def test_knowledge_assets_workbench_endpoints_cover_kb_upload_and_experience():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Refund Policy",
            "description": "Enterprise policy",
            "sync_type": "manual",
            "content": "退款规则：48小时内允许原路退款，逾期需要人工审批。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    upload_res = client.post(
        "/api/data/upload",
        headers=headers,
        data={"sandbox_id": "sb_flights_overview"},
        files={"files": ("ops_guide.txt", "退款场景下，客服需要先核验支付渠道。".encode("utf-8"), "text/plain")},
    )
    assert upload_res.status_code == 200

    _, _, proposal_id = _run_mock_iteration(headers, message="save experience asset")
    skill_res = client.post(
        "/api/skills/save",
        headers=headers,
        json={"proposal_id": proposal_id, "name": "refund-experience", "knowledge": ["退款需要先核验支付渠道"]},
    )
    assert skill_res.status_code == 200

    assets_res = client.get("/api/knowledge/assets", headers=headers)
    assert assets_res.status_code == 200
    assets = assets_res.json()["assets"]

    kb_asset = next(asset for asset in assets if asset["source_type"] == "knowledge_base" and asset["source_ref"] == kb_id)
    upload_asset = next(asset for asset in assets if asset["source_type"] == "upload" and asset["source_ref"] == "ops_guide.txt")
    experience_asset = next(asset for asset in assets if asset["source_type"] == "skill" and asset["title"] == "refund-experience")

    assert kb_asset["asset_type"] == "enterprise_kb"
    assert upload_asset["asset_type"] == "uploaded_file"
    assert experience_asset["asset_type"] == "experience"
    assert kb_asset["chunk_count"] >= 1
    assert upload_asset["chunk_count"] >= 1
    assert experience_asset["chunk_count"] >= 1

    content_res = client.get(f"/api/knowledge/assets/{kb_asset['asset_id']}/content?mode=full", headers=headers)
    assert content_res.status_code == 200
    assert "48小时内允许原路退款" in content_res.json()["content"]


def test_knowledge_index_search_debug_returns_locator_and_readable_asset():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Attribution Rulebook",
            "description": "Marketing glossary",
            "sync_type": "manual",
            "content": "活动归因规则：用户首单归因到最近一次有效点击，退款订单不计入活动转化。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    search_res = client.post(
        "/api/knowledge/index/search-debug",
        headers=headers,
        json={"sandbox_id": "sb_flights_overview", "query": "活动归因规则", "top_k": 3},
    )
    assert search_res.status_code == 200
    results = search_res.json()["results"]
    assert results
    first = results[0]
    assert first["asset_id"]
    assert first["full_document_locator"].startswith("asset://")

    content_res = client.get(f"/api/knowledge/assets/{first['asset_id']}/content?mode=full", headers=headers)
    assert content_res.status_code == 200
    assert "退款订单不计入活动转化" in content_res.json()["content"]


def test_text_document_upload_creates_source_chunks_review_and_semantic_page():
    headers = _login_admin()

    upload_res = client.post(
        "/api/data/upload",
        headers=headers,
        data={"sandbox_id": "sb_flights_overview"},
        files={
            "files": (
                "semantic_terms.md",
                "# GMV 指标口径\nGMV 指标 = 支付成功订单金额总和，退款订单需要排除。".encode("utf-8"),
                "text/markdown",
            )
        },
    )
    assert upload_res.status_code == 200
    uploaded = upload_res.json()["uploaded_files"][0]
    assert uploaded["document"]["parse_status"] == "success"
    document_id = uploaded["document"]["document_id"]

    doc_res = client.get(f"/api/knowledge/documents/{document_id}", headers=headers)
    assert doc_res.status_code == 200
    assert doc_res.json()["chunks"]

    doc_search_res = client.post(
        "/api/knowledge/documents/search",
        headers=headers,
        json={"sandbox_id": "sb_flights_overview", "query": "GMV退款订单", "top_k": 3},
    )
    assert doc_search_res.status_code == 200
    assert doc_search_res.json()["results"]

    review_res = client.get(
        "/api/knowledge/wiki/review-items?sandbox_id=sb_flights_overview&status=pending",
        headers=headers,
    )
    assert review_res.status_code == 200
    review_item = next(item for item in review_res.json()["review_items"] if item["source_document_id"] == document_id)
    assert review_item["proposed_payload"]["page_type"] == "metric"

    publish_res = client.post(
        f"/api/knowledge/wiki/review-items/{review_item['review_id']}/resolve",
        headers=headers,
        json={"action": "publish"},
    )
    assert publish_res.status_code == 200
    page = publish_res.json()["page"]
    assert page["status"] == "published"
    assert page["source_document_id"] == document_id

    semantic_res = client.post(
        "/api/knowledge/semantic/search",
        headers=headers,
        json={"sandbox_id": "sb_flights_overview", "query": "GMV 指标口径", "top_k": 3},
    )
    assert semantic_res.status_code == 200
    assert any(item["page_id"] == page["page_id"] for item in semantic_res.json()["results"])


def test_pdf_document_parse_failure_is_recorded_without_breaking_upload():
    headers = _login_admin()

    upload_res = client.post(
        "/api/data/upload",
        headers=headers,
        data={"sandbox_id": "sb_flights_overview"},
        files={"files": ("broken_policy.pdf", b"%PDF-1.4\nnot a valid pdf body", "application/pdf")},
    )
    assert upload_res.status_code == 200
    document = upload_res.json()["uploaded_files"][0]["document"]
    document_id = document["document_id"]

    doc_res = client.get(f"/api/knowledge/documents/{document_id}", headers=headers)
    assert doc_res.status_code == 200
    assert doc_res.json()["parse_status"] in {"pending", "running", "failed"}
    if doc_res.json()["parse_status"] == "failed":
        assert doc_res.json()["parse_error"]


def test_pending_experience_can_be_published_from_proposal():
    headers = _login_admin()
    _, _, proposal_id = _run_mock_iteration(headers, message="summarize refund handling")

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    pending_items = pending_res.json()["pending_experiences"]
    pending_item = next(item for item in pending_items if item["proposal_id"] == proposal_id)
    assert pending_item["sandbox_id"] == "sb_flights_overview"

    publish_res = client.post(
        "/api/knowledge/experiences/publish-from-proposal",
        headers=headers,
        json={
            "proposal_id": proposal_id,
            "name": "refund-playbook",
            "description": "Refund handling checklist",
            "knowledge": ["退款前先校验支付渠道", "高风险场景升级主管审批"],
            "mount_sandbox_ids": ["sb_flights_overview"],
        },
    )
    assert publish_res.status_code == 200
    asset = publish_res.json()["asset"]
    assert asset["asset_type"] == "experience"
    assert any(item["sandbox_id"] == "sb_flights_overview" for item in asset["mounted_sandboxes"])

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    assert all(item["proposal_id"] != proposal_id for item in pending_res.json()["pending_experiences"])


def test_pending_experience_can_be_dismissed():
    headers = _login_admin()
    _, _, proposal_id = _run_mock_iteration(headers, message="dismiss this experience")

    dismiss_res = client.post(f"/api/knowledge/experiences/{proposal_id}/dismiss", headers=headers)
    assert dismiss_res.status_code == 200

    pending_res = client.get("/api/knowledge/experiences/pending", headers=headers)
    assert pending_res.status_code == 200
    assert all(item["proposal_id"] != proposal_id for item in pending_res.json()["pending_experiences"])


def test_python_runtime_can_query_index_and_read_full_asset():
    headers = _login_admin()

    kb_res = client.post(
        "/api/knowledge_bases",
        headers=headers,
        json={
            "name": "Service Playbook",
            "description": "Ops guide",
            "sync_type": "manual",
            "content": "服务手册：退款申请命中高风险标签时，需要升级到主管审批。",
        },
    )
    assert kb_res.status_code == 200
    kb_id = kb_res.json()["id"]

    mount_res = client.post(
        "/api/sandboxes/sb_flights_overview/knowledge_bases",
        headers=headers,
        json={"knowledge_bases": [kb_id]},
    )
    assert mount_res.status_code == 200

    sandbox_res = client.get("/api/sandboxes", headers=headers)
    sandbox = next(item for item in sandbox_res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    result = main_module._execute_analysis_steps(
        result_data={
            "steps": [
                {
                    "tool": "python",
                    "code": (
                        "hits = query_knowledge_index('高风险标签', top_k=1)\n"
                        "asset = read_knowledge_asset(hits[0]['asset_id'], mode='full')\n"
                        "final_df = pd.DataFrame([{'title': asset['title'], 'content': asset['content']}])\n"
                    ),
                }
            ]
        },
        sandbox=sandbox,
        selected_tables=[],
        selected_files=[],
        sandbox_id="sb_flights_overview",
        session_id="ss_knowledge_runtime_tools",
    )

    assert not result.get("error")
    assert result["rows"]
    assert result["rows"][0]["title"] == "Service Playbook"
    assert "升级到主管审批" in result["rows"][0]["content"]
