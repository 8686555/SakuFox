import json

import pytest

import app.agent as agent_module


def _bundle_response(html_document: str) -> str:
    return json.dumps(
        {
            "title": "报告标题",
            "summary": "报告摘要",
            "html_document": html_document,
            "chart_bindings": [
                {
                    "chart_id": "chart_1",
                    "option": {"xAxis": {}, "yAxis": {}, "series": []},
                    "height": 320,
                }
            ],
        },
        ensure_ascii=False,
    )


def _run_bundle_generation():
    return agent_module.generate_auto_analysis_report_bundle(
        message="测试自动分析",
        session_history=[],
        business_knowledge=[],
        session_patches=[],
        loop_rounds=[],
        chart_specs=[{"xAxis": {}, "yAxis": {}, "series": []}],
        final_result_rows=[],
        stop_reason="model_stopped_using_tools",
        rounds_completed=1,
        provider="openai",
    )


def test_report_bundle_retries_until_ai_html_is_qualified(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    responses = [
        _bundle_response("<!doctype html><html><body><h1>报告1</h1></body></html>"),
        _bundle_response("<!doctype html><html><body><h1>报告2</h1></body></html>"),
        _bundle_response("<!doctype html><html><body><h1>报告3</h1><div data-chart-id=\"chart_1\"></div></body></html>"),
    ]
    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        idx = min(call_state["count"], len(responses) - 1)
        call_state["count"] += 1
        yield responses[idx]

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    assert call_state["count"] == 3
    assert "<html" in str(bundle["html_document"]).lower()
    assert "data-chart-id=\"chart_1\"" in str(bundle["html_document"])


def test_report_bundle_raises_after_three_invalid_ai_attempts(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    invalid_response = _bundle_response("<!doctype html><html><body><h1>报告</h1></body></html>")
    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        yield invalid_response

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        _run_bundle_generation()
    assert call_state["count"] == 3


def test_report_bundle_upgrades_html_fragment_by_ai(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}
    fragment_response = _bundle_response("<div><h1>报告片段</h1><div data-chart-id=\"chart_1\"></div></div>")

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        yield fragment_response

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)
    monkeypatch.setattr(
        agent_module,
        "_generate_html_document_by_llm",
        lambda **kwargs: "<!doctype html><html><body><h1>升级后报告</h1><div data-chart-id=\"chart_1\"></div></body></html>",
    )

    bundle = _run_bundle_generation()
    assert call_state["count"] == 1
    html_text = str(bundle["html_document"]).lower()
    assert "<html" in html_text
    assert "data-chart-id=\"chart_1\"" in html_text
