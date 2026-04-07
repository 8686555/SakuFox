import json

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


def test_report_bundle_accepts_standalone_ai_html_and_injects_missing_chart_placeholders(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        yield _bundle_response("<!doctype html><html><body><h1>报告</h1></body></html>")

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    assert call_state["count"] == 1
    assert "<html" in str(bundle["html_document"]).lower()
    assert "data-chart-id=\"chart_1\"" in str(bundle["html_document"])


def test_report_bundle_wraps_html_fragment_without_retry(monkeypatch):
    monkeypatch.setattr(
        agent_module,
        "generate_auto_analysis_report",
        lambda **kwargs: "## 执行摘要\n- done",
    )

    call_state = {"count": 0}

    def fake_openai(system_prompt, user_prompt, model, config):
        call_state["count"] += 1
        yield _bundle_response("<div><h1>报告片段</h1></div>")

    monkeypatch.setattr(agent_module, "_call_openai_protocol", fake_openai)

    bundle = _run_bundle_generation()
    assert call_state["count"] == 1
    html_text = str(bundle["html_document"]).lower()
    assert "<html" in html_text
    assert "data-chart-id=\"chart_1\"" in html_text


def test_build_polished_report_sections_skips_placeholder_only_sections():
    markdown = (
        "# 票价影响因素分析报告\n"
        "-\n\n"
        "## Executive Summary\n"
        "核心发现：票价波动主要来自维度内差异。\n\n"
        "## Key Findings\n"
        "-\n"
    )
    _, summary, rendered = agent_module._build_polished_report_sections(markdown, "自动分析报告")
    assert rendered.count('class="report-section"') == 1
    assert "Executive Summary" in rendered
    assert "Key Findings" not in rendered
    assert not summary.endswith("-")
