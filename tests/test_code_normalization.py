from app.code_normalization import normalize_llm_step_code


def test_visible_newline_between_python_statements_is_restored():
    code = "summary = df0.copy()\\nyear_df = df1.copy()\\nchart_specs.append({'title': 'x'})"

    normalized = normalize_llm_step_code(code)

    assert normalized == "summary = df0.copy()\nyear_df = df1.copy()\nchart_specs.append({'title': 'x'})"


def test_visible_newline_inside_string_literal_is_left_alone():
    code = "final_df = pd.DataFrame([{'text': 'line1\\nline2'}])"

    assert normalize_llm_step_code(code) == code
