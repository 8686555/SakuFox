import re


_VISIBLE_NEWLINE_CODE_BOUNDARY = re.compile(
    r"\\n\s*(?:"
    r"[A-Za-z_][A-Za-z0-9_\.]*\s*="
    r"|[A-Za-z_][A-Za-z0-9_\.]*\s*,"
    r"|[A-Za-z_][A-Za-z0-9_\.]*\s*\["
    r"|[A-Za-z_][A-Za-z0-9_\.]*\s*\("
    r"|#"
    r"|for\s+"
    r"|if\s+"
    r"|elif\s+"
    r"|else:"
    r"|while\s+"
    r"|with\s+"
    r"|try:"
    r"|except\b"
    r"|finally:"
    r"|def\s+"
    r"|class\s+"
    r"|return\b"
    r"|from\s+"
    r"|import\s+"
    r")"
)


def normalize_llm_step_code(code: object) -> str:
    """Restore double-escaped line breaks when the LLM returned code as one line."""
    text = str(code or "").strip()
    if "\\n" not in text or "\n" in text or "\r" in text:
        return text
    if not _VISIBLE_NEWLINE_CODE_BOUNDARY.search(text):
        return text
    return (
        text.replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\r", "\n")
        .replace("\\t", "\t")
        .strip()
    )
