import re

from app.i18n import t

try:
    from sqlglot import exp, parse  # type: ignore
    from sqlglot.errors import ParseError  # type: ignore
    _HAS_SQLGLOT = True
except Exception:  # pragma: no cover - optional dependency fallback
    exp = None  # type: ignore
    parse = None  # type: ignore
    ParseError = Exception  # type: ignore
    _HAS_SQLGLOT = False


TEMPLATE_PATTERN = re.compile(r"(\{\{|\}\}|\{%|%\}|\$\{)")
SELECT_PREFIX_PATTERN = re.compile(r"^\s*(with\b[\s\S]+?select\b|select\b)", re.I)
FORBIDDEN_PATTERN = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|merge|copy|use|commit|rollback)\b", re.I)
TABLE_PATTERN = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)", re.I)

_DISALLOWED_NODE_NAMES = [
    "Insert",
    "Update",
    "Delete",
    "Drop",
    "Alter",
    "Create",
    "Truncate",
    "Grant",
    "Revoke",
    "Merge",
    "Command",
    "Copy",
    "Use",
    "Transaction",
    "Commit",
    "Rollback",
]
DISALLOWED_NODES = tuple(getattr(exp, name) for name in _DISALLOWED_NODE_NAMES if _HAS_SQLGLOT and hasattr(exp, name))


def _normalize_identifier(name: str) -> str:
    text = str(name or "").strip().strip('"').strip("`").strip("[").strip("]")
    return text.lower()


def _extract_cte_names_fallback(sql: str) -> set[str]:
    text = str(sql or "")
    if not re.match(r"^\s*with\b", text, re.I):
        return set()

    length = len(text)
    index = 0
    cte_names: set[str] = set()

    with_match = re.match(r"^\s*with\b", text, re.I)
    if not with_match:
        return cte_names
    index = with_match.end()

    recursive_match = re.match(r"\s*recursive\b", text[index:], re.I)
    if recursive_match:
        index += recursive_match.end()

    while index < length:
        while index < length and text[index].isspace():
            index += 1

        name_match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)", text[index:])
        if not name_match:
            break
        alias = _normalize_identifier(name_match.group(1))
        index += name_match.end()

        while index < length and text[index].isspace():
            index += 1

        as_match = re.match(r"as\b", text[index:], re.I)
        if not as_match:
            break
        index += as_match.end()

        while index < length and text[index].isspace():
            index += 1

        if index >= length or text[index] != "(":
            break

        cte_names.add(alias)
        depth = 0
        while index < length:
            char = text[index]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    index += 1
                    break
            index += 1

        while index < length and text[index].isspace():
            index += 1

        if index < length and text[index] == ",":
            index += 1
            continue
        break

    return cte_names


def _table_identifier(table) -> str:
    if not _HAS_SQLGLOT:
        return ""
    parts: list[str] = []
    catalog = str(table.catalog or "").strip()
    schema = str(table.db or "").strip()
    table_name = str(table.name or "").strip()
    if catalog:
        parts.append(catalog)
    if schema:
        parts.append(schema)
    if table_name:
        parts.append(table_name)
    return ".".join(parts)


def _parse_single_statement(sql: str):
    raw = str(sql or "")
    if not raw.strip():
        raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))
    if TEMPLATE_PATTERN.search(raw):
        raise ValueError(t("error_sql_template_not_allowed", default="SQL 不支持模板/Jinja"))

    if _HAS_SQLGLOT:
        try:
            statements = [stmt for stmt in parse(raw) if stmt is not None]
        except ParseError as exc:
            raise ValueError(t("error_sql_parse_failed", detail=str(exc), default=f"SQL parse failed: {str(exc)}"))
        if len(statements) != 1:
            raise ValueError(t("error_sql_multi_statement", default="仅允许单条 SQL 查询"))
        return statements[0]

    if ";" in raw.strip().rstrip(";"):
        raise ValueError(t("error_sql_multi_statement", default="仅允许单条 SQL 查询"))
    if not SELECT_PREFIX_PATTERN.search(raw):
        raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))
    if FORBIDDEN_PATTERN.search(raw):
        raise ValueError(t("error_danger_op", default="SQL 包含危险操作"))
    return raw


def _assert_read_only_select(statement) -> None:
    if not _HAS_SQLGLOT:
        if not SELECT_PREFIX_PATTERN.search(str(statement or "")):
            raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))
        if FORBIDDEN_PATTERN.search(str(statement or "")):
            raise ValueError(t("error_danger_op", default="SQL 包含危险操作"))
        return

    if DISALLOWED_NODES and any(statement.find(node_type) is not None for node_type in DISALLOWED_NODES):
        raise ValueError(t("error_danger_op", default="SQL 包含危险操作"))
    if not isinstance(statement, exp.Query):
        raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))
    if statement.find(exp.Select) is None:
        raise ValueError(t("error_select_only", default="仅允许 SELECT 查询"))


def extract_tables(sql: str) -> list[str]:
    statement = _parse_single_statement(sql)
    if not _HAS_SQLGLOT:
        cte_names = _extract_cte_names_fallback(str(statement))
        tables: list[str] = []
        seen: set[str] = set()
        for match in TABLE_PATTERN.finditer(str(statement)):
            identifier = _normalize_identifier(match.group(1))
            if identifier in cte_names or identifier.split(".")[-1] in cte_names:
                continue
            if identifier and identifier not in seen:
                seen.add(identifier)
                tables.append(identifier)
        return tables

    cte_names: set[str] = set()
    for cte in statement.find_all(exp.CTE):
        alias = str(cte.alias_or_name or "").strip()
        if alias:
            cte_names.add(_normalize_identifier(alias))

    tables: list[str] = []
    seen: set[str] = set()
    for table in statement.find_all(exp.Table):
        identifier = _table_identifier(table)
        normalized_identifier = _normalize_identifier(identifier)
        normalized_name = _normalize_identifier(str(table.name or ""))
        if not identifier:
            continue
        if normalized_identifier in cte_names or normalized_name in cte_names:
            continue
        if normalized_identifier in seen:
            continue
        seen.add(normalized_identifier)
        tables.append(identifier)
    return tables


def enforce_select_only(sql: str) -> None:
    statement = _parse_single_statement(sql)
    _assert_read_only_select(statement)


def enforce_table_whitelist(sql: str, allowed_tables: list[str]) -> list[str]:
    statement = _parse_single_statement(sql)
    _assert_read_only_select(statement)
    tables = extract_tables(sql)
    allowed = {_normalize_identifier(item) for item in (allowed_tables or []) if str(item or "").strip()}
    allowed_with_last = set(allowed)
    for item in list(allowed):
        if "." in item:
            allowed_with_last.add(item.split(".")[-1])

    denied: list[str] = []
    for table_name in tables:
        normalized = _normalize_identifier(table_name)
        last = normalized.split(".")[-1]
        if normalized in allowed_with_last or last in allowed_with_last:
            continue
        denied.append(table_name)

    if denied:
        raise PermissionError(t("error_denied_tables", tables=",".join(denied), default=f"越权访问表: {','.join(denied)}"))
    return tables


def apply_mask_to_rows(rows: list[dict], sensitive_fields: dict[str, list[str]]) -> list[dict]:
    sensitive_cols = set()
    for fields in sensitive_fields.values():
        sensitive_cols.update(fields)
    masked_rows = []
    for row in rows:
        out = dict(row)
        for col in row.keys():
            if col in sensitive_cols and row[col] is not None:
                value = str(row[col])
                if len(value) <= 4:
                    out[col] = "****"
                else:
                    out[col] = value[:2] + "*" * (len(value) - 4) + value[-2:]
        masked_rows.append(out)
    return masked_rows
