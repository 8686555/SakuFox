from app.store import User, store


def get_accessible_tables(user: User) -> list[str]:
    visible = []
    for table_name, conf in store.tables.items():
        if set(user.groups).intersection(conf["allowed_groups"]):
            visible.append(table_name)
    return sorted(visible)


def get_accessible_sandboxes(user: User) -> list[dict]:
    items = []
    for sandbox_id, sandbox in store.sandboxes.items():
        if set(user.groups).intersection(sandbox["allowed_groups"]):
            items.append({"sandbox_id": sandbox_id, **sandbox})
    return items


def assert_sandbox_access(user: User, sandbox_id: str) -> dict:
    sandbox = store.sandboxes.get(sandbox_id)
    if not sandbox:
        raise ValueError("沙盒不存在")
    if not set(user.groups).intersection(sandbox["allowed_groups"]):
        raise PermissionError("无权访问该沙盒")
    return sandbox


def filter_tables_by_user(user: User, tables: list[str]) -> list[str]:
    allowed = set(get_accessible_tables(user))
    return [t for t in tables if t in allowed]


def get_sensitive_fields(table_names: list[str]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    for t in table_names:
        conf = store.tables.get(t)
        if conf:
            output[t] = conf["sensitive_fields"]
    return output
