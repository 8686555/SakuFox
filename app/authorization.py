from app.store import User, store


def _matches(value: str, expected: str) -> bool:
    return expected == "*" or value == expected


def has_permission(
    user: User,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
) -> bool:
    wanted_action = str(action or "").strip()
    wanted_type = str(resource_type or "").strip()
    wanted_id = str(resource_id or "*").strip() or "*"
    if not wanted_action or not wanted_type:
        return False
    for perm in user.permissions or []:
        perm_action = str(perm.get("action") or "").strip()
        perm_type = str(perm.get("resource_type") or "").strip()
        perm_id = str(perm.get("resource_id") or "*").strip() or "*"
        if _matches(wanted_action, perm_action) and _matches(wanted_type, perm_type) and _matches(wanted_id, perm_id):
            return True
    return False


def require_permission(
    user: User,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
) -> None:
    if not has_permission(user, action, resource_type, resource_id):
        raise PermissionError(f"missing permission: {action}:{resource_type}:{resource_id or '*'}")


def _acl_allows(user: User, allowed_groups: list[str] | None, allowed_roles: list[str] | None) -> bool:
    group_acl = set(allowed_groups or [])
    role_acl = set(allowed_roles or [])
    if not group_acl and not role_acl:
        return True
    return bool(group_acl.intersection(user.groups or []) or role_acl.intersection(user.roles or []))


def get_accessible_tables(user: User) -> list[str]:
    if not has_permission(user, "read", "table"):
        return []
    visible = []
    for table_name, conf in store.tables.items():
        if _acl_allows(user, conf.get("allowed_groups") or [], conf.get("allowed_roles") or []):
            visible.append(table_name)
    return sorted(visible)


def get_accessible_sandboxes(user: User) -> list[dict]:
    if not has_permission(user, "read", "sandbox"):
        return []
    items = []
    for sandbox_id, sandbox in store.sandboxes.items():
        if _acl_allows(user, sandbox.get("allowed_groups") or [], sandbox.get("allowed_roles") or []):
            items.append({"sandbox_id": sandbox_id, **sandbox})
    return items


def assert_sandbox_access(user: User, sandbox_id: str, action: str = "read") -> dict:
    sandbox = store.sandboxes.get(sandbox_id)
    if not sandbox:
        raise ValueError("沙盒不存在")
    require_permission(user, action, "sandbox", sandbox_id)
    if not _acl_allows(user, sandbox.get("allowed_groups") or [], sandbox.get("allowed_roles") or []):
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
