from datetime import datetime, timezone

from app.authorization import filter_tables_by_user
from app.store import User, store


def save_skill_from_proposal(user: User, proposal_id: str, name: str) -> dict:
    proposal = store.proposals.get(proposal_id)
    if not proposal:
        raise ValueError("提案不存在")
    if proposal["user_id"] != user.user_id:
        raise PermissionError("仅可保存自己的提案")
    if proposal["status"] != "executed":
        raise ValueError("仅可保存已执行提案")
    inherited_tables = filter_tables_by_user(user, proposal["tables"])
    payload = {
        "name": name,
        "owner_id": user.user_id,
        "owner_name": user.display_name,
        "groups": user.groups,
        "sql_template": proposal["sql"],
        "session_patches": proposal["session_patches"],
        "inherited_tables": inherited_tables,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    skill_id = store.create_skill(payload)
    return {"skill_id": skill_id, **payload}


def list_skills(user: User) -> list[dict]:
    output = []
    for skill_id, item in store.skills.items():
        if item["owner_id"] == user.user_id or set(item["groups"]).intersection(user.groups):
            output.append({"skill_id": skill_id, **item})
    output.sort(key=lambda x: x["created_at"], reverse=True)
    return output
