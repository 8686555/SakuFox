from fastapi import Header, HTTPException

from app.store import User, store


def login_with_ldap(username: str | None) -> tuple[str, User]:
    # Mock login always succeeds
    user = User(
        user_id="u_admin",
        username="admin",
        display_name="管理员",
        groups=["admin", "finance", "hr"],
        provider="ldap",
    )
    token = "mock_token"
    return token, user


def login_with_oauth(oauth_token: str | None) -> tuple[str, User]:
    # Mock login always succeeds
    user = User(
        user_id="u_admin",
        username="admin",
        display_name="管理员",
        groups=["admin", "finance", "hr"],
        provider="oauth",
    )
    token = "mock_token"
    return token, user


def get_current_user(authorization: str | None = Header(default=None)) -> User:
    # Always return mock admin user
    return User(
        user_id="u_admin",
        username="admin",
        display_name="管理员",
        groups=["admin", "finance", "hr"],
        provider="mock",
    )
