import uuid

from fastapi.testclient import TestClient

import app.skills as skills_module
from app.main import app
from app.store import User


def test_default_auth_enabled_requires_login(monkeypatch):
    monkeypatch.delenv("ENABLE_AUTH_SYSTEM", raising=False)
    monkeypatch.delenv("AUTH_TYPE", raising=False)
    client = TestClient(app)

    me = client.get("/api/me")
    assert me.status_code == 401

    providers = client.get("/api/auth/providers")
    assert providers.status_code == 200
    payload = providers.json()
    assert payload["enabled"] is True
    assert payload["auth_type"] == "local"
    assert payload["local"] is True
    assert payload["registration"] is True


def test_auth_disabled_returns_anonymous_user_without_login(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "false")
    client = TestClient(app)

    me = client.get("/api/me")
    assert me.status_code == 200
    payload = me.json()
    assert payload["features"]["auth_system"] is False
    assert payload["user"]["username"] == "anonymous"
    assert "Admin" in payload["user"]["roles"]

    providers = client.get("/api/auth/providers")
    assert providers.status_code == 200
    assert providers.json()["enabled"] is False

    login = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert login.status_code == 404


def test_local_registration_login_and_admin_access(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "local")
    username = f"local_{uuid.uuid4().hex[:10]}"
    client = TestClient(app)

    registered = client.post(
        "/api/auth/register",
        json={"username": username, "password": "secret123", "display_name": "Local User"},
    )
    assert registered.status_code == 200
    payload = registered.json()
    assert payload["token"].startswith("tk_")
    assert payload["user"]["username"] == username
    assert "Admin" in payload["user"]["roles"]

    me = client.get("/api/me")
    assert me.status_code == 200
    assert me.json()["user"]["username"] == username

    admin_only = client.get("/api/db-connections")
    assert admin_only.status_code == 200

    duplicate = client.post("/api/auth/register", json={"username": username, "password": "secret123"})
    assert duplicate.status_code == 409

    wrong_password = TestClient(app).post(
        "/api/auth/login",
        json={"provider": "local", "username": username, "password": "wrong-password"},
    )
    assert wrong_password.status_code == 401

    login = TestClient(app).post(
        "/api/auth/login",
        json={"provider": "local", "username": username, "password": "secret123"},
    )
    assert login.status_code == 200


def test_skill_visibility_private_owner_and_shared_for_others(monkeypatch):
    owner = User(
        user_id="u_owner",
        username="owner",
        display_name="Owner",
        groups=["admin"],
        provider="local",
        roles=["Admin"],
    )
    other = User(
        user_id="u_other",
        username="other",
        display_name="Other",
        groups=["admin"],
        provider="local",
        roles=["Admin"],
    )

    class FakeStore:
        skills = {
            "sk_private": {
                "owner_id": "u_owner",
                "name": "Private skill",
                "layers": {"visibility": "private", "groups": ["admin"]},
                "created_at": "2026-01-01T00:00:00+00:00",
            },
            "sk_shared": {
                "owner_id": "u_owner",
                "name": "Shared skill",
                "layers": {"visibility": "shared", "shared": True},
                "created_at": "2026-01-02T00:00:00+00:00",
            },
        }

    monkeypatch.setattr(skills_module, "store", FakeStore())

    owner_ids = {item["skill_id"] for item in skills_module.list_skills(owner)}
    other_ids = {item["skill_id"] for item in skills_module.list_skills(other)}

    assert owner_ids == {"sk_private", "sk_shared"}
    assert other_ids == {"sk_shared"}


def test_mock_login_sets_cookie_and_returns_roles(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "mock")
    client = TestClient(app)

    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    payload = res.json()
    assert payload["token"].startswith("tk_")
    assert "Admin" in payload["user"]["roles"]

    me = client.get("/api/me")
    assert me.status_code == 200
    assert me.json()["user"]["username"] == "admin"
    assert "Admin" in me.json()["user"]["roles"]


def test_logout_revokes_cookie_session(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "mock")
    client = TestClient(app)
    login = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert login.status_code == 200

    logout = client.post("/api/auth/logout")
    assert logout.status_code == 200

    me = client.get("/api/me")
    assert me.status_code == 401


def test_legacy_non_admin_cannot_manage_db_connections(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "mock")
    client = TestClient(app)
    res = client.post("/api/auth/login", json={"provider": "oauth", "oauth_token": "oauth_marketing_bob"})
    assert res.status_code == 200
    token = res.json()["token"]

    forbidden = client.get("/api/db-connections", headers={"Authorization": f"Bearer {token}"})
    assert forbidden.status_code == 403


def test_real_ldap_mode_requires_configuration(monkeypatch):
    client = TestClient(app)
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "ldap")
    monkeypatch.delenv("LDAP_SERVER_URI", raising=False)
    monkeypatch.delenv("LDAP_SEARCH_BASE", raising=False)

    res = client.post(
        "/api/auth/login",
        json={"provider": "ldap", "username": "alice", "password": "secret"},
    )
    assert res.status_code == 500
    assert "LDAP is not configured" in res.json()["detail"]
