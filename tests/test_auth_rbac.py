from fastapi.testclient import TestClient

from app.main import app


def test_mock_login_sets_cookie_and_returns_roles():
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


def test_logout_revokes_cookie_session():
    client = TestClient(app)
    login = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert login.status_code == 200

    logout = client.post("/api/auth/logout")
    assert logout.status_code == 200

    me = client.get("/api/me")
    assert me.status_code == 401


def test_non_admin_cannot_manage_db_connections():
    client = TestClient(app)
    res = client.post("/api/auth/login", json={"provider": "oauth", "oauth_token": "oauth_marketing_bob"})
    assert res.status_code == 200
    token = res.json()["token"]

    forbidden = client.get("/api/db-connections", headers={"Authorization": f"Bearer {token}"})
    assert forbidden.status_code == 403


def test_real_ldap_mode_requires_configuration(monkeypatch):
    client = TestClient(app)
    monkeypatch.setenv("AUTH_TYPE", "ldap")
    monkeypatch.delenv("LDAP_SERVER_URI", raising=False)
    monkeypatch.delenv("LDAP_SEARCH_BASE", raising=False)

    res = client.post(
        "/api/auth/login",
        json={"provider": "ldap", "username": "alice", "password": "secret"},
    )
    assert res.status_code == 500
    assert "LDAP is not configured" in res.json()["detail"]
