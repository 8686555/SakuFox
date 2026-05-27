from fastapi.testclient import TestClient
import pytest
import uuid

from app.main import app


client = TestClient(app)


@pytest.fixture(autouse=True)
def _enable_auth_system(monkeypatch):
    monkeypatch.setenv("ENABLE_AUTH_SYSTEM", "true")
    monkeypatch.setenv("AUTH_TYPE", "mock")


def _login_admin() -> dict[str, str]:
    res = client.post("/api/auth/login", json={"provider": "ldap", "username": "admin"})
    assert res.status_code == 200
    token = res.json()["token"]
    return {"Authorization": f"Bearer {token}"}


def test_sql_toolbox_execute_and_save_virtual_view():
    headers = _login_admin()
    view_name = f"flight_department_cnt_{uuid.uuid4().hex[:8]}"

    execute_res = client.post(
        "/api/sql-toolbox/execute",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "sql": "SELECT department, COUNT(*) AS cnt FROM tutorial_flights GROUP BY department ORDER BY cnt DESC LIMIT 3",
        },
    )
    assert execute_res.status_code == 200
    run = execute_res.json()["run"]
    assert run["status"] == "success"
    assert run["row_count"] > 0
    assert run["columns"][0]["name"] == "department"

    runs_res = client.get("/api/sql-toolbox/runs?sandbox_id=sb_flights_overview", headers=headers)
    assert runs_res.status_code == 200
    assert runs_res.json()["runs"]

    save_res = client.post(
        "/api/sandboxes/sb_flights_overview/virtual-views",
        headers=headers,
        json={
            "source_run_id": run["run_id"],
            "name": view_name,
            "description": "按部门汇总差旅次数",
            "field_descriptions": {"department": "部门名称", "cnt": "差旅次数"},
        },
    )
    assert save_res.status_code == 200
    virtual_view = save_res.json()["virtual_view"]
    assert virtual_view["name"] == view_name
    assert virtual_view["source_run_id"] == run["run_id"]
    assert any(col.get("description") for col in virtual_view["columns"])

    sandboxes_res = client.get("/api/sandboxes", headers=headers)
    assert sandboxes_res.status_code == 200
    sandbox = next(item for item in sandboxes_res.json()["sandboxes"] if item["sandbox_id"] == "sb_flights_overview")
    assert any(vv["name"] == view_name for vv in sandbox.get("virtual_views", []))


def test_sql_toolbox_rejects_multi_statement_sql():
    headers = _login_admin()
    res = client.post(
        "/api/sql-toolbox/execute",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "sql": "SELECT 1; SELECT 2",
        },
    )
    assert res.status_code == 400
    assert "单条" in res.json()["detail"] or "single" in res.json()["detail"].lower()


def test_sql_toolbox_allows_cte_alias_without_treating_it_as_physical_table():
    headers = _login_admin()
    res = client.post(
        "/api/sql-toolbox/execute",
        headers=headers,
        json={
            "sandbox_id": "sb_flights_overview",
            "sql": (
                "WITH stats AS ("
                "SELECT AVG(cost) AS mean_cost, AVG(distance) AS mean_distance, "
                "AVG(cost*cost) AS mean_cost2, AVG(distance*distance) AS mean_distance2 "
                "FROM tutorial_flights"
                ") "
                "SELECT 'cost' AS metric, "
                "ROUND(SQRT(CASE WHEN mean_cost2 - mean_cost*mean_cost < 0 THEN 0 ELSE mean_cost2 - mean_cost*mean_cost END), 2) AS stddev_est, "
                "ROUND(mean_cost, 2) AS mean_val "
                "FROM stats "
                "UNION ALL "
                "SELECT 'distance' AS metric, "
                "ROUND(SQRT(CASE WHEN mean_distance2 - mean_distance*mean_distance < 0 THEN 0 ELSE mean_distance2 - mean_distance*mean_distance END), 2) AS stddev_est, "
                "ROUND(mean_distance, 2) AS mean_val "
                "FROM stats"
            ),
        },
    )
    assert res.status_code == 200
    run = res.json()["run"]
    assert run["status"] == "success"
    assert run["row_count"] == 2
