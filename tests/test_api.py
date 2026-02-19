import pytest
from fastapi.testclient import TestClient
from etl.api import app


def test_health_returns_200():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert "ok" in r.json() or r.json().get("status") == "ok"


def test_status_returns_200_or_503_and_has_db_key():
    client = TestClient(app)
    r = client.get("/status")
    assert r.status_code in (200, 503)  # 503 if DB unavailable
    data = r.json()
    assert "database" in data or "db" in data or "status" in data
