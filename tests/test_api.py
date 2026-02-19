import pytest
from fastapi.testclient import TestClient
from etl.api import app


def test_health_returns_200():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert "ok" in r.json() or r.json().get("status") == "ok"
