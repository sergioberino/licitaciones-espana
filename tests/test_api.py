"""Smoke tests for FastAPI endpoints."""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from etl.api import app

    return TestClient(app)


def test_nlp_analizar_dry_run_endpoint(client, tmp_path, monkeypatch):
    """Endpoint dispara subproceso dry_run, devuelve 202 con pid + log_path."""
    from etl.nlp.api import nlp_log_path, nlp_pid_path

    log_path = tmp_path / "nlp_analizar.log"
    pid_path = log_path.with_suffix(".pid")
    monkeypatch.setattr("etl.api.nlp_log_path", lambda: log_path)
    monkeypatch.setattr("etl.api.nlp_pid_path", lambda: pid_path)
    monkeypatch.setattr("etl.nlp.api.nlp_log_path", lambda: log_path)
    monkeypatch.setattr("etl.nlp.api.nlp_pid_path", lambda: pid_path)

    mock_proc = MagicMock()
    mock_proc.pid = 4242

    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"limit": 1, "dry_run": True, "todo": True})

    assert r.status_code == 202
    body = r.json()
    assert "pid" in body and isinstance(body["pid"], int)
    assert "log_path" in body
    assert "started_at" in body
    popen.assert_called_once()
    assert "--dry-run" in popen.call_args[0][0]


def test_nlp_log_endpoint_returns_tail(client, tmp_path, monkeypatch):
    from etl.nlp.api import nlp_log_path

    log_path = tmp_path / "nlp_analizar.log"
    log_path.write_text("line1\nline2\nline3\n", encoding="utf-8")
    monkeypatch.setattr("etl.api.nlp_log_path", lambda: log_path)

    r = client.get("/nlp/log?lines=10")
    assert r.status_code == 200
    body = r.json()
    assert "lines" in body and "exists" in body
    assert body["exists"] is True
    assert body["lines"] == ["line1", "line2", "line3"]


def test_nlp_current_run_endpoint_idle(client, tmp_path, monkeypatch):
    """Si no hay run en curso, running=false."""
    from etl.nlp.api import nlp_pid_path

    pid_path = tmp_path / "nlp_analizar.pid"
    monkeypatch.setattr("etl.api.nlp_pid_path", lambda: pid_path)
    monkeypatch.setattr("etl.nlp.api.nlp_pid_path", lambda: pid_path)
    pid_path.unlink(missing_ok=True)

    r = client.get("/nlp/current-run")
    assert r.status_code == 200
    assert r.json()["running"] is False


def test_nlp_analizar_validation_error(client):
    """limit fuera de rango → 422 (selector válido para pasar el model_validator).

    --limit 0 ahora es VÁLIDO (sin cap, WP2.1.1); usamos -1 para forzar el rechazo.
    """
    r = client.post("/nlp/analizar", json={"limit": -1, "todo": True})
    assert r.status_code == 422


def test_nlp_analizar_concurrent_run_returns_409(client, tmp_path, monkeypatch):
    """Si existe pid_file con PID vivo, segunda invocación → 409."""
    from etl.nlp.api import nlp_pid_path

    pid_path = tmp_path / "nlp_analizar.pid"
    monkeypatch.setattr("etl.api.nlp_pid_path", lambda: pid_path)
    monkeypatch.setattr("etl.nlp.api.nlp_pid_path", lambda: pid_path)
    pid_path.write_text(str(os.getpid()))
    try:
        r = client.post("/nlp/analizar", json={"limit": 1, "dry_run": True, "todo": True})
        assert r.status_code == 409
    finally:
        pid_path.unlink(missing_ok=True)


# -----------------------------------------------------------------------------
# WP2.1.1 — selector temporal en POST /nlp/analizar
# -----------------------------------------------------------------------------


def _patch_paths(monkeypatch, tmp_path):
    """Asegura que cada test escribe en su tmp_path para no colisionar."""
    log_path = tmp_path / "nlp_analizar.log"
    pid_path = log_path.with_suffix(".pid")
    monkeypatch.setattr("etl.api.nlp_log_path", lambda: log_path)
    monkeypatch.setattr("etl.api.nlp_pid_path", lambda: pid_path)
    monkeypatch.setattr("etl.nlp.api.nlp_log_path", lambda: log_path)
    monkeypatch.setattr("etl.nlp.api.nlp_pid_path", lambda: pid_path)
    return log_path, pid_path


def test_nlp_analizar_requires_selector(client, tmp_path, monkeypatch):
    """Body vacío sin selector → 422 (model_validator)."""
    _patch_paths(monkeypatch, tmp_path)
    r = client.post("/nlp/analizar", json={})
    assert r.status_code == 422


def test_nlp_analizar_anos_propagates_to_cmd(client, tmp_path, monkeypatch):
    """--anos se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4243
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"anos": "2024-2026", "dry_run": True})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--anos" in cmd and "2024-2026" in cmd
    assert "--dry-run" in cmd


def test_nlp_analizar_anos_meses_propagates_to_cmd(client, tmp_path, monkeypatch):
    """--anos + --meses se propagan al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4244
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"anos": "2024-2025", "meses": "3-6"})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--anos" in cmd and "2024-2025" in cmd
    assert "--meses" in cmd and "3-6" in cmd


def test_nlp_analizar_codigo_bdns_propagates(client, tmp_path, monkeypatch):
    """--codigo-bdns se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4245
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"codigo_bdns": 12345})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--codigo-bdns" in cmd and "12345" in cmd


def test_nlp_analizar_todo_propagates(client, tmp_path, monkeypatch):
    """--todo se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4246
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"todo": True, "dry_run": True})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--todo" in cmd


def test_nlp_analizar_meses_sin_anos_rejected(client, tmp_path, monkeypatch):
    """meses sin anos → 422 (model_validator Pydantic)."""
    _patch_paths(monkeypatch, tmp_path)
    r = client.post("/nlp/analizar", json={"meses": "3-6", "todo": True})
    assert r.status_code == 422


def test_nlp_analizar_mutual_exclusion_rejected(client, tmp_path, monkeypatch):
    """anos + codigo_bdns simultáneos → 422 (model_validator Pydantic)."""
    _patch_paths(monkeypatch, tmp_path)
    r = client.post("/nlp/analizar", json={"anos": "2024-2024", "codigo_bdns": 12})
    assert r.status_code == 422


def test_nlp_analizar_anos_bad_format_rejected(client, tmp_path, monkeypatch):
    """Formato inválido en anos → 422."""
    _patch_paths(monkeypatch, tmp_path)
    r = client.post("/nlp/analizar", json={"anos": "2026-2024"})
    assert r.status_code == 422


def test_nlp_analizar_meses_out_of_bounds_rejected(client, tmp_path, monkeypatch):
    """Meses fuera de [1,12] → 422."""
    _patch_paths(monkeypatch, tmp_path)
    r = client.post("/nlp/analizar", json={"anos": "2024-2024", "meses": "0-13"})
    assert r.status_code == 422


def test_subvenciones_ficha_returns_404_for_missing(client):
    from unittest.mock import MagicMock

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = None
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
    mock_conn.cursor.return_value.__exit__ = lambda s, *a: None
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = lambda s, *a: None

    with patch("etl.api.get_database_url", return_value="postgresql://x"):
        with patch("etl.api.psycopg2.connect", return_value=mock_conn):
            r = client.get("/subvenciones/-999999/ficha")
    assert r.status_code == 404


def test_subvenciones_ficha_returns_row_with_nlp_json(client):
    from unittest.mock import MagicMock

    row = {
        "id": -99001,
        "descripcion": "test",
        "nlp_document_key": "url:fictest123",
        "nlp_json": {"objeto": "test ficha"},
        "schema_version": "v5.0.2",
        "document_source": "url_bases_reguladoras",
        "document_heuristic_step": 1,
        "document_ref": "https://example.org/bases.pdf",
        "nlp_extracted_at_cache": None,
    }
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = row
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
    mock_conn.cursor.return_value.__exit__ = lambda s, *a: None
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = lambda s, *a: None

    with patch("etl.api.get_database_url", return_value="postgresql://x"):
        with patch("etl.api.psycopg2.connect", return_value=mock_conn):
            r = client.get("/subvenciones/-99001/ficha")
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == -99001
    assert body["nlp_json"] == {"objeto": "test ficha"}
    assert body["schema_version"] == "v5.0.2"
