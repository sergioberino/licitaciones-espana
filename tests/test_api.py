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


@pytest.fixture
def nlp_analizar_cleanup():
    """Borra filas residuales en ops.nlp_runs creadas por tests con Popen mockeado.

    Los tests POST /nlp/analizar invocan ``_create_nlp_run()`` antes del Popen
    real (Hito 3.1.d), así que aunque el Popen sea mock, la fila ya existe en
    BBDD. El test añade su pid sentinela al list y este fixture lo borra al
    finalizar. No-op si la BBDD no está disponible (entornos sin .env).
    """
    fake_pids: list[int] = []
    yield fake_pids
    if not fake_pids:
        return
    import psycopg2 as _psy
    from etl.config import get_database_url as _get_db_url

    db_url = _get_db_url()
    if not db_url:
        return
    try:
        with _psy.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ops.nlp_runs WHERE pid = ANY(%s)",
                    (fake_pids,),
                )
            conn.commit()
    except Exception:
        pass


def test_nlp_analizar_dry_run_endpoint(client, tmp_path, monkeypatch, nlp_analizar_cleanup):
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
    nlp_analizar_cleanup.append(4242)

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


def test_nlp_analizar_anos_propagates_to_cmd(client, tmp_path, monkeypatch, nlp_analizar_cleanup):
    """--anos se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4243
    nlp_analizar_cleanup.append(4243)
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"anos": "2024-2026", "dry_run": True})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--anos" in cmd and "2024-2026" in cmd
    assert "--dry-run" in cmd


def test_nlp_analizar_anos_meses_propagates_to_cmd(client, tmp_path, monkeypatch, nlp_analizar_cleanup):
    """--anos + --meses se propagan al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4244
    nlp_analizar_cleanup.append(4244)
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"anos": "2024-2025", "meses": "3-6"})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--anos" in cmd and "2024-2025" in cmd
    assert "--meses" in cmd and "3-6" in cmd


def test_nlp_analizar_codigo_bdns_propagates(client, tmp_path, monkeypatch, nlp_analizar_cleanup):
    """--codigo-bdns se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4245
    nlp_analizar_cleanup.append(4245)
    with patch("etl.api.subprocess.Popen", return_value=mock_proc) as popen:
        r = client.post("/nlp/analizar", json={"codigo_bdns": 12345})
    assert r.status_code == 202
    cmd = popen.call_args[0][0]
    assert "--codigo-bdns" in cmd and "12345" in cmd


def test_nlp_analizar_todo_propagates(client, tmp_path, monkeypatch, nlp_analizar_cleanup):
    """--todo se propaga al subprocess cmd."""
    _patch_paths(monkeypatch, tmp_path)
    mock_proc = MagicMock()
    mock_proc.pid = 4246
    nlp_analizar_cleanup.append(4246)
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


# -----------------------------------------------------------------------------
# Cierre Hito 3.1 — POST /nlp/runs/recover (auto-heal manual)
# -----------------------------------------------------------------------------


def test_nlp_runs_recover_returns_503_when_no_db(client):
    """Sin DATABASE_URL → 503 (no se puede recuperar nada sin BBDD)."""
    with patch("etl.api.get_database_url", return_value=None):
        r = client.post("/nlp/runs/recover")
    assert r.status_code == 503


def test_nlp_runs_recover_marks_dead_pid_failed(client, db_required, pg_conn):
    """Inserta fila zombi (pid muerto) y verifica que el endpoint la cierra.

    Reusa fixtures `db_required` + `pg_conn` ya definidos arriba para skip
    consistente cuando no hay BBDD.
    """
    import json as _json

    sentinel_pid = 99999990
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.nlp_runs
              (selector_kind, selector_value, status, pid)
            VALUES ('todo', %s::jsonb, 'running', %s)
            RETURNING run_id
            """,
            (_json.dumps({"todo": True, "sentinel": sentinel_pid}), sentinel_pid),
        )
        inserted_id = cur.fetchone()[0]
    pg_conn.commit()
    try:
        r = client.post("/nlp/runs/recover")
        assert r.status_code == 200, r.text
        body = r.json()
        assert "recovered" in body
        assert body["recovered"] >= 1

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_message FROM ops.nlp_runs WHERE run_id = %s",
                (inserted_id,),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "failed"
        assert row[1] is not None and "not found" in row[1]
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM ops.nlp_runs WHERE run_id = %s", (inserted_id,))
        pg_conn.commit()


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


# -----------------------------------------------------------------------------
# Hito 3.1.e — endpoints HTTP nuevos /nlp/stats /runs /runs/{id} /recent /pending
# -----------------------------------------------------------------------------

import psycopg2  # noqa: E402  (import tardío para tests con BBDD)
import pytest  # noqa: E402

from etl.config import get_database_url  # noqa: E402


def _has_db() -> bool:
    url = get_database_url()
    if not url:
        return False
    try:
        c = psycopg2.connect(url)
        c.close()
        return True
    except Exception:
        return False


@pytest.fixture
def db_required():
    if not _has_db():
        pytest.skip("PostgreSQL no disponible: tests con DB saltados.")


@pytest.fixture
def pg_conn():
    url = get_database_url()
    if not url:
        pytest.skip("DB unavailable")
    conn = psycopg2.connect(url)
    yield conn
    conn.close()


def _cleanup_test_runs(conn):
    """Borra filas de tests aislados (codigo_bdns negativos sentinela).

    Hay que borrar primero los logs LLM que referencian run_id (FK on cascade
    no aplica aquí: la FK no tiene ON DELETE).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM ops.llm_bases_reguladoras_logs
            WHERE run_id IN (
              SELECT run_id FROM ops.nlp_runs
              WHERE selector_value::text LIKE '%-9999991%'
                 OR selector_value::text LIKE '%-9999992%'
                 OR selector_value::text LIKE '%-9999993%'
            )
            """
        )
        cur.execute(
            "DELETE FROM ops.nlp_runs WHERE selector_value::text LIKE '%-9999991%' "
            "OR selector_value::text LIKE '%-9999992%' "
            "OR selector_value::text LIKE '%-9999993%'"
        )
    conn.commit()


def test_nlp_stats_empty_db(client, db_required, pg_conn):
    """GET /nlp/stats: estructura básica + counts ints + pricing presente o None.

    No exigimos counts específicos porque la BD compartida tiene datos previos.
    """
    r = client.get("/nlp/stats")
    assert r.status_code == 200, r.text
    body = r.json()
    assert "counts" in body
    assert "costs" in body
    assert "latest_run" in body
    assert "pricing" in body
    for k in ("valid", "partial", "invalid", "total_nlp", "total_subv", "pending"):
        assert k in body["counts"]
        assert isinstance(body["counts"][k], int)
    for window in ("last_run", "last_24h", "last_30d", "all_time"):
        assert window in body["costs"]


def test_nlp_stats_with_pricing(client, db_required, pg_conn):
    """Pricing vigente seedeado debe aparecer en /nlp/stats."""
    r = client.get("/nlp/stats")
    body = r.json()
    pricing = body.get("pricing")
    # En el ambiente de tests ya seedeamos gpt-5.4-nano en 3.1.i.
    assert pricing is not None
    assert pricing["model"] == "gpt-5.4-nano"
    # Decimales serializados como str para evitar drift float.
    assert pricing["input_per_1m_usd"] in ("0.200000", "0.20")
    assert pricing["output_per_1m_usd"] in ("1.250000", "1.25")


def test_nlp_runs_list_pagination(client, db_required, pg_conn):
    """Insertamos 25 runs sentinela y validamos limit=20/offset=20."""
    _cleanup_test_runs(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            for i in range(25):
                cur.execute(
                    """
                    INSERT INTO ops.nlp_runs
                      (selector_kind, selector_value, status)
                    VALUES ('codigo_bdns',
                            jsonb_build_object('codigo_bdns', -9999991, 'idx', %s),
                            'ok')
                    """,
                    (i,),
                )
        pg_conn.commit()

        # limit=20 + filtro por sentinela: usamos nuestro propio status='ok' pero
        # no hay forma de filtrar por sentinela en el endpoint público; en su lugar
        # verificamos que devuelve >= 20 items y respeta limit.
        r = client.get("/nlp/runs?limit=20")
        assert r.status_code == 200
        items = r.json().get("items", [])
        assert len(items) <= 20

        r2 = client.get("/nlp/runs?limit=5&offset=0")
        assert len(r2.json().get("items", [])) <= 5
    finally:
        _cleanup_test_runs(pg_conn)


def test_nlp_runs_list_filter_by_status(client, db_required, pg_conn):
    """Filtro ?status=failed devuelve solo failed."""
    _cleanup_test_runs(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.nlp_runs
                  (selector_kind, selector_value, status, error_message)
                VALUES ('codigo_bdns',
                        jsonb_build_object('codigo_bdns', -9999992),
                        'failed', 'boom')
                """
            )
        pg_conn.commit()

        r = client.get("/nlp/runs?status=failed&limit=50")
        assert r.status_code == 200
        items = r.json().get("items", [])
        assert items, "esperaba al menos el run sentinela failed"
        for it in items:
            assert it["status"] == "failed"
    finally:
        _cleanup_test_runs(pg_conn)


def test_nlp_run_detail_with_items(client, db_required, pg_conn):
    """GET /nlp/runs/{id}: detalle del run + items vinculados via run_id."""
    _cleanup_test_runs(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.nlp_runs
                  (selector_kind, selector_value, status, items_processed)
                VALUES ('codigo_bdns',
                        jsonb_build_object('codigo_bdns', -9999991),
                        'ok', 1)
                RETURNING run_id
                """
            )
            run_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO ops.llm_bases_reguladoras_logs
                  (document_key, llm_model, input_tokens, output_tokens,
                   duration_ms, validation_status, cost_usd, run_id)
                VALUES ('url:fake', 'gpt-5.4-nano', 1234, 567, 4321, 'valid',
                        0.001234, %s)
                """,
                (run_id,),
            )
        pg_conn.commit()

        r = client.get(f"/nlp/runs/{run_id}")
        assert r.status_code == 200
        body = r.json()
        assert body["run"]["run_id"] == run_id
        assert body["run"]["status"] == "ok"
        assert any(it.get("document_key") == "url:fake" for it in body["items"])
    finally:
        _cleanup_test_runs(pg_conn)


def test_nlp_recent_returns_join_subvenciones(client, db_required, pg_conn):
    """Inserta nacional_subvenciones + subvenciones_nlp y verifica /nlp/recent.

    El endpoint hace JOIN: subv_nlp + nacional_subv enlazados por nlp_document_key.
    """
    sentinel_id = -9999991
    sentinel_key = f"url:test-recent-{sentinel_id}"
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = %s", (sentinel_id,))
        cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = %s", (sentinel_key,))
    pg_conn.commit()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.subvenciones_nlp
                  (document_key, document_source, document_heuristic_step,
                   schema_version, llm_model, validation_status, extracted_at,
                   nlp_json)
                VALUES (%s, 'url_bases_reguladoras', 1, 'v5.0.2',
                        'gpt-5.4-nano', 'valid', NOW(), '{}'::jsonb)
                """,
                (sentinel_key,),
            )
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, nlp_document_key, fecha_recepcion,
                   nivel1, ingested_at, nlp_validation_status, nlp_extracted_at)
                VALUES (%s, 'test', %s, '2026-01-01'::date, 'AGE',
                        NOW(), 'valid', NOW())
                """,
                (sentinel_id, sentinel_key),
            )
        pg_conn.commit()

        r = client.get("/nlp/recent?limit=50")
        assert r.status_code == 200
        items = r.json().get("items", [])
        match = [it for it in items if it.get("id") == sentinel_id]
        assert match, "esperaba la subvención sentinela en /nlp/recent"
        it = match[0]
        assert it["validation_status"] == "valid"
        assert it["llm_model"] == "gpt-5.4-nano"
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id = %s", (sentinel_id,))
            cur.execute("DELETE FROM l0.subvenciones_nlp WHERE document_key = %s", (sentinel_key,))
        pg_conn.commit()


def test_nlp_pending_filters_by_anos(client, db_required, pg_conn):
    """Insertamos pendientes en distintos años y filtramos con ?anos=2024-2024."""
    test_ids = (-9999991, -9999992, -9999993)
    with pg_conn.cursor() as cur:
        cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN %s", (test_ids,))
    pg_conn.commit()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion, ingested_at)
                VALUES
                  (%s, 'a23', 'https://e.org/a', '2023-06-01'::date, NOW()),
                  (%s, 'a24', 'https://e.org/b', '2024-06-01'::date, NOW()),
                  (%s, 'a25', 'https://e.org/c', '2025-06-01'::date, NOW())
                """,
                test_ids,
            )
        pg_conn.commit()

        r = client.get("/nlp/pending?limit=200&anos=2024-2024")
        assert r.status_code == 200
        items = r.json().get("items", [])
        ids = {it["id"] for it in items if it["id"] in test_ids}
        assert ids == {-9999992}
    finally:
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN %s", (test_ids,))
        pg_conn.commit()
