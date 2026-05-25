import pytest


def test_run_batch_new_signature_dry_run(pg_conn):
    from etl.nlp.pipeline import BatchStats, run_batch

    stats = run_batch(pg_conn, limit=5, force=False, dry_run=True, todo=True)
    assert isinstance(stats, BatchStats)
    assert stats.processed == 0
    assert hasattr(stats, "planned")
    assert isinstance(stats.planned, list)


def test_run_batch_order_by_id_desc(pg_conn):
    """SELECT_PENDING devuelve items ordenados por id DESC."""
    from etl.nlp.pipeline import select_pending

    cur = pg_conn.cursor()
    cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-90001, -90002, -90003)")
    cur.execute(
        """
        INSERT INTO l0.nacional_subvenciones (id, descripcion, url_bases_reguladoras, ingested_at)
        VALUES (-90001, 't1', 'https://e.org/a', '2025-01-01'::timestamptz),
               (-90002, 't2', 'https://e.org/b', '2025-01-01'::timestamptz),
               (-90003, 't3', 'https://e.org/c', '2025-01-01'::timestamptz)
        """
    )
    pg_conn.commit()
    try:
        rows = select_pending(pg_conn, limit=10, todo=True)
        ids = [r[0] for r in rows]
        test_ids = [i for i in ids if i in (-90001, -90002, -90003)]
        assert test_ids == sorted(test_ids, reverse=True)
    finally:
        cur.execute("DELETE FROM l0.nacional_subvenciones WHERE id IN (-90001, -90002, -90003)")
        pg_conn.commit()


# -----------------------------------------------------------------------------
# WP2.1.1 — Filtro temporal por fecha_recepcion + selector --codigo-bdns / --todo
# -----------------------------------------------------------------------------


_TEMPORAL_TEST_IDS = (-90100, -90101, -90102, -90103, -90104)


def _cleanup_temporal(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM l0.nacional_subvenciones WHERE id IN %s",
            (_TEMPORAL_TEST_IDS,),
        )
    pg_conn.commit()


def test_select_pending_filters_by_anos(pg_conn):
    """--anos 2024-2024 devuelve solo filas con fecha_recepcion en 2024."""
    from etl.nlp.pipeline import select_pending

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion, ingested_at)
                VALUES
                  (-90100, 't23', 'https://e.org/23', '2023-06-15'::date, '2025-01-01'::timestamptz),
                  (-90101, 't24', 'https://e.org/24', '2024-06-15'::date, '2025-01-01'::timestamptz),
                  (-90102, 't25', 'https://e.org/25', '2025-06-15'::date, '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()

        rows = select_pending(pg_conn, limit=100, anos=(2024, 2024))
        ids = {r[0] for r in rows if r[0] in _TEMPORAL_TEST_IDS}
        assert ids == {-90101}
    finally:
        _cleanup_temporal(pg_conn)


def test_select_pending_filters_by_anos_y_meses_cross_product(pg_conn):
    """--anos 2024-2025 --meses 3-6: cross-product mar-jun de cada año."""
    from etl.nlp.pipeline import select_pending

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion, ingested_at)
                VALUES
                  (-90100, 'feb24', 'https://e.org/a', '2024-02-15'::date, '2025-01-01'::timestamptz),
                  (-90101, 'may24', 'https://e.org/b', '2024-05-15'::date, '2025-01-01'::timestamptz),
                  (-90102, 'apr25', 'https://e.org/c', '2025-04-15'::date, '2025-01-01'::timestamptz),
                  (-90103, 'aug25', 'https://e.org/d', '2025-08-15'::date, '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()

        rows = select_pending(pg_conn, limit=100, anos=(2024, 2025), meses=(3, 6))
        ids = {r[0] for r in rows if r[0] in _TEMPORAL_TEST_IDS}
        assert ids == {-90101, -90102}
    finally:
        _cleanup_temporal(pg_conn)


def test_select_pending_codigo_bdns_solo(pg_conn):
    """--codigo-bdns N devuelve la fila aunque ya esté procesada (ignora nlp_document_key IS NULL)."""
    from etl.nlp.pipeline import select_pending

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion,
                   nlp_document_key, ingested_at)
                VALUES
                  (-90100, 'ya', 'https://e.org/x', '2024-06-15'::date,
                   'url:already', '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()

        rows = select_pending(pg_conn, limit=100, codigo_bdns=-90100)
        ids = [r[0] for r in rows]
        assert ids == [-90100]
    finally:
        _cleanup_temporal(pg_conn)


def test_select_pending_todo_no_filter_fecha(pg_conn):
    """--todo ignora la fecha_recepcion (incluye fechas variadas y NULL)."""
    from etl.nlp.pipeline import select_pending

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion, ingested_at)
                VALUES
                  (-90100, 'a', 'https://e.org/a', '2020-01-01'::date, '2025-01-01'::timestamptz),
                  (-90101, 'b', 'https://e.org/b', '2024-06-15'::date, '2025-01-01'::timestamptz),
                  (-90102, 'c', 'https://e.org/c', NULL,                 '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()

        # --limit=0 desactiva el cap → garantiza que las filas negativas insertadas
        # aparecen incluso si en la BBDD real hay >100 pendientes con id positivo.
        rows = select_pending(pg_conn, limit=0, todo=True)
        ids = {r[0] for r in rows if r[0] in _TEMPORAL_TEST_IDS}
        assert ids == {-90100, -90101, -90102}
    finally:
        _cleanup_temporal(pg_conn)


def test_select_pending_excluye_fecha_recepcion_null_cuando_hay_anos(pg_conn):
    """Filas con fecha_recepcion IS NULL quedan EXCLUIDAS cuando hay --anos."""
    from etl.nlp.pipeline import select_pending

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, fecha_recepcion, ingested_at)
                VALUES
                  (-90100, 'sin', 'https://e.org/a', NULL,                 '2025-01-01'::timestamptz),
                  (-90101, 'con', 'https://e.org/b', '2024-06-15'::date,   '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()

        rows = select_pending(pg_conn, limit=100, anos=(2024, 2024))
        ids = {r[0] for r in rows if r[0] in _TEMPORAL_TEST_IDS}
        assert ids == {-90101}
    finally:
        _cleanup_temporal(pg_conn)


def test_select_pending_limit_zero_sin_cap(pg_conn):
    """limit=0 → devuelve todas las filas pendientes (sin LIMIT en SQL)."""
    from etl.nlp.pipeline import _build_select_pending_sql, select_pending

    sql, _params = _build_select_pending_sql(
        anos=None, meses=None, codigo_bdns=None, todo=True, limit=0
    )
    assert "LIMIT" not in sql.upper()

    _cleanup_temporal(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO l0.nacional_subvenciones
                  (id, descripcion, url_bases_reguladoras, ingested_at)
                VALUES
                  (-90100, 'a', 'https://e.org/a', '2025-01-01'::timestamptz),
                  (-90101, 'b', 'https://e.org/b', '2025-01-01'::timestamptz),
                  (-90102, 'c', 'https://e.org/c', '2025-01-01'::timestamptz)
                """
            )
        pg_conn.commit()
        rows = select_pending(pg_conn, limit=0, todo=True)
        ids = {r[0] for r in rows if r[0] in _TEMPORAL_TEST_IDS}
        assert ids == {-90100, -90101, -90102}
    finally:
        _cleanup_temporal(pg_conn)


def test_run_batch_valida_selector_args_falta(pg_conn):
    """run_batch sin anos/codigo_bdns/todo → ValueError."""
    from etl.nlp.pipeline import run_batch

    with pytest.raises(ValueError, match=r"selector"):
        run_batch(pg_conn, limit=5, dry_run=True)


def test_run_batch_valida_meses_sin_anos(pg_conn):
    """meses sin anos → ValueError."""
    from etl.nlp.pipeline import run_batch

    with pytest.raises(ValueError, match=r"meses"):
        run_batch(pg_conn, limit=5, dry_run=True, meses=(3, 6), todo=True)


def test_run_batch_valida_mutual_exclusion(pg_conn):
    """anos + codigo_bdns simultáneos → ValueError."""
    from etl.nlp.pipeline import run_batch

    with pytest.raises(ValueError, match=r"mutual|exclus"):
        run_batch(pg_conn, limit=5, dry_run=True, anos=(2024, 2024), codigo_bdns=123)


# -----------------------------------------------------------------------------
# Hito 3.1.d — Tracking en ops.nlp_runs
# -----------------------------------------------------------------------------


def _fetch_run(conn, run_id: int) -> dict:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM ops.nlp_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def test_run_batch_creates_nlp_run_row(pg_conn):
    """Con dry_run=True, run_batch crea fila en ops.nlp_runs y la cierra ok.

    BatchStats expone run_id; los counters terminan a cero (no hay items reales)
    y status='ok'.
    """
    from etl.nlp.pipeline import BatchStats, run_batch

    stats = run_batch(pg_conn, limit=1, dry_run=True, codigo_bdns=-99999999)
    assert isinstance(stats, BatchStats)
    assert hasattr(stats, "run_id")
    assert stats.run_id is not None

    row = _fetch_run(pg_conn, stats.run_id)
    assert row is not None
    assert row["status"] == "ok"
    assert row["finished_at"] is not None
    assert row["dry_run"] is True
    assert row["selector_kind"] == "codigo_bdns"


def test_run_batch_selector_value_jsonb_anos_meses(pg_conn):
    """selector_value JSONB contiene {anos:[a,b], meses:[m1,m2]} para anos+meses."""
    from etl.nlp.pipeline import run_batch

    stats = run_batch(pg_conn, limit=1, dry_run=True, anos=(2024, 2024), meses=(3, 6))
    row = _fetch_run(pg_conn, stats.run_id)
    assert row["selector_kind"] == "anos"
    assert row["selector_value"] == {"anos": [2024, 2024], "meses": [3, 6]}
    assert row["limit_value"] == 1


def test_run_batch_selector_value_codigo_bdns(pg_conn):
    """selector_value JSONB = {codigo_bdns: N} para --codigo-bdns."""
    from etl.nlp.pipeline import run_batch

    stats = run_batch(pg_conn, limit=10, dry_run=True, codigo_bdns=-99999999)
    row = _fetch_run(pg_conn, stats.run_id)
    assert row["selector_kind"] == "codigo_bdns"
    assert row["selector_value"] == {"codigo_bdns": -99999999}


def test_run_batch_selector_value_todo(pg_conn):
    """selector_value JSONB = {} para --todo."""
    from etl.nlp.pipeline import run_batch

    stats = run_batch(pg_conn, limit=1, dry_run=True, todo=True)
    row = _fetch_run(pg_conn, stats.run_id)
    assert row["selector_kind"] == "todo"
    assert row["selector_value"] == {}


def test_run_batch_marks_failed_on_exception(pg_conn, monkeypatch):
    """Si select_pending lanza, run_batch cierra fila con status='failed'.

    Capturamos el run_id con un side_effect que también lo guarda fuera
    para verificar la fila tras la excepción.
    """
    from etl.nlp import pipeline

    captured = {}
    real_create = pipeline._create_nlp_run

    def _wrap_create(*args, **kwargs):
        rid = real_create(*args, **kwargs)
        captured["run_id"] = rid
        return rid

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(pipeline, "_create_nlp_run", _wrap_create)
    monkeypatch.setattr(pipeline, "select_pending", _boom)

    with pytest.raises(RuntimeError, match="simulated failure"):
        pipeline.run_batch(pg_conn, limit=1, dry_run=True, todo=True)

    assert "run_id" in captured
    row = _fetch_run(pg_conn, captured["run_id"])
    assert row is not None
    assert row["status"] == "failed"
    assert row["finished_at"] is not None
    assert "simulated failure" in (row["error_message"] or "")


def test_run_batch_uses_env_nlp_run_id(pg_conn, monkeypatch):
    """Si NLP_RUN_ID está en env, run_batch reusa esa fila (no crea otra).

    El endpoint POST /nlp/analizar crea la fila ANTES del Popen y pasa el id
    via env var para evitar la race contra el polling de la UI.
    """
    from etl.nlp import pipeline

    # Crear una fila pre-existente "owned" por el caller.
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.nlp_runs (selector_kind, selector_value, limit_value, dry_run, force, llm_model, pid)
            VALUES ('todo', '{}'::jsonb, 1, TRUE, FALSE, NULL, NULL)
            RETURNING run_id
            """
        )
        pre_existing = cur.fetchone()[0]
    pg_conn.commit()

    monkeypatch.setenv("NLP_RUN_ID", str(pre_existing))
    stats = pipeline.run_batch(pg_conn, limit=1, dry_run=True, todo=True)
    assert stats.run_id == pre_existing

    row = _fetch_run(pg_conn, pre_existing)
    assert row["status"] == "ok"
    assert row["finished_at"] is not None
