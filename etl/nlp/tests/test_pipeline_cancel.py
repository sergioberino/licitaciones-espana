"""Tests para cancelación cooperativa vía SIGTERM (Hito 3.2.a).

Cubren tres comportamientos:

1. ``request_cancel()`` y ``is_cancel_requested()`` exponen un flag global.
2. ``_install_sigterm_handler()`` registra un handler de SIGTERM que setea el
   flag (el envío real lo hace el endpoint POST /nlp/runs/{id}/cancel).
3. Si el flag se setea entre items del bucle de ``run_batch``, el run se cierra
   con ``status='cancelled'`` y los counters reflejan el trabajo parcial.
"""
from __future__ import annotations

import signal

import pytest


def _fetch_run(conn, run_id: int) -> dict:
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM ops.nlp_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def test_request_cancel_sets_flag():
    from etl.nlp import pipeline

    pipeline._reset_cancel_flag()
    assert pipeline.is_cancel_requested() is False
    pipeline.request_cancel()
    try:
        assert pipeline.is_cancel_requested() is True
    finally:
        pipeline._reset_cancel_flag()


def test_install_sigterm_handler_sets_flag_on_signal():
    """El handler instalado debe setear el flag al recibir SIGTERM (mismo proceso)."""
    import os

    if os.name == "nt":
        pytest.skip("SIGTERM no soportado en Windows")

    from etl.nlp import pipeline

    pipeline._reset_cancel_flag()
    previous = signal.getsignal(signal.SIGTERM)
    try:
        pipeline._install_sigterm_handler()
        os.kill(os.getpid(), signal.SIGTERM)
        # El handler es síncrono dentro del intérprete Python; el flag ya está set.
        assert pipeline.is_cancel_requested() is True
    finally:
        pipeline._reset_cancel_flag()
        signal.signal(signal.SIGTERM, previous)


def test_run_batch_resets_cancel_flag_on_entry(pg_conn):
    """run_batch limpia un flag residual de un run anterior antes de empezar."""
    from etl.nlp import pipeline

    pipeline.request_cancel()
    try:
        stats = pipeline.run_batch(pg_conn, limit=1, dry_run=True, codigo_bdns=-99999998)
        assert stats.run_id is not None
        row = _fetch_run(pg_conn, stats.run_id)
        assert row["status"] == "ok"
        assert row["finished_at"] is not None
    finally:
        pipeline._reset_cancel_flag()


def test_run_batch_exits_cancelled_when_flag_set_mid_loop(pg_conn, monkeypatch):
    """Si request_cancel() se dispara tras el primer item, finaliza status='cancelled'.

    Simulamos 3 filas "pendientes" con select_pending patched. Tras cada
    iteración (vía hook en _bump_nlp_run de items_skipped) seteamos el flag y
    verificamos que el bucle rompe antes de procesar el resto.
    """
    from etl.nlp import pipeline

    pipeline._reset_cancel_flag()

    fake_rows = [(-99990001, None, None), (-99990002, None, None), (-99990003, None, None)]

    def _fake_select(*args, **kwargs):
        return fake_rows

    original_bump = pipeline._bump_nlp_run
    cancelled_after = {"called": False}

    def _bump_then_cancel(conn, run_id, **counters):
        original_bump(conn, run_id, **counters)
        if "items_skipped" in counters and not cancelled_after["called"]:
            pipeline.request_cancel()
            cancelled_after["called"] = True

    monkeypatch.setattr(pipeline, "select_pending", _fake_select)
    monkeypatch.setattr(pipeline, "_bump_nlp_run", _bump_then_cancel)

    try:
        stats = pipeline.run_batch(pg_conn, limit=10, dry_run=False, todo=True)
        assert stats.run_id is not None
        row = _fetch_run(pg_conn, stats.run_id)
        assert row is not None, "no se encontró la fila ops.nlp_runs"
        assert row["status"] == "cancelled"
        assert row["finished_at"] is not None
        assert row["items_planned"] == 3
        assert row["items_skipped"] == 1
        assert (row["error_message"] or "").startswith("cancelled")
    finally:
        pipeline._reset_cancel_flag()
        with pg_conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ops.nlp_runs WHERE run_id = %s",
                (stats.run_id,),
            )
        pg_conn.commit()
