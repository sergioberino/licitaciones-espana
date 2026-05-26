"""Hito 3.1 cierre: recovery de runs zombi en ops.nlp_runs.

Patrón paralelo al scheduler.recover_stale_runs: detecta filas con
status='running' cuyo PID ya no existe (container reiniciado) o cuyo
``started_at`` excede ``MAX_NLP_RUN_DURATION_HOURS`` (4h por defecto).
"""
from __future__ import annotations

import json
import os

import pytest

from etl.nlp.recovery import (
    MAX_NLP_RUN_DURATION_HOURS,
    recover_stale_nlp_runs,
)


def _insert_run(
    conn,
    *,
    pid,
    started_offset_hours: float = 0,
    status: str = "running",
    selector_kind: str = "todo",
    selector_value: dict | None = None,
):
    """Inserta una fila sentinela en ops.nlp_runs y devuelve run_id.

    ``started_offset_hours``: horas restadas a NOW() (positivo = pasado).
    """
    if selector_value is None:
        selector_value = {"todo": True}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.nlp_runs
              (selector_kind, selector_value, started_at, status, pid)
            VALUES (%s, %s::jsonb, NOW() - (INTERVAL '1 hour' * %s), %s, %s)
            RETURNING run_id
            """,
            (selector_kind, json.dumps(selector_value), started_offset_hours, status, pid),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _fetch_status(conn, run_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, error_message, finished_at FROM ops.nlp_runs WHERE run_id = %s",
            (run_id,),
        )
        return cur.fetchone()


def _delete_runs(conn, run_ids):
    if not run_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ops.nlp_runs WHERE run_id = ANY(%s)", (list(run_ids),))
    conn.commit()


def test_recover_dead_pid_marks_failed(pg_conn):
    """PID inexistente con started_at reciente → marca failed con motivo 'not found'."""
    run_id = _insert_run(pg_conn, pid=99999999, started_offset_hours=0)
    try:
        recovered = recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status, error_message, finished_at = _fetch_status(pg_conn, run_id)
        assert status == "failed"
        assert error_message is not None and "not found" in error_message
        assert finished_at is not None
        assert recovered >= 1
    finally:
        _delete_runs(pg_conn, [run_id])


def test_recover_alive_pid_within_cutoff_no_action(pg_conn):
    """PID vivo (os.getpid()) y started_at reciente → no se toca."""
    run_id = _insert_run(pg_conn, pid=os.getpid(), started_offset_hours=0)
    try:
        recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status, error_message, _ = _fetch_status(pg_conn, run_id)
        assert status == "running"
        assert error_message is None
    finally:
        _delete_runs(pg_conn, [run_id])


def test_recover_alive_pid_exceeds_cutoff_marks_timeout(pg_conn):
    """PID vivo pero started_at > 4h → marca failed con motivo 'Timeout'."""
    run_id = _insert_run(
        pg_conn,
        pid=os.getpid(),
        started_offset_hours=MAX_NLP_RUN_DURATION_HOURS + 1,
    )
    try:
        recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status, error_message, finished_at = _fetch_status(pg_conn, run_id)
        assert status == "failed"
        assert error_message is not None and "Timeout" in error_message
        assert finished_at is not None
    finally:
        _delete_runs(pg_conn, [run_id])


def test_recover_no_pid_within_cutoff_no_action(pg_conn):
    """pid=NULL y started_at reciente → no se toca (subprocess aún arrancando)."""
    run_id = _insert_run(pg_conn, pid=None, started_offset_hours=0)
    try:
        recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status, error_message, _ = _fetch_status(pg_conn, run_id)
        assert status == "running"
        assert error_message is None
    finally:
        _delete_runs(pg_conn, [run_id])


def test_recover_no_pid_exceeds_cutoff_marks_timeout(pg_conn):
    """pid=NULL y started_at > 4h → marca failed con motivo timeout."""
    run_id = _insert_run(
        pg_conn,
        pid=None,
        started_offset_hours=MAX_NLP_RUN_DURATION_HOURS + 1,
    )
    try:
        recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status, error_message, _ = _fetch_status(pg_conn, run_id)
        assert status == "failed"
        assert error_message is not None and "Timeout" in error_message
    finally:
        _delete_runs(pg_conn, [run_id])


def test_recover_returns_count(pg_conn):
    """Cuenta exacta: 3 runs muertos + 1 vivo dentro de cutoff → return 3."""
    dead_ids = [
        _insert_run(pg_conn, pid=99999991, started_offset_hours=0),
        _insert_run(pg_conn, pid=99999992, started_offset_hours=0),
        _insert_run(pg_conn, pid=99999993, started_offset_hours=0),
    ]
    alive_id = _insert_run(pg_conn, pid=os.getpid(), started_offset_hours=0)
    try:
        count = recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        assert count == 3
        for did in dead_ids:
            status, _, _ = _fetch_status(pg_conn, did)
            assert status == "failed"
        status_alive, _, _ = _fetch_status(pg_conn, alive_id)
        assert status_alive == "running"
    finally:
        _delete_runs(pg_conn, dead_ids + [alive_id])


def test_recover_does_not_touch_finished_runs(pg_conn):
    """status='ok' o 'failed' nunca son tocados (filtro WHERE status='running')."""
    ok_id = _insert_run(pg_conn, pid=99999994, status="ok", started_offset_hours=10)
    failed_id = _insert_run(
        pg_conn, pid=99999995, status="failed", started_offset_hours=10
    )
    try:
        recover_stale_nlp_runs(pg_conn)
        pg_conn.commit()
        status_ok, msg_ok, _ = _fetch_status(pg_conn, ok_id)
        status_failed, _, _ = _fetch_status(pg_conn, failed_id)
        assert status_ok == "ok"
        assert msg_ok is None
        assert status_failed == "failed"
    finally:
        _delete_runs(pg_conn, [ok_id, failed_id])
