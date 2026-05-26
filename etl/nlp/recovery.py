"""Auto-heal de runs zombi en ops.nlp_runs.

Patrón paralelo a :func:`etl.scheduler.recover_stale_runs`: detecta filas con
``status='running'`` cuyo proceso ya no existe (container reiniciado, kill -9)
o cuyo ``started_at`` excede ``MAX_NLP_RUN_DURATION_HOURS`` (default 4h) y las
marca como ``failed`` con ``error_message`` descriptivo.

Llamado desde:
- ``_startup_recover_stale_nlp_runs`` (hook startup de la API).
- ``nlp_analizar`` antes de spawnear un nuevo Popen.
- ``POST /nlp/runs/recover`` (botón manual desde la UI).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

MAX_NLP_RUN_DURATION_HOURS = int(os.environ.get("NLP_MAX_RUN_HOURS", "4"))


def recover_stale_nlp_runs(conn) -> int:
    """Detect orphaned 'running' nlp_runs (dead PID o timeout) y márcalos failed.

    Returns:
        Número de runs marcados como failed.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT run_id, pid, started_at FROM ops.nlp_runs WHERE status = 'running'"
        )
        stale = cur.fetchall()

    if not stale:
        return 0

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=MAX_NLP_RUN_DURATION_HOURS)
    recovered = 0

    for row in stale:
        pid = row.get("pid")
        started = row.get("started_at")
        reason: str | None = None

        if pid:
            try:
                os.kill(pid, 0)
                if started and started < cutoff:
                    reason = f"Timeout: running for >{MAX_NLP_RUN_DURATION_HOURS}h"
                else:
                    continue
            except ProcessLookupError:
                reason = f"Process {pid} not found (container restarted?)"
            except PermissionError:
                continue
        else:
            if started and started < cutoff:
                reason = (
                    f"Timeout: no PID recorded, running >{MAX_NLP_RUN_DURATION_HOURS}h"
                )
            else:
                continue

        if reason:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ops.nlp_runs
                       SET status = 'failed',
                           finished_at = NOW(),
                           error_message = %s
                     WHERE run_id = %s
                    """,
                    (reason, row["run_id"]),
                )
            recovered += 1

    return recovered
