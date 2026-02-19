import argparse
from datetime import datetime

import psycopg2
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from etl.cli import cmd_ingest, cmd_scheduler_register, cmd_scheduler_run, cmd_scheduler_stop, _comprobar_base_datos
from etl.config import get_database_url, get_db_schema
from etl.ingest_l0 import CONJUNTOS_REGISTRY
from etl.scheduler import get_next_run_at, list_tasks_with_last_run


class IngestRunBody(BaseModel):
    """Body for POST /ingest/run. Same options as CLI ingest command."""

    conjunto: str
    subconjunto: str | None = None
    anos: str | None = None
    solo_descargar: bool = False
    solo_procesar: bool = False


class SchedulerRegisterBody(BaseModel):
    """Body for POST /scheduler/register. Optional list of conjuntos; if empty, register all."""

    conjuntos: list[str] = []


class SchedulerRunBody(BaseModel):
    """Body for POST /scheduler/run. Optional conjunto/subconjunto for single task; detach to run loop in background."""

    conjunto: str | None = None
    subconjunto: str | None = None
    detach: bool = False

app = FastAPI(title="ETL API", version="1.0")


def _serialize_row(row: dict) -> dict:
    """Convert a row dict to JSON-serializable form (datetime -> ISO string)."""
    out = dict(row)
    for key in ("last_started_at", "last_finished_at", "next_run_at", "created_at"):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get(
    "/ingest/conjuntos",
    summary="List ingest conjuntos and subconjuntos",
    description="Returns the same conjunto/subconjunto catalog as the CLI (ingest --subconjuntos). Used by the frontend to populate the Ingestar form.",
)
def ingest_conjuntos():
    """List available conjuntos and their subconjuntos for ingest."""
    conjuntos = [
        {
            "id": cid,
            "subconjuntos": list(reg.get("subconjuntos", [])),
        }
        for cid, reg in sorted(CONJUNTOS_REGISTRY.items())
        if cid != "test"
    ]
    return {"conjuntos": conjuntos}


@app.post(
    "/ingest/run",
    summary="Run ingest for a conjunto/subconjunto",
    description="Runs the same logic as the CLI 'ingest' command: download/generate parquet and load into L0. Body: conjunto (required), subconjunto (optional), anos (optional, required for nacional/ted), solo_descargar, solo_procesar. Returns 200 with ok/message or 4xx/5xx on error.",
)
def ingest_run(body: IngestRunBody):
    """Execute ingest for the given conjunto and optional subconjunto."""
    conjunto = (body.conjunto or "").strip().lower()
    if not conjunto:
        return JSONResponse(
            status_code=422,
            content={"detail": "conjunto is required"},
        )
    if conjunto not in CONJUNTOS_REGISTRY or conjunto == "test":
        return JSONResponse(
            status_code=422,
            content={
                "detail": f"Conjunto no reconocido: {conjunto}. Admitidos: {', '.join(sorted(c for c in CONJUNTOS_REGISTRY if c != 'test'))}.",
            },
        )
    reg = CONJUNTOS_REGISTRY[conjunto]
    subconjuntos = list(reg.get("subconjuntos", []))
    subconjunto = (body.subconjunto or "").strip() or None
    if subconjunto is None and len(subconjuntos) == 1:
        subconjunto = subconjuntos[0]
    if subconjunto is None and len(subconjuntos) > 1:
        return JSONResponse(
            status_code=422,
            content={"detail": f"Para '{conjunto}' indique subconjunto. Válidos: {', '.join(subconjuntos)}."},
        )
    if subconjunto and subconjunto not in subconjuntos:
        return JSONResponse(
            status_code=422,
            content={"detail": f"Subconjunto no reconocido para {conjunto}: {subconjunto}."},
        )
    anos = (body.anos or "").strip() or ""
    if reg.get("requires_anos") and not anos:
        return JSONResponse(
            status_code=422,
            content={"detail": "Obligatorio indicar anos X-Y para este conjunto (ej. 2023-2023)."},
        )
    args = argparse.Namespace(
        conjunto=conjunto,
        subconjunto=subconjunto,
        anos=anos,
        solo_descargar=body.solo_descargar,
        solo_procesar=body.solo_procesar,
        ingest_keep_parquet=False,
        ingest_list_subconjuntos=False,
        ingest_integration=False,
        ingest_conjuntos=False,
        ingest_delete=False,
        ingest_verbose=False,
        e2e_schema=None,
    )
    try:
        rc = cmd_ingest(args)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": str(e)},
        )
    if rc == 0:
        return {"ok": True, "message": "Ingest completed"}
    return JSONResponse(
        status_code=500,
        content={"ok": False, "message": "Ingest failed (see server logs)"},
    )


@app.post(
    "/scheduler/register",
    summary="Register scheduler tasks",
    description="Populate scheduler.tasks from CONJUNTOS_REGISTRY. Optional body: { \"conjuntos\": [\"nacional\", ...] }. If conjuntos empty or omitted, register all. Returns 200 with summary.",
)
def scheduler_register(body: SchedulerRegisterBody | None = None):
    """Register scheduler tasks for given conjuntos or all."""
    conjuntos = (body and body.conjuntos) or []
    args = argparse.Namespace(conjuntos=conjuntos)
    try:
        rc = cmd_scheduler_register(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Register failed (see server logs)"})
    return {"ok": True, "message": "Tasks registered"}


@app.post(
    "/scheduler/run",
    summary="Run scheduler or single task",
    description="If conjunto (and subconjunto if needed) given: run that task once (blocking, 200). If detach=true: start full scheduler loop in background (202). Reuses CLI logic.",
)
def scheduler_run(body: SchedulerRunBody | None = None):
    """Run a single task or start the scheduler loop in background."""
    body = body or SchedulerRunBody()
    conjunto = (body.conjunto or "").strip() or None
    subconjunto = (body.subconjunto or "").strip() or None
    if not body.detach and not conjunto:
        return JSONResponse(
            status_code=422,
            content={"detail": "Indique conjunto y subconjunto para una tarea, o detach=true para el bucle en background."},
        )
    if conjunto and subconjunto is None and conjunto in CONJUNTOS_REGISTRY:
        subs = list(CONJUNTOS_REGISTRY[conjunto].get("subconjuntos", ()))
        if len(subs) == 1:
            subconjunto = subs[0]
        elif len(subs) > 1:
            return JSONResponse(
                status_code=422,
                content={"detail": f"El conjunto '{conjunto}' tiene varios subconjuntos. Indique uno: {', '.join(subs)}."},
            )
    if conjunto and not subconjunto and not body.detach:
        return JSONResponse(
            status_code=422,
            content={"detail": "Indique subconjunto para esta tarea."},
        )
    args = argparse.Namespace(
        conjunto=conjunto,
        subconjunto=subconjunto,
        run_all=body.detach,
        tick_seconds=60,
        detach=body.detach,
    )
    try:
        rc = cmd_scheduler_run(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Run failed (see server logs)"})
    if body.detach:
        return JSONResponse(status_code=202, content={"ok": True, "message": "Scheduler started in background"})
    return {"ok": True, "message": "Task completed"}


@app.post(
    "/scheduler/stop",
    summary="Stop scheduler process",
    description="Send SIGTERM to the scheduler process (same as CLI 'scheduler stop'). Returns 200.",
)
def scheduler_stop():
    """Stop the scheduler process."""
    args = argparse.Namespace()
    try:
        rc = cmd_scheduler_stop(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(status_code=500, content={"ok": False, "message": "Stop failed (scheduler not running or invalid PID)"})
    return {"ok": True, "message": "Scheduler stop requested"}


@app.get("/status")
def status():
    ok, msg = _comprobar_base_datos()
    if ok:
        return {"status": "ok", "database": "connected"}
    return JSONResponse(
        status_code=503,
        content={"status": "error", "database": "unavailable"},
    )


@app.get("/scheduler/status")
def scheduler_status():
    url = get_database_url()
    if url is None:
        return JSONResponse(
            status_code=503,
            content={"tasks": []},
        )
    try:
        with psycopg2.connect(url) as conn:
            rows = list_tasks_with_last_run(conn)
    except psycopg2.Error:
        return JSONResponse(
            status_code=503,
            content={"tasks": []},
        )
    list_of_dicts = []
    for row in rows:
        r = dict(row)
        if r.get("last_status") == "running":
            r["next_run_at"] = None
        else:
            next_at = get_next_run_at(
                r.get("schedule_expr") or "Trimestral",
                r.get("last_finished_at"),
            )
            r["next_run_at"] = next_at
        list_of_dicts.append(_serialize_row(r))
    return {"tasks": list_of_dicts}


@app.get("/db-info")
def db_info():
    url = get_database_url()
    if url is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "database unavailable"},
        )
    db_schema = get_db_schema()
    schemas = ([db_schema] if db_schema else []) + ["dim", "scheduler"]
    placeholders = ", ".join(["%s"] * len(schemas))
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT nspname AS schema_name,
                           pg_size_pretty(SUM(pg_total_relation_size(c.oid))::bigint) AS size
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE nspname IN ({placeholders})
                    GROUP BY nspname
                    """,
                    schemas,
                )
                sizes = cur.fetchall()
                cur.execute(
                    f"""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema IN ({placeholders})
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                    """,
                    schemas,
                )
                tables = cur.fetchall()
    except psycopg2.Error:
        return JSONResponse(
            status_code=503,
            content={"detail": "database error"},
        )
    return {
        "schemas": [{"schema_name": r[0], "size": r[1]} for r in sizes],
        "tables": [{"schema": r[0], "name": r[1]} for r in tables],
    }
