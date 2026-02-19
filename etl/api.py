from datetime import datetime

import psycopg2
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from etl.cli import _comprobar_base_datos
from etl.config import get_database_url, get_db_schema
from etl.scheduler import get_next_run_at, list_tasks_with_last_run

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
