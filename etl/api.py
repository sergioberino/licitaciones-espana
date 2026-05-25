import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, model_validator

from etl import __version__
from etl.cli import (
    cmd_scheduler_register,
    cmd_scheduler_run,
    cmd_scheduler_stop,
    _comprobar_base_datos,
)
from etl.config import get_database_url
from etl.ingest_l0 import CONJUNTOS_REGISTRY
from etl.scheduler import (
    VALID_SCHEDULE_EXPRS,
    _build_default_schedules,
    _build_task_cmd,
    ensure_scheduler_schema,
    get_current_running_run,
    get_next_run_at,
    get_scheduler_log_path,
    get_scheduler_pid_path,
    is_scheduler_loop_running,
    list_running_runs,
    list_tasks_with_last_run,
    recover_stale_runs,
    register_tasks,
    stop_runs_by_ids,
    validate_schedule_expr,
)


def _ingest_log_path() -> Path:
    return Path(os.environ.get("LICITACIONES_TMP_DIR", "/tmp")) / "ingest.log"


def _borme_log_path() -> Path:
    return Path(os.environ.get("LICITACIONES_TMP_DIR", "/tmp")) / "borme.log"


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
    tasks: list[dict] = []
    schedule_expr: str | None = None


class SchedulerRunBody(BaseModel):
    """Body for POST /scheduler/run. Optional conjunto/subconjunto for single task; detach to run loop in background."""

    conjunto: str | None = None
    subconjunto: str | None = None
    detach: bool = False


class SchedulerRunsStopBody(BaseModel):
    run_ids: list[int]


class SchedulerUnregisterBody(BaseModel):
    tasks: list[dict]


class IncidentResolveBody(BaseModel):
    resolution: str
    note: str | None = None


class BormeIngestBody(BaseModel):
    anos: str


class BormeAnomaliasBody(BaseModel):
    anos: str
    anonimizar: bool = False


class SubvencionesFetchBody(BaseModel):
    numConv: list[int]


app = FastAPI(
    title="ETL API",
    version=__version__,
    description="Microservicio ETL: ingest L0, scheduler, BORME.",
)


@app.on_event("startup")
def _startup_recover_stale_runs():
    """Auto-heal stale 'running' scheduler records left by dead containers."""
    db_url = get_database_url()
    if not db_url:
        app.state.stale_runs_recovered = 0
        return
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            count = recover_stale_runs(conn)
            conn.commit()
            app.state.stale_runs_recovered = count
            if count:
                print(f"[startup] Recovered {count} stale scheduler run(s).")
    except Exception as e:
        app.state.stale_runs_recovered = 0
        print(f"[startup] Could not recover stale runs: {e}")


@app.on_event("startup")
def _startup_schema_check():
    """Log-only schema migration check on boot (no auto-apply, no state dependency)."""
    from etl import schema_check

    url = get_database_url()
    if not url:
        return
    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        try:
            schema_check.bootstrap(conn)
            status = schema_check.check(conn)
            schema_check.log_check(status)
        finally:
            conn.close()
    except Exception:
        pass


@app.on_event("startup")
def _startup_crash_detection():
    """Detect if ETL restarted after a crash and record incident."""
    from etl.log_supervisor import LogSupervisor
    from etl.scheduler import get_scheduler_pid_path

    app.state.recovery_status = None
    db_url = get_database_url()
    if not db_url:
        return

    stale_recovered = getattr(app.state, "stale_runs_recovered", 0)
    pid_path = get_scheduler_pid_path()
    stale_pid = False

    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
            os.kill(pid, 0)
        except (ValueError, ProcessLookupError):
            stale_pid = True
            try:
                pid_path.unlink(missing_ok=True)
            except OSError:
                pass
        except PermissionError:
            pass

    if not stale_recovered and not stale_pid:
        return

    log_path = pid_path.parent / "scheduler.log"
    hb_path = pid_path.parent / "scheduler.heartbeat"
    supervisor = LogSupervisor(
        log_path=log_path,
        db_url=db_url,
        heartbeat_path=hb_path,
    )

    from etl.docker_incident_context import collect_container_snapshot

    detail_parts: list[str] = []
    if stale_recovered:
        detail_parts.append(
            f"Se marcaron como fallidas {stale_recovered} ejecución(es) que quedaron en estado "
            "«running» sin proceso vivo (posible reinicio del contenedor o parada abrupta)."
        )
    if stale_pid:
        detail_parts.append(
            "Archivo PID del scheduler huérfano: apuntaba a un proceso que ya no existe "
            "(habitual tras reinicio manual del servicio o del contenedor)."
        )

    docker_ctx = collect_container_snapshot()
    detail_text = "\n\n".join(detail_parts) + "\n\n---\n\n" + docker_ctx

    supervisor.incident(
        "service_crash",
        "Reinicio del servicio ETL: posible caída o parada no limpia detectada al arrancar",
        detail=detail_text,
        severity="error",
    )
    print(f"[startup] Incidente de arranque registrado: {' | '.join(detail_parts)}")

    app.state.recovery_status = {
        "detected": True,
        "stale_runs": stale_recovered,
        "stale_pid": stale_pid,
        "auto_restart_attempted": False,
        "auto_restart_success": False,
    }

    # Attempt auto-restart of scheduler in background thread
    import threading

    def _auto_restart():
        import time

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                print(f"[crash-recovery] Intento de reinicio automático {attempt}/{max_retries}")
                args = argparse.Namespace(
                    conjunto=None,
                    subconjunto=None,
                    run_all=True,
                    tick_seconds=60,
                    detach=True,
                )
                rc = cmd_scheduler_run(args)
                if rc == 0:
                    app.state.recovery_status["auto_restart_attempted"] = True
                    app.state.recovery_status["auto_restart_success"] = True
                    print(
                        f"[crash-recovery] Scheduler reiniciado correctamente en el intento {attempt}"
                    )
                    return
            except Exception as e:
                print(f"[crash-recovery] Intento {attempt} fallido: {e}")
            time.sleep(5)

        app.state.recovery_status["auto_restart_attempted"] = True
        app.state.recovery_status["auto_restart_success"] = False
        fail_detail = (
            f"No se pudo reiniciar el scheduler automáticamente tras {max_retries} intentos. "
            "Revise los logs del contenedor ETL y el estado del proceso.\n\n---\n\n"
            + collect_container_snapshot()
        )
        supervisor.incident(
            "auto_restart_failed",
            "Fallo al reiniciar el scheduler automáticamente tras varios intentos",
            detail=fail_detail,
            severity="error",
        )
        print(
            f"[crash-recovery] Fallaron los {max_retries} intentos de reinicio. "
            "Revise los logs del contenedor ETL."
        )

    thread = threading.Thread(target=_auto_restart, daemon=True)
    thread.start()


def _serialize_row(row: dict) -> dict:
    """Convert a row dict to JSON-serializable form (datetime -> ISO string)."""
    out = dict(row)
    for key in (
        "started_at",
        "finished_at",
        "last_started_at",
        "last_finished_at",
        "next_run_at",
        "created_at",
        "resolved_at",
    ):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out


def _get_dim_status(conn) -> dict | None:
    """Check row and embedding presence for each dim table."""

    def _check_table(cur, table: str) -> dict:
        try:
            cur.execute(f"SELECT EXISTS(SELECT 1 FROM dim.{table})")
            has_rows = cur.fetchone()[0]
        except Exception:
            return {"has_rows": False, "has_embeddings": False}
        has_embeddings = False
        if has_rows:
            try:
                cur.execute(f"SELECT EXISTS(SELECT 1 FROM dim.{table} WHERE embedding IS NOT NULL)")
                has_embeddings = cur.fetchone()[0]
            except Exception:
                pass
        return {"has_rows": has_rows, "has_embeddings": has_embeddings}

    try:
        with conn.cursor() as cur:
            return {
                "cpv": _check_table(cur, "cpv_dim"),
                "cnae": _check_table(cur, "cnae_dim"),
            }
    except Exception:
        return None


@app.get(
    "/health",
    summary="Health check",
    description="Liveness, DB connectivity, migration status, and dimension table readiness.",
)
def health():
    db_url = get_database_url()
    db_ok = False
    dim_status = None
    if db_url:
        try:
            with psycopg2.connect(db_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                db_ok = True
                dim_status = _get_dim_status(conn)
        except Exception:
            pass
    migrations = {
        "schema_check_delegated": True,
        "note": "Use migrator job for migration management",
    }

    payload = {
        "status": "ok" if db_ok else "degraded",
        "db": db_ok,
        "migrations": migrations,
        "dim_status": dim_status,
    }
    status_code = 200 if db_ok else 503
    return JSONResponse(content=payload, status_code=status_code)


@app.get(
    "/migrations",
    summary="Migration audit trail",
    description="Returns all recorded schema migrations.",
)
def get_migrations():
    from etl import schema_check
    from etl.cli import INIT_MIGRATIONS

    db_url = get_database_url()
    if not db_url:
        return JSONResponse(status_code=503, content={"detail": "Database not configured"})
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        try:
            status = schema_check.check(conn)
            rows = schema_check.get_rows(conn)
        finally:
            conn.close()
        init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
        return {
            "service": "etl",
            "version": __version__,
            "deprecated": "Use ops.migrator_history via 'docker compose run migrator --history'. This endpoint will be removed in a future version.",
            "status": {
                "pending": len(status.pending),
                "pending_infra": len(init_pending),
                "applied": len(status.applied),
                "tampered": len(status.tampered),
            },
            "pending_files": status.pending,
            "tampered_files": status.tampered,
            "migrations": rows,
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"detail": str(e)})


@app.get(
    "/ddl",
    summary="DDL contract listing",
    description="Lists all .sql schema files with checksums and sizes. No DB access required.",
)
def get_ddl():
    from etl.schema_check import _schemas_dir, sha256 as _sha256

    d = _schemas_dir()
    schemas = []
    for f in sorted(d.iterdir(), key=lambda p: p.name):
        if f.suffix == ".sql":
            schemas.append(
                {
                    "filename": f.name,
                    "checksum": _sha256(f),
                    "size_bytes": f.stat().st_size,
                }
            )
    return {
        "etl_version": __version__,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "schemas": schemas,
    }


@app.get(
    "/ddl/{filename}",
    summary="DDL file content",
    description="Returns the raw SQL content of a schema file as text/plain.",
)
def get_ddl_file(filename: str):
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    from etl.schema_check import _schemas_dir

    path = _schemas_dir() / filename
    if not path.exists() or not filename.endswith(".sql"):
        raise HTTPException(status_code=404, detail="File not found")
    return PlainTextResponse(path.read_text(encoding="utf-8"), media_type="text/plain")


@app.post(
    "/init-db",
    summary="Apply init-db migrations",
    description="Applies infrastructure migrations (dim, scheduler). Returns results for dashboard.",
)
def post_init_db():
    """Run init-db logic and return JSON for the frontend migration apply flow."""
    from etl import schema_check
    from etl.cli import run_init_db, _schema_dir, INIT_MIGRATIONS

    exit_code, results = run_init_db(schema_dir=_schema_dir())
    # Refresh migration status so next /health reflects applied count
    db_url = get_database_url()
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            try:
                schema_check.bootstrap(conn)
                status = schema_check.check(conn)
                init_pending = [f for f in status.pending if f in INIT_MIGRATIONS]
                app.state.migration_status = {
                    "pending": len(status.pending),
                    "pending_infra": len(init_pending),
                    "applied": len(status.applied),
                    "tampered": len(status.tampered),
                }
            finally:
                conn.close()
        except Exception:
            pass
    failed = [r for r in results if not r.get("success")]
    return {
        "ok": exit_code == 0,
        "deprecated": "Use 'docker compose run migrator --apply' for production upgrades. This endpoint is retained for greenfield/dev convenience.",
        "message": f"Init-db: {len(results)} processed, {len(failed)} failed.",
        "results": results,
    }


@app.get(
    "/ingest/conjuntos",
    summary="List ingest conjuntos and subconjuntos",
    description="Returns the same conjunto/subconjunto catalog as the CLI (ingest --subconjuntos).",
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
    summary="Run ingest for a conjunto/subconjunto (non-blocking)",
    description="Validates inputs then spawns the CLI ingest command as a background process. "
    "Returns immediately with 200. The subprocess registers its own scheduler run "
    "and updates it on completion. Poll GET /ingest/current-run for status and "
    "GET /ingest/log for live subprocess output.",
)
def ingest_run(body: IngestRunBody):
    """Spawn a background ingest process for the given conjunto/subconjunto."""
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
            content={
                "detail": f"Para '{conjunto}' indique subconjunto. Válidos: {', '.join(subconjuntos)}."
            },
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

    cmd = [sys.executable, "-m", "etl.cli", "ingest", conjunto]
    if subconjunto:
        cmd.append(subconjunto)
    if anos:
        cmd.extend(["--anos", anos])
    if body.solo_descargar:
        cmd.append("--solo-descargar")
    if body.solo_procesar:
        cmd.append("--solo-procesar")

    log_path = _ingest_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        log_file = open(log_path, "w", encoding="utf-8")
        ingest_env = os.environ.copy()
        ingest_env["PYTHONUNBUFFERED"] = "1"
        kwargs: dict = {
            "stdout": log_file,
            "stderr": subprocess.STDOUT,
            "env": ingest_env,
        }
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"No se pudo iniciar el ingest: {e}"},
        )

    label = f"{conjunto}/{subconjunto}" if subconjunto else conjunto
    return {
        "ok": True,
        "message": f"Ingest de {label} iniciado en segundo plano (PID: {p.pid}). "
        f"Consulte el panel para seguir el progreso.",
    }


@app.post(
    "/scheduler/register",
    summary="Register scheduler tasks",
    description='Populate scheduler.tasks from CONJUNTOS_REGISTRY. Optional body: { "conjuntos": ["nacional", ...] }. If conjuntos empty or omitted, register all. Returns 200 with summary.',
)
def scheduler_register(body: SchedulerRegisterBody | None = None):
    """Register scheduler tasks for given conjuntos or all."""
    if body and body.schedule_expr is not None:
        try:
            validate_schedule_expr(body.schedule_expr)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    if body and body.tasks:
        task_pairs = [(t["conjunto"], t["subconjunto"]) for t in body.tasks]
        schedule_overrides: dict[tuple[str, str], str] = {}
        global_expr = body.schedule_expr
        for t in body.tasks:
            key = (t["conjunto"], t["subconjunto"])
            task_expr = t.get("schedule_expr") or global_expr
            if task_expr:
                try:
                    validate_schedule_expr(task_expr)
                except ValueError as e:
                    raise HTTPException(status_code=422, detail=str(e))
                schedule_overrides[key] = task_expr
        url = get_database_url()
        if not url:
            return JSONResponse(
                status_code=503, content={"ok": False, "message": "Database not configured"}
            )
        try:
            with psycopg2.connect(url) as conn:
                conn.autocommit = False
                ensure_scheduler_schema(conn)
                conn.commit()
                inserted, updated, registered = register_tasks(
                    conn,
                    task_pairs=task_pairs,
                    schedule_overrides=schedule_overrides,
                )
                conn.commit()
        except Exception as e:
            return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
        return {
            "ok": True,
            "message": f"{len(registered)} tareas registradas.",
            "registered": len(registered),
            "inserted": inserted,
            "updated": updated,
        }
    conjuntos = (body and body.conjuntos) or []
    args = argparse.Namespace(conjuntos=conjuntos)
    try:
        rc = cmd_scheduler_register(args)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    if rc != 0:
        return JSONResponse(
            status_code=500, content={"ok": False, "message": "Register failed (see server logs)"}
        )
    return {"ok": True, "message": "Tasks registered"}


@app.get(
    "/scheduler/defaults",
    summary="Scheduler default schedule expressions",
    description="Returns the list of valid schedule expressions and the default schedule_expr per known task.",
)
def scheduler_defaults():
    """Return valid schedule expressions and per-task defaults from CONJUNTOS_REGISTRY."""
    raw = _build_default_schedules()
    defaults = [
        {"conjunto": c, "subconjunto": s, "schedule_expr": expr}
        for (c, s), expr in sorted(raw.items())
    ]
    return {"valid_exprs": list(VALID_SCHEDULE_EXPRS), "defaults": defaults}


@app.post(
    "/scheduler/run",
    summary="Run scheduler or single task",
    description="Single task: spawns CLI in background (same as POST /ingest/run) and returns immediately so proxies do not time out. If detach=true: start full scheduler loop in background (202).",
)
def scheduler_run(body: SchedulerRunBody | None = None):
    """Run a single task or start the scheduler loop in background."""
    body = body or SchedulerRunBody()
    conjunto = (body.conjunto or "").strip() or None
    subconjunto = (body.subconjunto or "").strip() or None
    if not body.detach and not conjunto:
        return JSONResponse(
            status_code=422,
            content={
                "detail": "Indique conjunto y subconjunto para una tarea, o detach=true para el bucle en background."
            },
        )
    if conjunto and subconjunto is None and conjunto in CONJUNTOS_REGISTRY:
        subs = list(CONJUNTOS_REGISTRY[conjunto].get("subconjuntos", ()))
        if len(subs) == 1:
            subconjunto = subs[0]
        elif len(subs) > 1:
            return JSONResponse(
                status_code=422,
                content={
                    "detail": f"El conjunto '{conjunto}' tiene varios subconjuntos. Indique uno: {', '.join(subs)}."
                },
            )
    if conjunto and not subconjunto and not body.detach:
        return JSONResponse(
            status_code=422,
            content={"detail": "Indique subconjunto para esta tarea."},
        )
    # Single task from UI/API: do not block the HTTP request for the full ingest duration (often
    # 10+ minutes). Proxies and browsers time out → 502 and false "failure" toasts while work continues.
    if conjunto and subconjunto and not body.detach:
        if conjunto not in CONJUNTOS_REGISTRY or conjunto == "test":
            return JSONResponse(
                status_code=422,
                content={
                    "detail": f"Conjunto no reconocido: {conjunto}. Admitidos: {', '.join(sorted(c for c in CONJUNTOS_REGISTRY if c != 'test'))}.",
                },
            )
        reg = CONJUNTOS_REGISTRY[conjunto]
        subconjuntos = list(reg.get("subconjuntos", ()))
        if subconjunto not in subconjuntos:
            return JSONResponse(
                status_code=422,
                content={"detail": f"Subconjunto no reconocido para {conjunto}: {subconjunto}."},
            )
        cmd = _build_task_cmd(conjunto, subconjunto)
        log_path = _ingest_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_file = open(log_path, "w", encoding="utf-8")
            ingest_env = os.environ.copy()
            ingest_env["PYTHONUNBUFFERED"] = "1"
            kwargs: dict = {
                "stdout": log_file,
                "stderr": subprocess.STDOUT,
                "env": ingest_env,
            }
            if os.name != "nt":
                kwargs["start_new_session"] = True
            p = subprocess.Popen(cmd, **kwargs)
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "message": f"No se pudo iniciar la tarea: {e}"},
            )
        label = f"{conjunto}/{subconjunto}"
        return {
            "ok": True,
            "message": f"Tarea {label} iniciada en segundo plano (PID: {p.pid}). "
            "Use la salida del proceso y las ejecuciones en curso para seguir el progreso.",
        }

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
        return JSONResponse(
            status_code=500, content={"ok": False, "message": "Run failed (see server logs)"}
        )
    if body.detach:
        return JSONResponse(
            status_code=202, content={"ok": True, "message": "Scheduler started in background"}
        )
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
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": "Stop failed (scheduler not running or invalid PID)"},
        )
    return {"ok": True, "message": "Scheduler stop requested"}


@app.post(
    "/scheduler/runs/stop",
    summary="Stop selected running runs",
    description="Send SIGTERM to the processes of the given run_ids and mark them as failed.",
)
def scheduler_runs_stop(body: SchedulerRunsStopBody):
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(
            status_code=503, content={"ok": False, "message": "Database not configured"}
        )
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            stopped, errors = stop_runs_by_ids(conn, body.run_ids)
            conn.commit()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    msg = f"{stopped} ejecución(es) detenida(s)."
    if errors:
        msg += f" Errores: {'; '.join(errors)}"
    return {"ok": True, "stopped": stopped, "message": msg}


@app.post(
    "/scheduler/recover",
    summary="Recover stale scheduler runs",
    description="Detect orphaned 'running' records (dead PID or timed-out) and mark them as failed. Safe to call at any time.",
)
def scheduler_recover():
    """Manually trigger stale-run recovery."""
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(
            status_code=503, content={"ok": False, "message": "Database not configured"}
        )
    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            count = recover_stale_runs(conn)
            conn.commit()
    except psycopg2.Error as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    return {"ok": True, "recovered": count}


@app.post(
    "/scheduler/unregister",
    summary="Unregister scheduler tasks",
    description="Delete scheduled tasks by (conjunto, subconjunto) pairs. Runs are deleted by CASCADE. Running processes are NOT stopped.",
)
def scheduler_unregister(body: SchedulerUnregisterBody):
    db_url = get_database_url()
    if db_url is None:
        return JSONResponse(
            status_code=503, content={"ok": False, "message": "Database not configured"}
        )
    if not body.tasks:
        return JSONResponse(
            status_code=422, content={"ok": False, "message": "No tasks specified."}
        )
    try:
        deleted = 0
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = False
            for t in body.tasks:
                conjunto = t.get("conjunto", "")
                subconjunto = t.get("subconjunto", "")
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM scheduler.tasks WHERE conjunto = %s AND subconjunto = %s",
                        (conjunto, subconjunto),
                    )
                    deleted += cur.rowcount
            conn.commit()
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})
    return {"ok": True, "deleted": deleted, "message": f"{deleted} tarea(s) eliminada(s)."}


@app.get("/scheduler/log", summary="Tail the scheduler daemon log")
def scheduler_log(lines: int = 200):
    """Read the tail of the scheduler daemon log."""
    log_path = get_scheduler_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"lines": [], "exists": True, "total_lines": 0}
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}


@app.get("/scheduler/incidents", summary="List scheduler incidents")
def scheduler_incidents(status: str = "open"):
    """List scheduler incidents, filtered by status (open, resolved, all)."""
    db_url = get_database_url()
    if not db_url:
        return JSONResponse(status_code=503, content={"incidents": []})
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if status == "open":
                    cur.execute(
                        "SELECT * FROM scheduler.incidents WHERE resolved_at IS NULL ORDER BY created_at DESC"
                    )
                elif status == "resolved":
                    cur.execute(
                        "SELECT * FROM scheduler.incidents WHERE resolved_at IS NOT NULL ORDER BY created_at DESC"
                    )
                else:
                    cur.execute("SELECT * FROM scheduler.incidents ORDER BY created_at DESC")
                rows = cur.fetchall()
        return {"incidents": [_serialize_row(dict(r)) for r in rows]}
    except Exception as e:
        return JSONResponse(status_code=500, content={"incidents": [], "error": str(e)})


@app.get("/scheduler/incidents/{incident_id}", summary="Get incident detail")
def scheduler_incident_detail(incident_id: int):
    """Get a single scheduler incident by ID."""
    db_url = get_database_url()
    if not db_url:
        return JSONResponse(status_code=503, content={"detail": "Database not configured"})
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM scheduler.incidents WHERE incident_id = %s",
                    (incident_id,),
                )
                row = cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"detail": "Incident not found"})
        return _serialize_row(dict(row))
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.post("/scheduler/incidents/{incident_id}/resolve", summary="Resolve a scheduler incident")
def scheduler_incident_resolve(incident_id: int, body: IncidentResolveBody):
    """Mark a scheduler incident as resolved."""
    db_url = get_database_url()
    if not db_url:
        return JSONResponse(
            status_code=503, content={"ok": False, "message": "Database not configured"}
        )
    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE scheduler.incidents
                       SET resolved_at = NOW(), resolved_by = 'user', resolution = %s
                       WHERE incident_id = %s AND resolved_at IS NULL""",
                    (
                        body.resolution + (f"\n\nNote: {body.note}" if body.note else ""),
                        incident_id,
                    ),
                )
                if cur.rowcount == 0:
                    return JSONResponse(
                        status_code=404,
                        content={"ok": False, "message": "Incident not found or already resolved"},
                    )
                conn.commit()
        return {"ok": True, "message": "Incident resolved"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@app.get(
    "/status",
    summary="Database status",
    description="Checks the database connection and returns availability.",
)
def status():
    ok, msg = _comprobar_base_datos()
    if ok:
        return {"status": "ok", "database": "connected"}
    return JSONResponse(
        status_code=503,
        content={"status": "error", "database": "unavailable"},
    )


@app.get(
    "/scheduler/status",
    summary="Scheduler tasks status",
    description="Lists all registered scheduler tasks with their last run info and computed next_run_at.",
)
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
                created_at=r.get("created_at"),
            )
            r["next_run_at"] = next_at
        list_of_dicts.append(_serialize_row(r))
    loop_running = is_scheduler_loop_running()

    hb_path = get_scheduler_pid_path().parent / "scheduler.heartbeat"
    last_heartbeat = None
    try:
        if hb_path.exists():
            last_heartbeat = json.loads(hb_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        pass

    open_incidents = 0
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM scheduler.incidents WHERE resolved_at IS NULL")
                open_incidents = cur.fetchone()[0]
    except Exception:
        pass

    recovery = getattr(app.state, "recovery_status", None)

    return {
        "tasks": list_of_dicts,
        "loop_running": loop_running,
        "last_heartbeat": last_heartbeat,
        "open_incidents": open_incidents,
        "recovery": recovery,
    }


@app.get(
    "/scheduler/running",
    summary="Running scheduler runs",
    description="Lists all currently running ingest runs with task metadata.",
)
def scheduler_running():
    url = get_database_url()
    if url is None:
        return JSONResponse(status_code=503, content={"runs": []})
    try:
        with psycopg2.connect(url) as conn:
            rows = list_running_runs(conn)
    except psycopg2.Error:
        return JSONResponse(status_code=503, content={"runs": []})
    return {"runs": [_serialize_row(r) for r in rows]}


@app.get(
    "/ingest/log",
    summary="Tail the ingest subprocess log",
    description="Returns the last N lines of the ingest log file written by the background subprocess. "
    "Poll this while an ingest is running to show live progress.",
)
def ingest_log(lines: int = 80):
    """Read the tail of the ingest subprocess log."""
    log_path = _ingest_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"lines": [], "exists": True, "total_lines": 0}
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}


@app.get(
    "/ingest/current-run",
    summary="Current running ingest job",
    description="Returns the currently running ingest scheduler run, if any.",
)
def ingest_current_run():
    """Return the currently running ingest run (if any)."""
    url = get_database_url()
    if url is None:
        return JSONResponse(status_code=503, content={"running": False, "run": None})
    try:
        with psycopg2.connect(url) as conn:
            run = get_current_running_run(conn)
    except psycopg2.Error:
        return JSONResponse(status_code=503, content={"running": False, "run": None})
    if not run:
        return {"running": False, "run": None}
    return {"running": True, "run": _serialize_row(run)}


@app.get(
    "/db-info",
    summary="Database info (schemas and tables)",
    description="Returns database schemas with sizes, table listing, and total database size.",
)
def db_info():
    url = get_database_url()
    if url is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "database unavailable"},
        )
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT n.nspname AS schema_name,
                           pg_size_pretty(SUM(pg_total_relation_size(c.oid))::bigint) AS size
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND n.nspname NOT LIKE 'pg_temp_%%'
                    GROUP BY n.nspname
                    ORDER BY SUM(pg_total_relation_size(c.oid)) DESC
                """)
                sizes = cur.fetchall()
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                      AND table_schema NOT LIKE 'pg_temp_%%'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                """)
                tables = cur.fetchall()
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                db_total = cur.fetchone()
    except psycopg2.Error:
        return JSONResponse(
            status_code=503,
            content={"detail": "database error"},
        )
    return {
        "schemas": [{"schema_name": r[0], "size": r[1]} for r in sizes],
        "tables": [{"schema": r[0], "name": r[1]} for r in tables],
        "total_size": db_total[0] if db_total else None,
    }


@app.post("/borme/ingest", summary="BORME: scrape + parse + load into borme schema")
def borme_ingest(body: BormeIngestBody):
    """Spawn background CLI process for BORME ingest."""
    cmd = [sys.executable, "-m", "etl.cli", "borme", "ingest", "--anos", body.anos]
    log_path = _borme_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_file = open(log_path, "w", encoding="utf-8")
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"Could not start BORME ingest: {e}"},
        )
    return {
        "ok": True,
        "message": f"BORME ingest started (PID: {p.pid})",
        "pid": p.pid,
        "anos": body.anos,
    }


@app.post("/borme/anomalias", summary="BORME: anomaly detector vs L0 nacional")
def borme_anomalias(body: BormeAnomaliasBody):
    """Spawn background CLI process for BORME anomaly detection."""
    cmd = [sys.executable, "-m", "etl.cli", "borme", "anomalias", "--anos", body.anos]
    if body.anonimizar:
        cmd.append("--anonimizar")
    log_path = _borme_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        log_file = open(log_path, "w", encoding="utf-8")
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "message": f"Could not start BORME anomalias: {e}"},
        )
    return {
        "ok": True,
        "message": f"BORME anomalías started (PID: {p.pid})",
        "pid": p.pid,
        "anos": body.anos,
        "anonimizar": body.anonimizar,
    }


@app.get("/borme/jobs/{job_id}", summary="BORME job status")
def borme_job_status(job_id: str):
    """Check status of a BORME job by PID."""
    try:
        pid = int(job_id)
        os.kill(pid, 0)
        alive = True
    except (ValueError, ProcessLookupError, PermissionError):
        alive = False
    log_path = _borme_log_path()
    log_lines = []
    if log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            log_lines = text.splitlines()[-50:]
        except OSError:
            pass
    return {
        "job_id": job_id,
        "alive": alive,
        "status": "running" if alive else "finished",
        "log_tail": log_lines,
    }


@app.post(
    "/cnae/ingest",
    summary="Ingest CNAE codes",
    description="Fetch CNAE-2025 codes from ISTAC API and upsert into dim.cnae_dim.",
)
def cnae_ingest():
    from etl.cnae_ingest import run_cnae_ingest

    try:
        result = run_cnae_ingest()
        if result["ok"]:
            return result
        return JSONResponse(status_code=500, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@app.post(
    "/subvenciones/especificas",
    summary="Insertar convocatorias específicas",
    description="Recibe un array de numConv. Los que ya existan en BD se omiten; el resto se insertan.",
)
def subvenciones_especificas(body: SubvencionesFetchBody):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from nacional.subvenciones import scrape_especificas

    try:
        result = scrape_especificas(body.numConv)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/borme/log", summary="Tail the BORME subprocess log")
def borme_log(lines: int = 80):
    """Read the tail of the BORME subprocess log."""
    log_path = _borme_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"lines": [], "exists": True, "total_lines": 0}
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}


# ===========================================================================
# Track B WP2.1 — NLP (Batch B)
# ===========================================================================
from etl.nlp.api import nlp_log_path, nlp_pid_path


def _validate_range_str(value: str, *, min_val: int, max_val: int, name: str) -> tuple[int, int]:
    """Valida formato 'X' o 'X-Y' contra min_val/max_val. Lanza ValueError."""
    raw = (value or "").strip()
    if not raw:
        raise ValueError(f"{name} no puede estar vacío")
    parts = raw.split("-")
    if len(parts) == 1:
        try:
            v = int(parts[0])
        except ValueError:
            raise ValueError(f"{name} debe ser X o X-Y con números enteros (recibido: '{value}')")
        start, end = v, v
    elif len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()
        if not a or not b:
            raise ValueError(f"{name} formato X-Y con extremos no vacíos (recibido: '{value}')")
        try:
            start, end = int(a), int(b)
        except ValueError:
            raise ValueError(f"{name} debe ser X o X-Y con números enteros (recibido: '{value}')")
    else:
        raise ValueError(f"{name} formato inválido (recibido: '{value}')")
    if start > end:
        raise ValueError(f"{name}: el inicio ({start}) no puede ser mayor que el fin ({end})")
    if start < min_val or end > max_val:
        raise ValueError(f"{name} fuera de rango [{min_val},{max_val}]: {start}-{end}")
    return start, end


class NlpAnalizarBody(BaseModel):
    """Body de POST /nlp/analizar (WP2.1.1).

    Selector obligatorio: ``anos`` (str X-Y), ``codigo_bdns`` (int), o ``todo`` (bool).
    Modificadores: ``meses`` (str N-M, solo con ``anos``), ``limit`` (default 100),
    ``force``, ``dry_run``.
    """

    anos: str | None = None
    meses: str | None = None
    codigo_bdns: int | None = None
    todo: bool = False
    limit: int = 100
    force: bool = False
    dry_run: bool = False

    @model_validator(mode="after")
    def _validate_selector(self):
        if self.anos is not None:
            _validate_range_str(self.anos, min_val=1900, max_val=2999, name="anos")
        if self.meses is not None:
            _validate_range_str(self.meses, min_val=1, max_val=12, name="meses")
        selectors = [self.anos is not None, self.codigo_bdns is not None, bool(self.todo)]
        n = sum(selectors)
        if n == 0:
            raise ValueError(
                "Falta selector: indique uno y solo uno de anos, codigo_bdns o todo."
            )
        if n > 1:
            raise ValueError(
                "Selectores mutual exclusion: anos, codigo_bdns y todo son mutuamente excluyentes."
            )
        if self.meses is not None and self.anos is None:
            raise ValueError("meses solo es válido junto a anos.")
        return self


@app.post(
    "/nlp/analizar",
    summary="Run NLP analysis batch (non-blocking)",
    description="Spawns 'licitia-etl nlp analizar' as background subprocess. "
    "Returns 202 + pid + log_path. Poll /nlp/log and /nlp/current-run.",
)
def nlp_analizar(body: NlpAnalizarBody):
    # --limit 0 = sin cap (WP2.1.1). Con --codigo-bdns el limit se ignora.
    if body.limit < 0 or body.limit > 10000:
        return JSONResponse(status_code=422, content={"detail": "limit must be 0..10000"})

    pid_file = nlp_pid_path()
    if pid_file.exists():
        try:
            running_pid = int(pid_file.read_text().strip())
            os.kill(running_pid, 0)
            return JSONResponse(
                status_code=409,
                content={"detail": f"nlp run already in progress (pid={running_pid})"},
            )
        except (OSError, ValueError):
            pid_file.unlink(missing_ok=True)

    cmd = [sys.executable, "-m", "etl.cli", "nlp", "analizar", "--limit", str(body.limit)]
    if body.anos is not None:
        cmd.extend(["--anos", body.anos])
    if body.meses is not None:
        cmd.extend(["--meses", body.meses])
    if body.codigo_bdns is not None:
        cmd.extend(["--codigo-bdns", str(body.codigo_bdns)])
    if body.todo:
        cmd.append("--todo")
    if body.force:
        cmd.append("--force")
    if body.dry_run:
        cmd.append("--dry-run")

    log_path = nlp_log_path()
    try:
        log_file = open(log_path, "w", encoding="utf-8")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        kwargs: dict = {"stdout": log_file, "stderr": subprocess.STDOUT, "env": env}
        if os.name != "nt":
            kwargs["start_new_session"] = True
        p = subprocess.Popen(cmd, **kwargs)
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"spawn failed: {e}"})

    pid_file.write_text(str(p.pid))
    return JSONResponse(
        status_code=202,
        content={
            "pid": p.pid,
            "log_path": str(log_path),
            "started_at": datetime.utcnow().isoformat() + "Z",
        },
    )


@app.get("/nlp/log", summary="Tail the NLP analizar subprocess log")
def nlp_log(lines: int = 200):
    log_path = nlp_log_path()
    if not log_path.exists():
        return {"lines": [], "exists": False, "total_lines": 0}
    text = log_path.read_text(encoding="utf-8", errors="replace")
    all_lines = text.splitlines()
    tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return {"lines": tail, "exists": True, "total_lines": len(all_lines)}


@app.get("/nlp/current-run", summary="Status of the current/last NLP run")
def nlp_current_run():
    pid_file = nlp_pid_path()
    if not pid_file.exists():
        return {"running": False, "pid": None, "started_at": None}
    try:
        running_pid = int(pid_file.read_text().strip())
        os.kill(running_pid, 0)
        return {
            "running": True,
            "pid": running_pid,
            "started_at": datetime.fromtimestamp(pid_file.stat().st_mtime).isoformat() + "Z",
        }
    except (OSError, ValueError):
        pid_file.unlink(missing_ok=True)
        return {"running": False, "pid": None, "started_at": None}


@app.get(
    "/subvenciones/{subvencion_id}/ficha",
    summary="Ficha ejecutiva de una convocatoria (datos NLP completos)",
    description="Devuelve la fila de nacional_subvenciones con cols dual-write NLP "
    "y nlp_json de subvenciones_nlp. Backend-to-backend (LicitIA).",
)
def subvenciones_ficha(subvencion_id: int):
    url = get_database_url()
    if url is None:
        return JSONResponse(status_code=503, content={"detail": "database unavailable"})
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT s.*, n.nlp_json, n.document_source, n.document_heuristic_step,
                           n.document_ref, n.schema_version, n.extracted_at AS nlp_extracted_at_cache
                    FROM l0.nacional_subvenciones s
                    LEFT JOIN l0.subvenciones_nlp n
                      ON s.nlp_document_key = n.document_key
                    WHERE s.id = %s
                    """,
                    (subvencion_id,),
                )
                row = cur.fetchone()
    except psycopg2.Error:
        return JSONResponse(status_code=503, content={"detail": "database error"})
    if row is None:
        raise HTTPException(status_code=404, detail=f"subvención {subvencion_id} no encontrada")
    out = _serialize_row(dict(row))
    for key in ("nlp_extracted_at_cache", "extracted_at", "ingested_at", "fecha_publicacion"):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out
