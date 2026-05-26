"""Pipeline Batch B: SELECT pendientes → resolve → extract → LLM → validate → persist."""
from __future__ import annotations

import logging
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from etl.nlp.extractor import ExtractionError, extract_from_url
from etl.nlp.llm_client import analyze_document, load_schema
from etl.nlp.persistence import persist_analysis, propagate_from_cache
from etl.nlp.project import extract_matching_fields
from etl.nlp.resolver import resolve_document
from etl.nlp.validator import validate


logger = logging.getLogger(__name__)


_SELECT_PENDING_BASE = (
    "SELECT s.id, s.url_bases_reguladoras, s.documentos "
    "FROM l0.nacional_subvenciones s"
)


# -----------------------------------------------------------------------------
# Hito 3.2.a — Cancelación cooperativa SIGTERM
# -----------------------------------------------------------------------------
# El endpoint POST /nlp/runs/{id}/cancel envía SIGTERM al subprocess. El handler
# instalado en run_batch setea un flag global y el bucle principal lo chequea
# entre items para romper limpiamente y finalizar la fila con status='cancelled'.

_cancel_requested = False


def request_cancel() -> None:
    """Marca el flag global de cancelación. Idempotente."""
    global _cancel_requested
    _cancel_requested = True


def is_cancel_requested() -> bool:
    return _cancel_requested


def _reset_cancel_flag() -> None:
    global _cancel_requested
    _cancel_requested = False


def _install_sigterm_handler() -> None:
    """Registra handler de SIGTERM que setea el flag de cancelación.

    No reemplaza el handler en Windows (señal no soportada con la misma semántica).
    """
    if os.name == "nt":
        return

    def _handler(signum, frame):  # noqa: ARG001 — firma estándar de signal handlers.
        request_cancel()
        logger.warning("[nlp] SIGTERM received — cancel after current item")

    signal.signal(signal.SIGTERM, _handler)


def _validate_selector_args(
    anos: Optional[tuple[int, int]],
    meses: Optional[tuple[int, int]],
    codigo_bdns: Optional[int],
    todo: bool,
) -> None:
    """Reglas del contrato WP2.1.1; lanza ValueError si la combinación es inválida.

    Defense in depth: el CLI valida antes pero la API y usos programáticos
    también pasan por aquí.
    """
    selectors = [anos is not None, codigo_bdns is not None, bool(todo)]
    n = sum(selectors)
    if n == 0:
        raise ValueError(
            "Falta selector: indique uno y solo uno de --anos, --codigo-bdns o --todo."
        )
    if n > 1:
        raise ValueError(
            "Selectores mutual exclusion: --anos, --codigo-bdns y --todo son mutuamente excluyentes."
        )
    if meses is not None and anos is None:
        raise ValueError("--meses solo es válido junto a --anos.")
    if anos is not None:
        a, b = anos
        if a > b:
            raise ValueError(f"--anos: el inicio ({a}) no puede ser mayor que el fin ({b}).")
    if meses is not None:
        m1, m2 = meses
        if not (1 <= m1 <= 12 and 1 <= m2 <= 12):
            raise ValueError(f"--meses fuera de rango [1,12]: {m1}-{m2}.")
        if m1 > m2:
            raise ValueError(f"--meses: el inicio ({m1}) no puede ser mayor que el fin ({m2}).")


def _build_select_pending_sql(
    *,
    anos: Optional[tuple[int, int]],
    meses: Optional[tuple[int, int]],
    codigo_bdns: Optional[int],
    todo: bool,
    limit: Optional[int],
) -> tuple[str, dict]:
    """Construye el SELECT dinámico según el selector activo.

    Reglas del contrato:
    - anos        : filtra fecha_recepcion entre [start-01-01, end-12-31]; excluye NULLs.
    - anos+meses  : AND adicional EXTRACT(MONTH FROM fecha_recepcion) BETWEEN m1 AND m2.
    - codigo_bdns : id = N; quita el filtro nlp_document_key IS NULL para permitir reanálisis.
    - todo        : sin filtro de fecha; conserva nlp_document_key IS NULL y la condición de doc.
    - limit==0 o None: omite la cláusula LIMIT.
    """
    where: list[str] = []
    params: dict = {}

    if codigo_bdns is not None:
        where.append("s.id = %(codigo_bdns)s")
        params["codigo_bdns"] = codigo_bdns
    else:
        # anos / todo: pendientes y con documento resoluble.
        where.append("s.nlp_document_key IS NULL")
        where.append("(s.url_bases_reguladoras IS NOT NULL OR s.documentos IS NOT NULL)")
        if anos is not None:
            a, b = anos
            where.append("s.fecha_recepcion >= %(fecha_desde)s")
            where.append("s.fecha_recepcion <= %(fecha_hasta)s")
            params["fecha_desde"] = f"{a:04d}-01-01"
            params["fecha_hasta"] = f"{b:04d}-12-31"
            if meses is not None:
                m1, m2 = meses
                where.append(
                    "EXTRACT(MONTH FROM s.fecha_recepcion) BETWEEN %(mes_desde)s AND %(mes_hasta)s"
                )
                params["mes_desde"] = m1
                params["mes_hasta"] = m2

    sql = _SELECT_PENDING_BASE
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY s.id DESC"
    if limit is not None and limit > 0:
        sql += " LIMIT %(limit)s"
        params["limit"] = limit
    return sql, params


LOG_LLM_SQL = """
INSERT INTO ops.llm_bases_reguladoras_logs
  (document_key, llm_model, input_tokens, output_tokens, duration_ms,
   validation_status, cost_usd, run_id)
VALUES (%(document_key)s, %(llm_model)s, %(input_tokens)s, %(output_tokens)s,
        %(duration_ms)s, %(validation_status)s, %(cost_usd)s, %(run_id)s)
"""


def compute_cost_usd(
    conn,
    *,
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    at_date: Optional[date] = None,
) -> Optional[Decimal]:
    """Lee ops.llm_pricing y calcula coste USD para tokens dados.

    - Toma la tarifa cuyo intervalo ``[valid_from, valid_to)`` contiene ``at_date``
      (default ``date.today()``). Si no hay tarifa vigente, devuelve ``None``
      (compatibilidad hacia atrás antes del seed).
    - Cuando hay varias tarifas históricas vigentes en la misma fecha (overlap),
      se queda con la del ``valid_from`` más reciente.
    - Devuelve un :class:`decimal.Decimal` (precisión 6) para evitar drift float
      en agregados monetarios.
    """
    at = at_date or date.today()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT input_per_1m_usd, output_per_1m_usd
            FROM ops.llm_pricing
            WHERE provider = %s AND model = %s
              AND valid_from <= %s
              AND (valid_to IS NULL OR valid_to > %s)
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            (provider, model, at, at),
        )
        row = cur.fetchone()
    if row is None:
        return None
    in_price, out_price = row
    cost = (
        Decimal(input_tokens) / Decimal(1_000_000)
    ) * in_price + (
        Decimal(output_tokens) / Decimal(1_000_000)
    ) * out_price
    return cost.quantize(Decimal("0.000001"))


LOG_FAILURE_SQL = """
INSERT INTO ops.nlp_failures
  (subvencion_id, document_source, document_ref, llm_model, raw_snippet, error_message)
VALUES (%(subvencion_id)s, %(document_source)s, %(document_ref)s, %(llm_model)s,
        %(raw_snippet)s, %(error_message)s)
"""


@dataclass
class PlanItem:
    subvencion_id: int
    document_source: Optional[str]
    document_key: Optional[str]
    document_name: Optional[str]
    heuristic_step: Optional[int]
    document_ref: Optional[str]
    cache_hit: bool
    skipped_no_doc: bool


@dataclass
class BatchStats:
    processed: int = 0
    valid: int = 0
    partial: int = 0
    invalid: int = 0
    skipped_no_doc: int = 0
    dedup_hits: int = 0
    duration_seconds: float = 0.0
    planned: list[PlanItem] = field(default_factory=list)
    # Hito 3.1.d — id de la fila en ops.nlp_runs creada/actualizada por este batch.
    run_id: Optional[int] = None


# -----------------------------------------------------------------------------
# Hito 3.1.d — Helpers para tracking en ops.nlp_runs
# -----------------------------------------------------------------------------


def _selector_metadata(
    *,
    anos: Optional[tuple[int, int]],
    meses: Optional[tuple[int, int]],
    codigo_bdns: Optional[int],
    todo: bool,
) -> tuple[str, dict]:
    """Deriva (selector_kind, selector_value JSONB) para ops.nlp_runs.

    Asume que ``_validate_selector_args`` ya garantizó mutual exclusion.
    """
    if anos is not None:
        value: dict = {"anos": [anos[0], anos[1]]}
        if meses is not None:
            value["meses"] = [meses[0], meses[1]]
        return "anos", value
    if codigo_bdns is not None:
        return "codigo_bdns", {"codigo_bdns": codigo_bdns}
    return "todo", {}


def _create_nlp_run(
    conn,
    *,
    selector_kind: str,
    selector_value: dict,
    limit_value: Optional[int],
    dry_run: bool,
    force: bool,
    llm_model: Optional[str],
    pid: Optional[int],
) -> int:
    """Inserta fila inicial en ops.nlp_runs y devuelve run_id.

    Usa transacción corta (commit) para que `select * from ops.nlp_runs` muestre
    el run incluso aunque la conexión actual entre en una transacción larga.
    """
    import json as _json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.nlp_runs
              (selector_kind, selector_value, limit_value, dry_run, force, llm_model, pid)
            VALUES (%s, %s::jsonb, %s, %s, %s, %s, %s)
            RETURNING run_id
            """,
            (
                selector_kind,
                _json.dumps(selector_value),
                limit_value,
                dry_run,
                force,
                llm_model,
                pid,
            ),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _bump_nlp_run(conn, run_id: int, **counters) -> None:
    """Incrementa counters en ops.nlp_runs con commit corto.

    Acepta cualquier subconjunto de columnas: items_processed, items_valid,
    items_partial, items_invalid, items_skipped, items_cache_hits,
    cost_usd_total, input_tokens_total, output_tokens_total. items_planned
    se asigna en absoluto (set), no incremental.
    """
    if not counters:
        return
    sets = []
    params: list = []
    for col, delta in counters.items():
        if col == "items_planned":
            sets.append(f"{col} = %s")
            params.append(delta)
        else:
            sets.append(f"{col} = {col} + %s")
            params.append(delta)
    params.append(run_id)
    sql = f"UPDATE ops.nlp_runs SET {', '.join(sets)} WHERE run_id = %s"
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def _finalize_nlp_run(
    conn,
    run_id: int,
    *,
    status: str,
    error: Optional[str] = None,
) -> None:
    """Cierra la fila en ops.nlp_runs con status final y finished_at."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops.nlp_runs
               SET status = %s,
                   finished_at = NOW(),
                   error_message = COALESCE(%s, error_message)
             WHERE run_id = %s
            """,
            (status, error, run_id),
        )
    conn.commit()


def select_pending(
    conn,
    *,
    limit: Optional[int],
    anos: Optional[tuple[int, int]] = None,
    meses: Optional[tuple[int, int]] = None,
    codigo_bdns: Optional[int] = None,
    todo: bool = False,
) -> list[tuple]:
    """Helper testeable: devuelve filas pendientes ordenadas por id DESC.

    El selector (anos/codigo_bdns/todo) se valida en :func:`run_batch`; este helper
    se limita a construir y ejecutar el SQL para que los tests puedan ejercitar
    los filtros aisladamente.
    """
    sql, params = _build_select_pending_sql(
        anos=anos, meses=meses, codigo_bdns=codigo_bdns, todo=todo, limit=limit
    )
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def count_pending(
    conn,
    *,
    anos: Optional[tuple[int, int]] = None,
    meses: Optional[tuple[int, int]] = None,
    codigo_bdns: Optional[int] = None,
    todo: bool = False,
) -> int:
    """Cuenta filas pendientes con la misma semántica que :func:`select_pending` (sin LIMIT)."""
    sql, params = _build_select_pending_sql(
        anos=anos, meses=meses, codigo_bdns=codigo_bdns, todo=todo, limit=None
    )
    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _pending"
    with conn.cursor() as cur:
        cur.execute(count_sql, params)
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _check_cache_hit(conn, document_key: str) -> bool:
    """Lookup ligero: True si subvenciones_nlp.document_key existe con valid/partial."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM l0.subvenciones_nlp "
            "WHERE document_key = %s AND validation_status IN ('valid','partial') LIMIT 1",
            (document_key,),
        )
        return cur.fetchone() is not None


def run_batch(
    conn,
    *,
    limit: int = 100,
    force: bool = False,
    dry_run: bool = False,
    anos: Optional[tuple[int, int]] = None,
    meses: Optional[tuple[int, int]] = None,
    codigo_bdns: Optional[int] = None,
    todo: bool = False,
) -> BatchStats:
    """Procesa pendientes ordenados por id DESC.

    Selector obligatorio (uno y solo uno): ``anos``, ``codigo_bdns`` o ``todo``.
    ``meses`` solo es válido junto a ``anos``. ``limit==0`` desactiva el cap.
    Con ``codigo_bdns`` el ``limit`` se ignora a nivel de SQL (solo 1 fila como mucho).

    Lanza :class:`ValueError` ante combinaciones inválidas (defense in depth).
    """
    _validate_selector_args(anos, meses, codigo_bdns, todo)
    _reset_cancel_flag()
    _install_sigterm_handler()
    started = time.time()
    stats = BatchStats()
    schema = None if dry_run else load_schema()

    # ---- ops.nlp_runs lifecycle (Hito 3.1.d) -------------------------------
    selector_kind, selector_value = _selector_metadata(
        anos=anos, meses=meses, codigo_bdns=codigo_bdns, todo=todo
    )
    env_run_id = os.environ.get("NLP_RUN_ID")
    if env_run_id:
        # El endpoint POST /nlp/analizar crea la fila antes del Popen y nos pasa
        # el id por env var para evitar la race contra el polling de la UI.
        try:
            run_id: Optional[int] = int(env_run_id)
        except ValueError:
            run_id = None
    else:
        run_id = None
    if run_id is None:
        run_id = _create_nlp_run(
            conn,
            selector_kind=selector_kind,
            selector_value=selector_value,
            limit_value=limit if limit and limit > 0 else None,
            dry_run=dry_run,
            force=force,
            llm_model=os.environ.get("NLP_LLM_MODEL"),
            pid=os.getpid(),
        )
    stats.run_id = run_id

    try:
        effective_limit: Optional[int] = None if codigo_bdns is not None else limit
        rows = select_pending(
            conn,
            limit=effective_limit,
            anos=anos,
            meses=meses,
            codigo_bdns=codigo_bdns,
            todo=todo,
        )
        _bump_nlp_run(conn, run_id, items_planned=len(rows))
        if anos is not None:
            selector_repr = f"anos={anos[0]}-{anos[1]}" + (
                f" meses={meses[0]}-{meses[1]}" if meses is not None else ""
            )
        elif codigo_bdns is not None:
            selector_repr = f"codigo_bdns={codigo_bdns}"
        else:
            selector_repr = "todo=true"
        logger.info(
            "[nlp] start run_id=%d limit=%s selector=%s force=%s dry_run=%s pending_in_chunk=%d",
            run_id,
            limit if limit and limit > 0 else "off",
            selector_repr,
            force,
            dry_run,
            len(rows),
        )

        cancelled = False
        for row in rows:
            if is_cancel_requested():
                cancelled = True
                logger.warning(
                    "[nlp] cancel requested — breaking loop run_id=%d processed=%d",
                    run_id,
                    stats.processed,
                )
                break
            subv_id, url, documentos = row
            resolved = resolve_document(
                url_bases_reguladoras=url,
                documentos=documentos,
            )
            if resolved is None:
                stats.skipped_no_doc += 1
                logger.info("[nlp] id=%d skipped_no_doc=true", subv_id)
                if not dry_run:
                    _bump_nlp_run(conn, run_id, items_skipped=1)
                if dry_run:
                    stats.planned.append(
                        PlanItem(
                            subvencion_id=subv_id,
                            document_source=None,
                            document_key=None,
                            document_name=None,
                            heuristic_step=None,
                            document_ref=None,
                            cache_hit=False,
                            skipped_no_doc=True,
                        )
                    )
                continue

            cache_hit = (not force) and _check_cache_hit(conn, resolved.document_key)
            log_name_part = f" name='{resolved.document_name}'" if resolved.document_name else ""
            logger.info(
                "[nlp] id=%d step=%d source=%s%s ref=%s key=%s%s",
                subv_id,
                resolved.heuristic_step,
                resolved.document_source,
                log_name_part,
                resolved.document_ref or "-",
                resolved.document_key,
                " cache_hit=true (--force=false → skip LLM)" if cache_hit else "",
            )

            if dry_run:
                stats.planned.append(
                    PlanItem(
                        subvencion_id=subv_id,
                        document_source=resolved.document_source,
                        document_key=resolved.document_key,
                        document_name=resolved.document_name,
                        heuristic_step=resolved.heuristic_step,
                        document_ref=resolved.document_ref,
                        cache_hit=cache_hit,
                        skipped_no_doc=False,
                    )
                )
                continue

            if cache_hit:
                propagate_from_cache(
                    conn, subvencion_id=subv_id, document_key=resolved.document_key
                )
                stats.dedup_hits += 1
                _bump_nlp_run(conn, run_id, items_cache_hits=1)
                logger.info("[nlp] id=%d step=cache propagated=true", subv_id)
                continue

            try:
                extracted = extract_from_url(resolved.document_ref)
                logger.info(
                    "[nlp] id=%d step=extract chars=%d content_type=%s mode=%s",
                    subv_id,
                    extracted.char_count,
                    extracted.content_type or "-",
                    extracted.extraction_mode,
                )
            except ExtractionError as e:
                stats.invalid += 1
                logger.error("[nlp] id=%d step=extract error=%s", subv_id, e)
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            LOG_FAILURE_SQL,
                            {
                                "subvencion_id": subv_id,
                                "document_source": resolved.document_source,
                                "document_ref": resolved.document_ref,
                                "llm_model": None,
                                "raw_snippet": None,
                                "error_message": f"extraction: {e}",
                            },
                        )
                _bump_nlp_run(conn, run_id, items_invalid=1)
                continue

            try:
                llm_result = analyze_document(extracted.text, schema)
                logger.info(
                    "[nlp] id=%d step=llm model=%s tokens_in=%d tokens_out=%d duration_ms=%d",
                    subv_id,
                    llm_result.model,
                    llm_result.input_tokens,
                    llm_result.output_tokens,
                    llm_result.duration_ms,
                )
            except Exception as e:
                stats.invalid += 1
                logger.error("[nlp] id=%d step=llm error=%s", subv_id, e)
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            LOG_FAILURE_SQL,
                            {
                                "subvencion_id": subv_id,
                                "document_source": resolved.document_source,
                                "document_ref": resolved.document_ref,
                                "llm_model": None,
                                "raw_snippet": None,
                                "error_message": f"llm: {e}",
                            },
                        )
                _bump_nlp_run(conn, run_id, items_invalid=1)
                continue

            validation = validate(llm_result.raw_text)
            fields = (
                extract_matching_fields(validation.model)
                if validation.model and validation.status in ("valid", "partial")
                else None
            )
            nlp_json = validation.raw_dict or {}
            logger.info(
                "[nlp] id=%d step=validate status=%s errors=%d",
                subv_id,
                validation.status,
                len(validation.errors),
            )

            cost = compute_cost_usd(
                conn,
                provider=os.environ.get("NLP_LLM_PROVIDER", "openai"),
                model=llm_result.model,
                input_tokens=llm_result.input_tokens,
                output_tokens=llm_result.output_tokens,
            )

            if validation.status == "invalid":
                stats.invalid += 1
                logger.error(
                    "[nlp] id=%d step=validate invalid errors=%s",
                    subv_id,
                    "; ".join(f"{e.path}: {e.message}" for e in validation.errors),
                )
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            LOG_FAILURE_SQL,
                            {
                                "subvencion_id": subv_id,
                                "document_source": resolved.document_source,
                                "document_ref": resolved.document_ref,
                                "llm_model": llm_result.model,
                                "raw_snippet": llm_result.raw_text[:500],
                                "error_message": "; ".join(
                                    f"{e.path}: {e.message}" for e in validation.errors
                                ),
                            },
                        )
                        cur.execute(
                            LOG_LLM_SQL,
                            {
                                "document_key": resolved.document_key,
                                "llm_model": llm_result.model,
                                "input_tokens": llm_result.input_tokens,
                                "output_tokens": llm_result.output_tokens,
                                "duration_ms": llm_result.duration_ms,
                                "validation_status": "invalid",
                                "cost_usd": cost,
                                "run_id": run_id,
                            },
                        )
                _bump_nlp_run(
                    conn,
                    run_id,
                    items_invalid=1,
                    cost_usd_total=cost or Decimal("0"),
                    input_tokens_total=llm_result.input_tokens,
                    output_tokens_total=llm_result.output_tokens,
                )
                continue

            persist_analysis(
                conn,
                subvencion_id=subv_id,
                resolved=resolved,
                validation=validation,
                fields=fields,
                nlp_json=nlp_json,
                modelo_documental=None,
                input_char_count=extracted.char_count,
                llm_model=llm_result.model,
                extracted_at=datetime.now(timezone.utc),
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        LOG_LLM_SQL,
                        {
                            "document_key": resolved.document_key,
                            "llm_model": llm_result.model,
                            "input_tokens": llm_result.input_tokens,
                            "output_tokens": llm_result.output_tokens,
                            "duration_ms": llm_result.duration_ms,
                            "validation_status": validation.status,
                            "cost_usd": cost,
                            "run_id": run_id,
                        },
                    )

            if validation.status == "valid":
                stats.valid += 1
                _bump_nlp_run(
                    conn,
                    run_id,
                    items_processed=1,
                    items_valid=1,
                    cost_usd_total=cost or Decimal("0"),
                    input_tokens_total=llm_result.input_tokens,
                    output_tokens_total=llm_result.output_tokens,
                )
            elif validation.status == "partial":
                stats.partial += 1
                _bump_nlp_run(
                    conn,
                    run_id,
                    items_processed=1,
                    items_partial=1,
                    cost_usd_total=cost or Decimal("0"),
                    input_tokens_total=llm_result.input_tokens,
                    output_tokens_total=llm_result.output_tokens,
                )
            stats.processed += 1
            logger.info("[nlp] id=%d step=persist status=%s", subv_id, validation.status)

        stats.duration_seconds = time.time() - started
        final_status = "cancelled" if cancelled else "ok"
        logger.info(
            "[nlp] done run_id=%d status=%s processed=%d valid=%d partial=%d invalid=%d "
            "skipped_no_doc=%d dedup_hits=%d duration=%.1fs",
            run_id,
            final_status,
            stats.processed,
            stats.valid,
            stats.partial,
            stats.invalid,
            stats.skipped_no_doc,
            stats.dedup_hits,
            stats.duration_seconds,
        )
        _finalize_nlp_run(
            conn,
            run_id,
            status=final_status,
            error="cancelled by user (SIGTERM)" if cancelled else None,
        )
        return stats
    except Exception as exc:
        try:
            _finalize_nlp_run(conn, run_id, status="failed", error=str(exc))
        except Exception:
            # No enmascarar la excepción original si la finalización también falla.
            logger.exception("[nlp] failed to finalize run_id=%d", run_id)
        raise
