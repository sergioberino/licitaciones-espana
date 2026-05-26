"""Estimación de count/coste/duración para modo todo (Hito 3.2.b)."""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Optional

from etl.nlp.pipeline import compute_cost_usd, count_pending

_HISTORY_SAMPLE_LIMIT = 50
_MIN_HISTORY_SAMPLES = 5


def _history_averages(conn) -> dict[str, Optional[float]]:
    """Promedios de los últimos logs valid/partial (hasta 50 filas)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              AVG(input_tokens)::float   AS avg_input_tokens,
              AVG(output_tokens)::float  AS avg_output_tokens,
              AVG(duration_ms)::float     AS avg_duration_ms,
              COUNT(*)::int               AS runs_sampled
            FROM (
              SELECT input_tokens, output_tokens, duration_ms
              FROM ops.llm_bases_reguladoras_logs
              WHERE validation_status IN ('valid', 'partial')
              ORDER BY created_at DESC
              LIMIT %s
            ) recent
            """,
            (_HISTORY_SAMPLE_LIMIT,),
        )
        row = cur.fetchone()
    if row is None:
        return {
            "runs_sampled": 0,
            "avg_input_tokens": None,
            "avg_output_tokens": None,
            "avg_duration_ms": None,
        }
    return {
        "runs_sampled": int(row[3] or 0),
        "avg_input_tokens": row[0],
        "avg_output_tokens": row[1],
        "avg_duration_ms": row[2],
    }


def _pricing_label(conn, *, provider: str, model: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT model
            FROM ops.llm_pricing
            WHERE provider = %s AND model = %s
              AND valid_from <= CURRENT_DATE
              AND (valid_to IS NULL OR valid_to > CURRENT_DATE)
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            (provider, model),
        )
        row = cur.fetchone()
    if row:
        return f"{provider}/{model} (vigente)"
    return f"{provider}/{model}"


def estimate_nlp_run(
    conn,
    *,
    todo: bool = False,
    anos: Optional[tuple[int, int]] = None,
    meses: Optional[tuple[int, int]] = None,
    codigo_bdns: Optional[int] = None,
) -> dict[str, Any]:
    """Estima items_count, coste USD y duración para un selector NLP.

    v1: solo ``todo=True`` está soportado en la API; los demás kwargs existen
    para reutilizar ``count_pending`` en futuros modos.
    """
    items_count = count_pending(
        conn, anos=anos, meses=meses, codigo_bdns=codigo_bdns, todo=todo
    )
    hist = _history_averages(conn)
    runs_sampled = hist["runs_sampled"]
    avg_in = hist["avg_input_tokens"] or 0.0
    avg_out = hist["avg_output_tokens"] or 0.0
    avg_ms = hist["avg_duration_ms"] or 0.0

    provider = os.environ.get("NLP_LLM_PROVIDER", "openai")
    model = os.environ.get("NLP_LLM_MODEL", "gpt-5.4-nano")
    pricing_source = _pricing_label(conn, provider=provider, model=model)

    warning: Optional[str] = None
    cost_estimate_usd: Optional[str] = None

    if runs_sampled < _MIN_HISTORY_SAMPLES:
        warning = "Insufficient historical data for cost estimate"
    else:
        unit_cost = compute_cost_usd(
            conn,
            provider=provider,
            model=model,
            input_tokens=int(round(avg_in)),
            output_tokens=int(round(avg_out)),
        )
        if unit_cost is None:
            warning = "Insufficient historical data for cost estimate"
        else:
            total = (unit_cost * Decimal(items_count)).quantize(Decimal("0.000001"))
            cost_estimate_usd = str(total)

    duration_estimate_seconds = int(items_count * avg_ms / 1000) if avg_ms else 0

    result: dict[str, Any] = {
        "items_count": items_count,
        "cost_estimate_usd": cost_estimate_usd,
        "duration_estimate_seconds": duration_estimate_seconds,
        "based_on": {
            "runs_sampled": runs_sampled,
            "avg_input_tokens": int(round(avg_in)) if runs_sampled else 0,
            "avg_output_tokens": int(round(avg_out)) if runs_sampled else 0,
            "avg_duration_ms": int(round(avg_ms)) if runs_sampled else 0,
            "pricing_source": pricing_source,
        },
    }
    if warning:
        result["warning"] = warning
    return result
