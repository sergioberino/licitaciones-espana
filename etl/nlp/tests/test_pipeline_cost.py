"""Hito 3.1.c — Cálculo de cost_usd contra ops.llm_pricing.

`compute_cost_usd(conn, provider, model, input_tokens, output_tokens, at_date)`
lee la tarifa vigente y devuelve un Decimal (o None si no hay tarifa en ese
momento). Decimales para evitar drift float en cuentas monetarias.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest


_PROV = "openai"
_MODEL = "test-model-x"


def _cleanup(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ops.llm_pricing WHERE provider = %s AND model = %s",
            (_PROV, _MODEL),
        )
    conn.commit()


def test_compute_cost_with_vigente_pricing(pg_conn):
    """Tarifa vigente → cost = (in/1M)*in_price + (out/1M)*out_price (Decimal)."""
    from etl.nlp.pipeline import compute_cost_usd

    _cleanup(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from)
                VALUES (%s, %s, 0.200000, 1.250000, '2026-01-01')
                """,
                (_PROV, _MODEL),
            )
        pg_conn.commit()

        cost = compute_cost_usd(
            pg_conn,
            provider=_PROV,
            model=_MODEL,
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            at_date=date(2026, 5, 25),
        )
        assert cost is not None
        assert isinstance(cost, Decimal)
        # 1M * 0.20 + 1M * 1.25 = 1.45 USD
        assert cost == Decimal("1.450000")
    finally:
        _cleanup(pg_conn)


def test_compute_cost_no_pricing_returns_none(pg_conn):
    """Sin tarifa registrada → None (compat con histórico)."""
    from etl.nlp.pipeline import compute_cost_usd

    _cleanup(pg_conn)
    cost = compute_cost_usd(
        pg_conn,
        provider=_PROV,
        model=_MODEL,
        input_tokens=10_000,
        output_tokens=5_000,
        at_date=date(2026, 5, 25),
    )
    assert cost is None


def test_compute_cost_expired_returns_none(pg_conn):
    """Tarifa con valid_to en el pasado → None para fechas posteriores."""
    from etl.nlp.pipeline import compute_cost_usd

    _cleanup(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from, valid_to)
                VALUES (%s, %s, 0.500000, 2.000000, '2024-01-01', '2025-01-01')
                """,
                (_PROV, _MODEL),
            )
        pg_conn.commit()

        cost = compute_cost_usd(
            pg_conn,
            provider=_PROV,
            model=_MODEL,
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            at_date=date(2026, 5, 25),
        )
        assert cost is None
    finally:
        _cleanup(pg_conn)


def test_compute_cost_picks_most_recent_valid_from(pg_conn):
    """Si hay varias tarifas vigentes, usa la del valid_from más reciente."""
    from etl.nlp.pipeline import compute_cost_usd

    _cleanup(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            # Tarifa antigua con valid_to (cerrada).
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from, valid_to)
                VALUES (%s, %s, 1.000000, 5.000000, '2024-01-01', '2025-06-01')
                """,
                (_PROV, _MODEL),
            )
            # Tarifa actual (abierta).
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from)
                VALUES (%s, %s, 0.200000, 1.250000, '2025-06-01')
                """,
                (_PROV, _MODEL),
            )
        pg_conn.commit()

        cost = compute_cost_usd(
            pg_conn,
            provider=_PROV,
            model=_MODEL,
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            at_date=date(2026, 5, 25),
        )
        # Debe usar la más reciente: 1M*0.20 + 1M*1.25 = 1.45.
        assert cost == Decimal("1.450000")
    finally:
        _cleanup(pg_conn)


def test_compute_cost_at_specific_date(pg_conn):
    """Con at_date dentro del rango histórico → usa la tarifa antigua."""
    from etl.nlp.pipeline import compute_cost_usd

    _cleanup(pg_conn)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from, valid_to)
                VALUES (%s, %s, 1.000000, 5.000000, '2024-01-01', '2025-06-01')
                """,
                (_PROV, _MODEL),
            )
            cur.execute(
                """
                INSERT INTO ops.llm_pricing
                  (provider, model, input_per_1m_usd, output_per_1m_usd, valid_from)
                VALUES (%s, %s, 0.200000, 1.250000, '2025-06-01')
                """,
                (_PROV, _MODEL),
            )
        pg_conn.commit()

        cost = compute_cost_usd(
            pg_conn,
            provider=_PROV,
            model=_MODEL,
            input_tokens=1_000_000,
            output_tokens=1_000_000,
            at_date=date(2024, 6, 1),
        )
        # Tarifa antigua: 1M*1.0 + 1M*5.0 = 6.0.
        assert cost == Decimal("6.000000")
    finally:
        _cleanup(pg_conn)
