"""Tests para estimate_nlp_run (Hito 3.2.b)."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch


def test_estimate_todo_returns_count(pg_conn):
    from etl.nlp.estimate import estimate_nlp_run

    result = estimate_nlp_run(pg_conn, todo=True)
    assert "items_count" in result
    assert result["items_count"] >= 0
    assert "based_on" in result
    assert "duration_estimate_seconds" in result


def test_estimate_insufficient_history_null_cost(pg_conn, monkeypatch):
    from etl.nlp import estimate as estimate_mod

    monkeypatch.setattr(
        estimate_mod,
        "_history_averages",
        lambda conn: {
            "runs_sampled": 2,
            "avg_input_tokens": 1000,
            "avg_output_tokens": 500,
            "avg_duration_ms": 10000,
        },
    )
    monkeypatch.setattr(estimate_mod, "count_pending", lambda conn, **kw: 42)

    result = estimate_mod.estimate_nlp_run(pg_conn, todo=True)
    assert result["items_count"] == 42
    assert result["cost_estimate_usd"] is None
    assert result.get("warning") == "Insufficient historical data for cost estimate"


def test_estimate_with_history_computes_cost(pg_conn, monkeypatch):
    from etl.nlp import estimate as estimate_mod

    monkeypatch.setattr(
        estimate_mod,
        "_history_averages",
        lambda conn: {
            "runs_sampled": 10,
            "avg_input_tokens": 4521.0,
            "avg_output_tokens": 1834.0,
            "avg_duration_ms": 12450.0,
        },
    )
    monkeypatch.setattr(estimate_mod, "count_pending", lambda conn, **kw: 100)

    def _fake_cost(conn, *, provider, model, input_tokens, output_tokens, at_date=None):
        return Decimal("0.008120")

    monkeypatch.setattr(estimate_mod, "compute_cost_usd", _fake_cost)

    result = estimate_mod.estimate_nlp_run(pg_conn, todo=True)
    assert result["items_count"] == 100
    assert result["cost_estimate_usd"] == "0.812000"
    assert result["duration_estimate_seconds"] == int(100 * 12450 / 1000)
    assert result.get("warning") is None
    assert result["based_on"]["runs_sampled"] == 10
