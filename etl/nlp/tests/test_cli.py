"""Tests del CLI ``licitia-etl nlp analizar`` (WP2.1.1).

El parser de rangos y las validaciones cruzadas se ejercitan sin tocar la BBDD:
- ``_parse_range`` se prueba directamente.
- ``cmd_analizar`` se invoca con argparse.Namespace para verificar guardraíles
  ANTES de abrir conexión a Postgres (los tests no esperan DB).
"""
from __future__ import annotations

import argparse

import pytest


# -----------------------------------------------------------------------------
# _parse_range — helper de rangos cerrados
# -----------------------------------------------------------------------------


def test_parse_range_valid_xy():
    from etl.nlp.cli import _parse_range

    assert _parse_range("2024-2026", min_val=2000, max_val=2100, name="anos") == (2024, 2026)


def test_parse_range_valid_single_x():
    """Un solo valor X se interpreta como (X, X)."""
    from etl.nlp.cli import _parse_range

    assert _parse_range("2024", min_val=2000, max_val=2100, name="anos") == (2024, 2024)


def test_parse_range_invalid_format_open():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("2024-", min_val=2000, max_val=2100, name="anos")


def test_parse_range_invalid_format_alpha():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("abc", min_val=2000, max_val=2100, name="anos")


def test_parse_range_invalid_start_greater_than_end():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("2024-2020", min_val=2000, max_val=2100, name="anos")


def test_parse_range_meses_out_of_bounds_low():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("0-12", min_val=1, max_val=12, name="meses")


def test_parse_range_meses_out_of_bounds_high():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("1-13", min_val=1, max_val=12, name="meses")


def test_parse_range_meses_inverted():
    from etl.nlp.cli import _parse_range

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_range("5-3", min_val=1, max_val=12, name="meses")


# -----------------------------------------------------------------------------
# Construcción del subparser y propagación de args
# -----------------------------------------------------------------------------


def _build_analizar_parser():
    """Construye un parser raíz con el subcomando ``nlp analizar`` montado."""
    from etl.nlp.cli import add_subparser

    root = argparse.ArgumentParser(prog="licitia-etl-test")
    sub = root.add_subparsers(dest="command")
    add_subparser(sub)
    return root


def test_cli_codigo_bdns_only():
    parser = _build_analizar_parser()
    args = parser.parse_args(["nlp", "analizar", "--codigo-bdns", "12345"])
    assert args.codigo_bdns == 12345
    assert args.anos is None
    assert args.todo is False


def test_cli_anos_meses_ok():
    parser = _build_analizar_parser()
    args = parser.parse_args(
        ["nlp", "analizar", "--anos", "2024-2025", "--meses", "3-6"]
    )
    assert args.anos == (2024, 2025)
    assert args.meses == (3, 6)


def test_cli_todo_ok():
    parser = _build_analizar_parser()
    args = parser.parse_args(["nlp", "analizar", "--todo"])
    assert args.todo is True
    assert args.anos is None
    assert args.codigo_bdns is None


# -----------------------------------------------------------------------------
# Guardraíles cruzados en cmd_analizar (validación ANTES de abrir conexión BBDD)
# -----------------------------------------------------------------------------


def _make_args(**overrides) -> argparse.Namespace:
    """Construye un Namespace con los defaults del subparser."""
    base = dict(
        limit=100,
        force=False,
        dry_run=False,
        anos=None,
        meses=None,
        codigo_bdns=None,
        todo=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_cli_requires_selector(capsys):
    """Sin --anos/--codigo-bdns/--todo → exit_code != 0 antes de tocar la BBDD."""
    from etl.nlp.cli import cmd_analizar

    rc = cmd_analizar(_make_args(limit=5))
    assert rc != 0
    err = capsys.readouterr().err.lower()
    assert "selector" in err or "anos" in err


def test_cli_meses_requires_anos(capsys):
    from etl.nlp.cli import cmd_analizar

    rc = cmd_analizar(_make_args(meses=(3, 6), todo=True))
    assert rc != 0
    assert "meses" in capsys.readouterr().err.lower()


def test_cli_mutual_exclusion_anos_todo(capsys):
    from etl.nlp.cli import cmd_analizar

    rc = cmd_analizar(_make_args(anos=(2024, 2024), todo=True))
    assert rc != 0
    err = capsys.readouterr().err.lower()
    assert "mutual" in err or "exclus" in err


def test_cli_mutual_exclusion_anos_codigo_bdns(capsys):
    from etl.nlp.cli import cmd_analizar

    rc = cmd_analizar(_make_args(anos=(2024, 2024), codigo_bdns=1))
    assert rc != 0
    err = capsys.readouterr().err.lower()
    assert "mutual" in err or "exclus" in err
