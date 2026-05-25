"""CLI ``licitia-etl nlp`` — análisis NLP de bases reguladoras.

WP2.1.1: el subcomando ``analizar`` exige un selector temporal (``--anos``,
``--codigo-bdns`` o ``--todo``), alineado con el patrón nacional. Sigue
existiendo el modificador ``--meses`` (cross-product con ``--anos``), el cap
``--limit`` (0 = sin cap) y los flags ``--force`` / ``--dry-run``.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional

import psycopg2

from etl.nlp.pipeline import _validate_selector_args, run_batch


def _parse_range(s: str, *, min_val: int, max_val: int, name: str) -> tuple[int, int]:
    """Parsea ``X-Y`` (X≤Y) o ``X`` (devuelve ``(X,X)``).

    Valida que ambos extremos estén en ``[min_val, max_val]`` y que el inicio
    no supere al fin. Lanza :class:`argparse.ArgumentTypeError` para integrarse
    con el mensaje de error de argparse.
    """
    raw = (s or "").strip()
    if not raw:
        raise argparse.ArgumentTypeError(f"--{name} no puede estar vacío")
    parts = raw.split("-")
    try:
        if len(parts) == 1:
            v = int(parts[0])
            start, end = v, v
        elif len(parts) == 2:
            a, b = parts[0].strip(), parts[1].strip()
            if not a or not b:
                raise ValueError("extremo vacío")
            start, end = int(a), int(b)
        else:
            raise ValueError("demasiados separadores '-'")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--{name} debe tener formato X o X-Y con números enteros (recibido: '{s}')"
        )
    if start > end:
        raise argparse.ArgumentTypeError(
            f"--{name}: el inicio ({start}) no puede ser mayor que el fin ({end})"
        )
    if start < min_val or end > max_val:
        raise argparse.ArgumentTypeError(
            f"--{name} fuera de rango [{min_val},{max_val}]: {start}-{end}"
        )
    return start, end


def _parse_anos_arg(s: str) -> tuple[int, int]:
    return _parse_range(s, min_val=1900, max_val=2999, name="anos")


def _parse_meses_arg(s: str) -> tuple[int, int]:
    return _parse_range(s, min_val=1, max_val=12, name="meses")


def add_subparser(subparsers: argparse._SubParsersAction):
    sp = subparsers.add_parser("nlp", help="Batch B: análisis NLP de bases reguladoras")
    nlp_sub = sp.add_subparsers(dest="nlp_cmd", required=True)

    analizar = nlp_sub.add_parser(
        "analizar",
        help="Procesa convocatorias pendientes",
        description=(
            "Analiza convocatorias con LLM (Batch B / WP2.1).\n"
            "\n"
            "Selector OBLIGATORIO (uno y solo uno):\n"
            "  --anos X-Y           rango cerrado de años contra fecha_recepcion\n"
            "  --codigo-bdns N      una sola convocatoria por id (PK BIGINT)\n"
            "  --todo               procesa todo el catálogo sin filtro de fecha\n"
            "\n"
            "Modificadores:\n"
            "  --meses N-M          solo válido con --anos; cross-product mes×año\n"
            "  --limit N            cap absoluto (default 100; 0 = sin cap)\n"
            "                       ignorado con --codigo-bdns (siempre 1 fila)\n"
            "  --force              ignora cache; siempre llama al LLM\n"
            "  --dry-run            no llama al LLM ni persiste; muestra plan\n"
            "\n"
            "Por qué selector obligatorio (incluso en debug):\n"
            "  El LLM tiene coste por token y la cardinalidad de "
            "l0.nacional_subvenciones\n"
            "  crece de forma autónoma (scheduler). 'licitia-etl nlp analizar' sin\n"
            "  selector llevaría a procesar las N últimas filas por id DESC sin\n"
            "  control temporal — guardraíl contra gasto inesperado y reprocesos\n"
            "  silenciosos. Si quieres procesar TODO el catálogo, dilo explícito:\n"
            "  '--todo'."
        ),
        epilog=(
            "Ejemplos:\n"
            "  # Muestreo dry-run de las 5 convocatorias más recientes de 2026\n"
            "  licitia-etl nlp analizar --anos 2026-2026 --limit 5 --dry-run\n"
            "\n"
            "  # Análisis real de mar–jun de 2024 y 2025 (cross-product)\n"
            "  licitia-etl nlp analizar --anos 2024-2025 --meses 3-6 --limit 20\n"
            "\n"
            "  # Reanalizar una convocatoria concreta ignorando cache\n"
            "  licitia-etl nlp analizar --codigo-bdns 12345 --force\n"
            "\n"
            "  # Barrido completo del catálogo sin cap (cuidado con el coste)\n"
            "  licitia-etl nlp analizar --todo --limit 0\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    analizar.add_argument(
        "--anos",
        type=_parse_anos_arg,
        default=None,
        metavar="X-Y",
        help="Rango cerrado [start, end] de años contra fecha_recepcion (ej. 2026-2026 = solo 2026).",
    )
    analizar.add_argument(
        "--meses",
        type=_parse_meses_arg,
        default=None,
        metavar="N-M",
        help="Rango cerrado [1..12] de meses (solo válido con --anos; cross-product).",
    )
    analizar.add_argument(
        "--codigo-bdns",
        type=int,
        default=None,
        dest="codigo_bdns",
        metavar="N",
        help="Procesa una sola convocatoria por id BDNS (PK BIGINT). --limit se ignora.",
    )
    analizar.add_argument(
        "--todo",
        action="store_true",
        default=False,
        help="Procesa todo el catálogo sin filtro de fecha.",
    )
    analizar.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Cap absoluto del batch (default 100; 0 = sin cap). Ordena por id DESC.",
    )
    analizar.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Ignora cache; siempre llama al LLM.",
    )
    analizar.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        dest="dry_run",
        help="No llama al LLM ni persiste; muestra plan por item.",
    )
    analizar.set_defaults(func=cmd_analizar)


def _print_validation_error(
    err: ValueError,
    *,
    anos: Optional[tuple[int, int]],
    codigo_bdns: Optional[int],
    todo: bool,
) -> None:
    """Imprime el error de validación con hint accionable a stderr.

    Mantiene la lógica de validación pura en ``_validate_selector_args``
    (compartida con la API) y añade contexto CLI: razón + ejemplo concreto
    de la forma correcta de invocar.
    """
    msg = str(err)
    print(f"[nlp] error: {msg}", file=sys.stderr)

    selectors_set = sum(x is not None for x in (anos, codigo_bdns)) + (1 if todo else 0)
    if selectors_set == 0:
        hint_lines = [
            "",
            "  Selector obligatorio (incluso en debug) — elige uno:",
            "    --anos 2026-2026 [--meses 3-6]   ventana temporal por fecha_recepcion",
            "    --codigo-bdns 12345              una convocatoria concreta",
            "    --todo                           todo el catálogo (¡ojo al coste!)",
            "",
            "  Tip: usa --dry-run para validar el plan sin llamar al LLM.",
        ]
    elif selectors_set > 1:
        hint_lines = [
            "",
            "  Los selectores son mutuamente excluyentes. Ejemplos válidos:",
            "    licitia-etl nlp analizar --anos 2026-2026 --limit 5 --dry-run",
            "    licitia-etl nlp analizar --codigo-bdns 12345",
            "    licitia-etl nlp analizar --todo --limit 0",
        ]
    else:
        # Selector OK pero falla en --meses o rangos.
        hint_lines = [
            "",
            "  Pista: --meses requiere --anos; rangos son cerrados con inicio ≤ fin.",
            "  Ej: --anos 2024-2025 --meses 3-6",
        ]
    for line in hint_lines:
        print(line, file=sys.stderr)


def cmd_analizar(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )

    anos: Optional[tuple[int, int]] = getattr(args, "anos", None)
    meses: Optional[tuple[int, int]] = getattr(args, "meses", None)
    codigo_bdns: Optional[int] = getattr(args, "codigo_bdns", None)
    todo: bool = bool(getattr(args, "todo", False))
    limit: int = int(getattr(args, "limit", 100))
    force: bool = bool(getattr(args, "force", False))
    dry_run: bool = bool(getattr(args, "dry_run", False))

    # Guardraíles ANTES de abrir conexión a BBDD para fail-fast con exit_code=2.
    try:
        _validate_selector_args(anos, meses, codigo_bdns, todo)
    except ValueError as e:
        _print_validation_error(e, anos=anos, codigo_bdns=codigo_bdns, todo=todo)
        return 2

    if codigo_bdns is not None and limit != 100:
        logging.warning("[nlp][warn] --limit ignored when --codigo-bdns is set")

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    try:
        stats = run_batch(
            conn,
            limit=limit,
            force=force,
            dry_run=dry_run,
            anos=anos,
            meses=meses,
            codigo_bdns=codigo_bdns,
            todo=todo,
        )
    finally:
        conn.close()

    if dry_run:
        for item in stats.planned:
            print(
                f"  id={item.subvencion_id}  "
                f"step={item.heuristic_step or '-'}  "
                f"source={item.document_source or 'n/a'}  "
                + (f"name='{item.document_name}'  " if item.document_name else "")
                + (f"ref={item.document_ref}  " if item.document_ref else "")
                + (f"key={item.document_key}  " if item.document_key else "")
                + (
                    "cache_hit=true"
                    if item.cache_hit
                    else ("skipped_no_doc=true" if item.skipped_no_doc else "")
                )
            )
        print(f"plan_for={len(stats.planned)} items dry_run=true")
    else:
        print(
            json.dumps(
                {
                    "processed": stats.processed,
                    "valid": stats.valid,
                    "partial": stats.partial,
                    "invalid": stats.invalid,
                    "skipped_no_doc": stats.skipped_no_doc,
                    "dedup_hits": stats.dedup_hits,
                    "duration_seconds": round(stats.duration_seconds, 2),
                }
            )
        )
    return 0
