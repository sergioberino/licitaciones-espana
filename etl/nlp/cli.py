import argparse
import json
import logging
import os
import sys

import psycopg2

from etl.nlp.pipeline import run_batch


def add_subparser(subparsers: argparse._SubParsersAction):
    sp = subparsers.add_parser("nlp", help="Batch B: análisis NLP de bases reguladoras")
    nlp_sub = sp.add_subparsers(dest="nlp_cmd", required=True)

    analizar = nlp_sub.add_parser("analizar", help="Procesa convocatorias pendientes")
    analizar.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Cap absoluto del batch (default 100). Ordena por id DESC.",
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


def cmd_analizar(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        stream=sys.stdout,
    )
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    try:
        stats = run_batch(conn, limit=args.limit, force=args.force, dry_run=args.dry_run)
    finally:
        conn.close()

    if args.dry_run:
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
