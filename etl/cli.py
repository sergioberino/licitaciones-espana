"""
CLI for this ETL microservice. Entrypoint: licitia-etl.

Subcommands:
  check-connection   Validate PostgreSQL connection (exit 0 if OK).
  check-embedding   Validate embedding service is reachable (GET /openapi.json; exit 0 if OK).
  init-db           Apply schema migrations (001 → 001b → 002 → 003 → 004 → 005).
  generate_embedding  Populate dim.cpv_router from dim.cpv_dim via embedding service.

Configuration is read from this service's environment (e.g. this service's .env):
DB_*, EMBEDDING_SERVICE_URL, EMBED_BATCH_SIZE, INGEST_BATCH_SIZE, LICITACIONES_SCHEMAS_DIR.
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

from etl import __version__
from etl.config import (
    get_database_url,
    get_embed_batch_size,
    get_embedding_service_url,
    get_ingest_batch_size,
)


def _load_env() -> None:
    """Load this service's .env (current directory and this workspace root)."""
    workspace_root = Path(__file__).resolve().parent.parent
    load_dotenv()
    load_dotenv(workspace_root / ".env")


def _schema_dir() -> Path:
    """Schema directory: LICITACIONES_SCHEMAS_DIR from this service's env, or default schemas/ in this workspace."""
    base = os.environ.get("LICITACIONES_SCHEMAS_DIR")
    if base:
        return Path(base)
    return Path(__file__).resolve().parent.parent / "schemas"


def cmd_check_connection(_args: argparse.Namespace) -> int:
    url = get_database_url()
    if url is None:
        print("FAIL No DB configuration (set DB_HOST, DB_NAME, DB_USER in this service's .env)", file=sys.stderr)
        return 1
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        print("OK")
        return 0
    except psycopg2.Error as e:
        print(f"FAIL {e}", file=sys.stderr)
        return 1


def cmd_check_embedding(_args: argparse.Namespace) -> int:
    """Check that the embedding service is reachable by GET /openapi.json."""
    base_url = get_embedding_service_url()
    url = f"{base_url}/openapi.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            print("FAIL Embedding service returned empty openapi.json", file=sys.stderr)
            return 1
        print("OK")
        return 0
    except requests.RequestException as e:
        print(f"FAIL {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"FAIL Invalid JSON from embedding service: {e}", file=sys.stderr)
        return 1


SCHEMA_FILES = [
    "001_dim_cpv.sql",
    "001b_dim_cpv_router.sql",
    "002_nacional.sql",
    "003_catalunya.sql",
    "004_valencia.sql",
    "005_views.sql",
]


def cmd_init_db(args: argparse.Namespace) -> int:
    if get_database_url() is None:
        print("FAIL No DB configuration (set DB_HOST, DB_NAME, DB_USER in this service's .env)", file=sys.stderr)
        return 1
    schema_dir = getattr(args, "schema_dir", None) or _schema_dir()
    if not schema_dir.exists():
        print(f"FAIL Schema dir not found: {schema_dir}", file=sys.stderr)
        return 1
    url = get_database_url()
    for filename in SCHEMA_FILES:
        path = schema_dir / filename
        if not path.exists():
            print(f"FAIL Schema file not found: {path}", file=sys.stderr)
            return 1
        sql = path.read_text(encoding="utf-8")
        try:
            with psycopg2.connect(url) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
        except psycopg2.Error as e:
            print(f"FAIL Applying {filename}: {e}", file=sys.stderr)
            return 1
        print(f"Applied {filename}")
    print("init-db done.")
    return 0


def cmd_generate_embedding(args: argparse.Namespace) -> int:
    from etl.embed_cpv import run_cpv_embed

    if get_database_url() is None:
        print("FAIL No DB configuration (set DB_HOST, DB_NAME, DB_USER in this service's .env)", file=sys.stderr)
        return 1
    target = getattr(args, "target", "cpv")
    if target != "cpv":
        print(f"FAIL Unsupported --target {target}; only 'cpv' is supported.", file=sys.stderr)
        return 1
    embed_url = getattr(args, "embedding_service_url", None) or get_embedding_service_url()
    embed_batch = get_embed_batch_size()
    ingest_batch = get_ingest_batch_size()
    try:
        total, failed = run_cpv_embed(
            get_database_url(),
            embed_url,
            embed_batch_size=embed_batch,
            ingest_batch_size=ingest_batch,
        )
        print(f"Inserted {total} rows into dim.cpv_router.")
        if failed:
            print(f"WARN {len(failed)} rows failed (num_codes): {failed[:20]}{'...' if len(failed) > 20 else ''}", file=sys.stderr)
            return 1
        return 0
    except Exception as e:
        print(f"FAIL {e}", file=sys.stderr)
        return 1


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(
        prog="licitia-etl",
        description="ETL microservice CLI for licitaciones-espana. Initialization and DB checks.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser(
        "check-connection",
        help="Validate PostgreSQL connection (exit 0 if OK).",
        description="Validate that the ETL can connect to PostgreSQL using configured credentials.",
    ).set_defaults(func=cmd_check_connection)

    subparsers.add_parser(
        "check-embedding",
        help="Validate embedding service is reachable (exit 0 if OK).",
        description="GET the embedding service /openapi.json; exit 0 if a response is returned.",
    ).set_defaults(func=cmd_check_embedding)

    init_parser = subparsers.add_parser(
        "init-db",
        help="Apply schema migrations and populate dim (e.g. dim.cpv_dim).",
        description="Run schemas 001 → 001b → 002 → 003 → 004 → 005. Idempotent.",
    )
    init_parser.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Path to schemas directory (default: this workspace schemas/ or LICITACIONES_SCHEMAS_DIR)",
    )
    init_parser.set_defaults(func=cmd_init_db, schema_dir=None)

    gen_parser = subparsers.add_parser(
        "generate_embedding",
        help="Populate dim.cpv_router from dim.cpv_dim using the embedding service.",
        description=(
            "Read CPV labels, call embedding service in batches, bulk insert into dim.cpv_router. "
            "Batch sizes are read from env: EMBED_BATCH_SIZE (embedding API), INGEST_BATCH_SIZE (SQL bulk insert). "
            "Intended for scheduled runs (e.g. cron / orchestrator)."
        ),
    )
    gen_parser.add_argument(
        "--target",
        type=str,
        default="cpv",
        metavar="TARGET",
        help="Entity to embed (default: cpv; only cpv supported)",
    )
    gen_parser.add_argument(
        "--embedding-service-url",
        type=str,
        default=None,
        metavar="URL",
        help="Base URL for embedding API (default: EMBEDDING_SERVICE_URL from env)",
    )
    gen_parser.set_defaults(func=cmd_generate_embedding)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0
    if getattr(args, "schema_dir", None) is None:
        args.schema_dir = _schema_dir()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
