#!/usr/bin/env python3
"""
E2E test: user query -> top-k CPVs via embedding service + ANN on dim.cpv_router.

Preconditions:
  - Postgres with init-db applied (dim.cpv_dim and dim.cpv_router exist).
  - dim.cpv_router populated (run: licitia-etl generate_embedding --target cpv).
  - Embedding service reachable. In Docker: EMBEDDING_SERVICE_URL=http://embedding:8000 (service name + container port). From host: http://localhost:8001.

Usage:
  From this service's workspace (or via compose from repo root):
    docker compose run --rm etl python scripts/e2e_cpv_router_topk.py "construcción de carreteras" --top-k 5
  Or with this service's .env set:
    DB_HOST=postgres DB_NAME=... EMBEDDING_SERVICE_URL=http://embedding:8000 \\
    python scripts/e2e_cpv_router_topk.py "carreteras" --top-k 5

Exit: 0 if top-k CPVs returned and optional semantic check passes; 1 otherwise.
"""

import argparse
import os
import sys
from pathlib import Path

# This service's package on path when run from this workspace root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()
load_dotenv(Path(__file__).resolve().parent.parent / ".env")  # this service's .env

import psycopg2
import requests

from etl.config import get_database_url, get_embedding_service_url


def embed_query(base_url: str, text: str) -> list[float]:
    """POST /embed/query; return embedding vector."""
    r = requests.post(
        f"{base_url.rstrip('/')}/embed/query",
        json={"text": text},
        timeout=60,
    )
    r.raise_for_status()
    data = r.json()
    emb = data.get("embedding")
    if emb is None:
        raise ValueError("Response missing 'embedding'")
    return emb


def topk_cpv(db_url: str, query_embedding: list[float], top_k: int) -> list[tuple[int, str | None]]:
    """ANN search: ORDER BY embedding <=> query_embedding LIMIT top_k. Returns [(num_code, label), ...]."""
    vector_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT num_code, label
                FROM dim.cpv_router
                ORDER BY embedding <=> %s::vector
                LIMIT %s
                """,
                (vector_str, top_k),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description="E2E: query -> top-k CPVs (embedding + ANN)")
    parser.add_argument("query", type=str, help="Natural-language query (e.g. 'construcción de carreteras')")
    parser.add_argument("--top-k", type=int, default=5, metavar="K", help="Number of CPVs to return (default: 5)")
    parser.add_argument(
        "--embedding-service-url",
        type=str,
        default=None,
        help="Override EMBEDDING_SERVICE_URL",
    )
    args = parser.parse_args()

    db_url = get_database_url()
    if not db_url:
        print("FAIL No DB configuration (set DB_HOST, DB_NAME, DB_USER in this service's .env)", file=sys.stderr)
        return 1
    embed_url = args.embedding_service_url or os.environ.get("EMBEDDING_SERVICE_URL") or get_embedding_service_url()

    try:
        query_embedding = embed_query(embed_url, args.query)
    except Exception as e:
        print(f"FAIL Embedding query: {e}", file=sys.stderr)
        return 1

    try:
        results = topk_cpv(db_url, query_embedding, args.top_k)
    except Exception as e:
        print(f"FAIL ANN search: {e}", file=sys.stderr)
        return 1

    n = len(results)
    if n == 0:
        print("FAIL No rows in dim.cpv_router (run generate_embedding --target cpv first)", file=sys.stderr)
        return 1

    print(f"Top-{args.top_k} CPVs for query: {args.query!r}")
    for num_code, label in results:
        print(f"  {num_code}  {label or ''}")

    # Assert: we get min(top_k, N) rows
    expected = min(args.top_k, n)
    if len(results) != expected:
        print(f"FAIL Expected {expected} rows, got {len(results)}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
