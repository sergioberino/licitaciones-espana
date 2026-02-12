"""
Populate dim.cpv_router from dim.cpv_dim using the embedding service.

ETL orchestrates; embedding service computes; DB stores.
"""

import time
from typing import List, Tuple

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import requests


# Retry settings for embedding service
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 2.0


def _embed_passage(base_url: str, text: str) -> List[float]:
    """Call POST /embed/passage; return embedding vector. Raises on failure after retries."""
    url = f"{base_url}/embed/passage"
    payload = {"text": text or ""}
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(url, json=payload, timeout=120)
            r.raise_for_status()
            data = r.json()
            emb = data.get("embedding")
            if emb is None:
                raise ValueError("Response missing 'embedding'")
            return emb
        except (requests.RequestException, ValueError, KeyError) as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    raise last_exc  # type: ignore[misc]


def _vector_literal(embedding: List[float]):
    """Format embedding as PostgreSQL vector literal (pass as AsIs so it is not quoted)."""
    s = "[" + ",".join(str(x) for x in embedding) + "]"
    return psycopg2.extensions.AsIs(s + "::vector")


def run_cpv_embed(
    db_url: str,
    embedding_base_url: str,
    embed_batch_size: int = 256,
    ingest_batch_size: int = 10000,
) -> Tuple[int, List[int]]:
    """
    Read dim.cpv_dim (num_code, label), call embedding service in batches, bulk insert into dim.cpv_router.

    Returns (total_inserted, list of failed num_codes).
    """
    base_url = embedding_base_url.rstrip("/")
    failed_num_codes: List[int] = []
    total_inserted = 0

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dim' AND table_name = 'cpv_dim')"
            )
            if not cur.fetchone()[0]:
                raise RuntimeError("dim.cpv_dim does not exist; run init-db first")

            cur.execute("TRUNCATE dim.cpv_router")
            conn.commit()

        buffer: List[Tuple[int, List[float], str]] = []

        with conn.cursor(name="stream_cpv") as cur:
            cur.itersize = max(embed_batch_size, ingest_batch_size) * 2
            cur.execute("SELECT num_code, label FROM dim.cpv_dim ORDER BY num_code")

            while True:
                rows = cur.fetchmany(embed_batch_size)
                if not rows:
                    break

                for num_code, label in rows:
                    try:
                        embedding = _embed_passage(base_url, label or "")
                        buffer.append((num_code, embedding, label or ""))
                    except Exception:
                        failed_num_codes.append(num_code)

                while len(buffer) >= ingest_batch_size:
                    batch = buffer[:ingest_batch_size]
                    buffer = buffer[ingest_batch_size:]
                    with conn.cursor() as insert_cur:
                        psycopg2.extras.execute_values(
                            insert_cur,
                            """
                            INSERT INTO dim.cpv_router (num_code, embedding, label)
                            VALUES %s
                            """,
                            [
                                (num_code, _vector_literal(embedding), label)
                                for num_code, embedding, label in batch
                            ],
                            template="(%s, %s, %s)",
                            page_size=len(batch),
                        )
                    conn.commit()
                    total_inserted += len(batch)

        # Flush remainder
        if buffer:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO dim.cpv_router (num_code, embedding, label)
                    VALUES %s
                    """,
                    [
                        (num_code, _vector_literal(embedding), label)
                        for num_code, embedding, label in buffer
                    ],
                    template="(%s, %s, %s)",
                    page_size=len(buffer),
                )
            conn.commit()
            total_inserted += len(buffer)

    return total_inserted, failed_num_codes
