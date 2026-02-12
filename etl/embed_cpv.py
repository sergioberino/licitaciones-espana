"""
Populate dim.cpv_router from dim.cpv_dim using the embedding service.

ETL orchestrates; embedding service computes; DB stores.
Uses parallel embedding per batch, tmp staging, and a pipelined ingest consumer.
"""

import json
import logging
import math
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import requests

# Retry settings for embedding service
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 2.0
BAR_WIDTH = 20
LOG_PREFIX = "[generate_embedding]"

logger = logging.getLogger("etl.embed_cpv")


def _configure_logging() -> None:
    """Ensure logger has a stderr handler with consistent prefix when run from CLI."""
    if logger.handlers:
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(LOG_PREFIX + " %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


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


def _get_tmp_dir() -> Path:
    """Staging base for embedding batches. Uses LICITACIONES_TMP_DIR (default /app/tmp)."""
    raw = os.environ.get("LICITACIONES_TMP_DIR", "/app/tmp")
    return Path(raw)


def _progress_bar(done: int, total: int) -> str:
    """Minimal character progress bar for the current batch."""
    if total <= 0:
        return "[" + "-" * BAR_WIDTH + "] 0%"
    pct = min(100, int(100 * done / total))
    filled = int(BAR_WIDTH * done / total)
    return f"[{'#' * filled}{'-' * (BAR_WIDTH - filled)}] {pct}%"


def run_cpv_embed(
    db_url: str,
    embedding_base_url: str,
    embed_batch_size: int = 256,
    ingest_batch_size: int = 10000,
    embed_max_workers: int = 1,
) -> Tuple[int, List[int]]:
    """
    Read dim.cpv_dim (num_code, label), call embedding service (parallel or sequential per batch),
    stage batches to tmp, and ingest via a pipelined consumer.

    embed_max_workers=1 means sequential embedding; >1 allows concurrent requests per batch (hardware-dependent).
    Returns (total_inserted, list of failed num_codes).
    """
    _configure_logging()
    base_url = embedding_base_url.rstrip("/")
    failed_num_codes: List[int] = []
    tmp_base = _get_tmp_dir() / "embedding_cpv"
    tmp_base.mkdir(parents=True, exist_ok=True)

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dim' AND table_name = 'cpv_dim')"
            )
            if not cur.fetchone()[0]:
                raise RuntimeError("dim.cpv_dim does not exist; run init-db first")
            cur.execute("SELECT count(*) FROM dim.cpv_dim")
            total_rows = cur.fetchone()[0]
            cur.execute("TRUNCATE dim.cpv_router")
            conn.commit()

    total_batches = math.ceil(total_rows / embed_batch_size) if total_rows else 0
    logger.info(
        "total_batches=%s embed_batch_size=%s ingest_batch_size=%s embed_max_workers=%s",
        total_batches,
        embed_batch_size,
        ingest_batch_size,
        embed_max_workers,
    )

    if total_batches == 0:
        return 0, []

    # Bounded queue: producer puts (batch_index, path); consumer ingests. Sentinel (None, None) to stop.
    ingest_queue: queue.Queue[Optional[Tuple[int, str]]] = queue.Queue(maxsize=2)
    total_inserted = [0]  # mutable for consumer to update
    ingest_error: List[Optional[Exception]] = [None]  # consumer sets on failure

    def consumer() -> None:
        try:
            with psycopg2.connect(db_url) as conn:
                while True:
                    item = ingest_queue.get()
                    if item is None:
                        break
                    batch_index, path = item
                    with open(path, "r", encoding="utf-8") as f:
                        batch_data = json.load(f)
                    # batch_data: list of {"num_code", "embedding", "label"}
                    rows = [
                        (r["num_code"], _vector_literal(r["embedding"]), r.get("label") or "")
                        for r in batch_data
                    ]
                    with conn.cursor() as insert_cur:
                        psycopg2.extras.execute_values(
                            insert_cur,
                            """
                            INSERT INTO dim.cpv_router (num_code, embedding, label)
                            VALUES %s
                            """,
                            rows,
                            template="(%s, %s, %s)",
                            page_size=len(rows),
                        )
                    conn.commit()
                    total_inserted[0] += len(rows)
                    try:
                        Path(path).unlink()
                    except OSError:
                        pass
                    logger.info(
                        "batch %s/%s ingested (%s rows so far)",
                        batch_index + 1,
                        total_batches,
                        total_inserted[0],
                    )
        except Exception as e:
            logger.exception("ingest failed: %s", e)
            ingest_error[0] = e

    consumer_thread = threading.Thread(target=consumer, daemon=False)
    consumer_thread.start()

    try:
        with psycopg2.connect(db_url) as conn:
            with conn.cursor(name="stream_cpv") as cur:
                cur.itersize = embed_batch_size * 2
                cur.execute("SELECT num_code, label FROM dim.cpv_dim ORDER BY num_code")
                batch_index = 0
                while True:
                    rows = cur.fetchmany(embed_batch_size)
                    if not rows:
                        break

                    logger.info(
                        "batch %s/%s embedding (%s items)...",
                        batch_index + 1,
                        total_batches,
                        len(rows),
                    )

                    # Parallel embedding for this batch
                    results: List[Tuple[int, List[float], str]] = []
                    batch_failed: List[int] = []
                    done_count = 0
                    next_log_at = 25

                    def do_one(row: Tuple[int, str]) -> Tuple[int, Optional[List[float]], str]:
                        num_code, label = row
                        try:
                            emb = _embed_passage(base_url, label or "")
                            return (num_code, emb, label or "")
                        except Exception:
                            return (num_code, None, label or "")

                    with ThreadPoolExecutor(max_workers=min(embed_max_workers, len(rows))) as executor:
                        futures = {executor.submit(do_one, r): r for r in rows}
                        for fut in as_completed(futures):
                            num_code, emb, label = fut.result()
                            if emb is not None:
                                results.append((num_code, emb, label))
                            else:
                                batch_failed.append(num_code)
                            done_count += 1
                            pct = 100 * done_count // len(rows) if rows else 0
                            if pct >= next_log_at:
                                logger.info(
                                    "batch %s/%s %s",
                                    batch_index + 1,
                                    total_batches,
                                    _progress_bar(done_count, len(rows)),
                                )
                                next_log_at = min(100, (pct // 25 + 1) * 25)

                    failed_num_codes.extend(batch_failed)
                    logger.info("batch %s/%s embedded, staging...", batch_index + 1, total_batches)

                    # Stage batch to tmp
                    batch_file = tmp_base / f"batch_{batch_index:04d}.json"
                    with open(batch_file, "w", encoding="utf-8") as f:
                        json.dump(
                            [{"num_code": n, "embedding": e, "label": l} for n, e, l in results],
                            f,
                            ensure_ascii=False,
                        )
                    ingest_queue.put((batch_index, str(batch_file)))
                    batch_index += 1

        ingest_queue.put(None)
        consumer_thread.join()
        if ingest_error[0] is not None:
            raise ingest_error[0]
        return total_inserted[0], failed_num_codes

    except Exception:
        ingest_queue.put(None)
        consumer_thread.join(timeout=5)
        raise
