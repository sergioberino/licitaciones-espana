"""
Load this microservice's configuration from environment (no hardcoded credentials).

All values are read from this service's environment (e.g. from this service's .env in the
ETL workspace). DB_* for Postgres; EMBEDDING_SERVICE_URL for the embedding API.
"""

import os
from typing import Optional
from urllib.parse import quote_plus


def get_database_url() -> Optional[str]:
    """Build PostgreSQL connection URL from this service's DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD."""
    host = os.environ.get("DB_HOST")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD", "")
    if not all((host, name, user)):
        return None
    user_enc = quote_plus(user)
    password_enc = quote_plus(password) if password else ""
    auth = f"{user_enc}:{password_enc}" if password_enc else user_enc
    return f"postgresql://{auth}@{host}:{port}/{name}"


def get_embedding_service_url() -> str:
    """Base URL for the embedding API. Read from this service's EMBEDDING_SERVICE_URL (default http://embedding:8000)."""
    return os.environ.get("EMBEDDING_SERVICE_URL", "http://embedding:8000").rstrip("/")


def get_embed_batch_size() -> int:
    """Batch size for embedding service calls (EMBED_BATCH_SIZE). Default 256 (reasonable for multilingual-e5-large in production)."""
    raw = os.environ.get("EMBED_BATCH_SIZE", "256")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 256


def get_ingest_batch_size() -> int:
    """Batch size for bulk INSERT into Postgres (INGEST_BATCH_SIZE). Default 10000 rows per transaction."""
    raw = os.environ.get("INGEST_BATCH_SIZE", "10000")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 10000
