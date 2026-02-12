"""
Carga la configuración de este microservicio desde el entorno (sin credenciales en código).

Todos los valores se leen del entorno de este servicio (p. ej. .env del workspace ETL).
DB_* para Postgres; EMBEDDING_SERVICE_URL para el API de embedding; EMBED_* e INGEST_* para lotes.
"""

import os
from typing import Optional
from urllib.parse import quote_plus


def get_database_url() -> Optional[str]:
    """Construye la URL de conexión a PostgreSQL desde DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD de este servicio."""
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
    """URL base del API de embedding. Variable EMBEDDING_SERVICE_URL (ej. http://embedding:8000 en red Docker)."""
    return os.environ.get("EMBEDDING_SERVICE_URL", "http://localhost:8000").rstrip("/")


def get_embed_batch_size() -> int:
    """Tamaño de lote para llamadas al servicio de embedding (EMBED_BATCH_SIZE). Por defecto 256."""
    raw = os.environ.get("EMBED_BATCH_SIZE", "256")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 256


def get_ingest_batch_size() -> int:
    """Tamaño de lote para INSERT masivo en Postgres (INGEST_BATCH_SIZE). Por defecto 10000 filas por transacción."""
    raw = os.environ.get("INGEST_BATCH_SIZE", "10000")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 10000


def get_embed_max_workers() -> int:
    """Máximo de peticiones de embedding concurrentes por lote (EMBED_MAX_WORKERS). 1 = secuencial; aumentar si hay más capacidad."""
    raw = os.environ.get("EMBED_MAX_WORKERS", "1")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 1
