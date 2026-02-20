"""
Carga la configuración de este microservicio desde el entorno (sin credenciales en código).

Todos los valores se leen del entorno de este servicio (p. ej. .env del workspace ETL).
"""

import os
from typing import Optional
from urllib.parse import quote_plus


def get_database_url() -> Optional[str]:
    """Construye la URL de conexión a PostgreSQL desde las variables de entorno del servicio."""
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


def get_ingest_batch_size() -> int:
    """Tamaño de lote para INSERT masivo en Postgres. Por defecto 10000 filas por transacción."""
    raw = os.environ.get("INGEST_BATCH_SIZE", "10000")
    try:
        n = int(raw)
        return max(1, n)
    except ValueError:
        return 10000


def get_db_schema() -> Optional[str]:
    """Nombre del schema de trabajo para ingesta L0 (raw). L0_DB_SCHEMA o DB_SCHEMA. Requerido para init-db."""
    raw = (os.environ.get("L0_DB_SCHEMA") or os.environ.get("DB_SCHEMA") or "").strip()
    return raw if raw else None
