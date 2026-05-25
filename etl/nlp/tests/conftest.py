"""Fixtures compartidos para tests NLP."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[3] / ".env")
except ImportError:
    pass

# docker-compose expone postgres en localhost:5432; el .env del servicio usa DB_HOST=postgres.
if os.environ.get("DB_HOST") == "postgres":
    os.environ["DB_HOST"] = "localhost"


@pytest.fixture
def pg_conn():
    from etl.config import get_database_url

    url = get_database_url()
    if url is None:
        pytest.skip("Falta configuración DB_* (DB_HOST, DB_NAME, DB_USER)")
    try:
        conn = psycopg2.connect(url)
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL no disponible: {exc}")
    yield conn
    conn.close()
