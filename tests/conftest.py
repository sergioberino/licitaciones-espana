"""
Fixtures para tests del ETL. BD de prueba vía DB_* y DB_SCHEMA (opcional).
Si no están definidos, los tests que requieren BD se marcan como skip.
"""

import os
from pathlib import Path

import pytest

# Comparte la misma carga del .env del submódulo que los tests de etl/nlp/tests/.
# Esto permite que tests/test_api.py también lea DB_HOST/DB_NAME/DB_USER al
# arrancar (sin esto, las fixtures con BD se saltan en local).
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

if os.environ.get("DB_HOST") == "postgres":
    # docker-compose expone el contenedor como `postgres`; en host es `localhost`.
    os.environ["DB_HOST"] = "localhost"


@pytest.fixture(scope="session")
def db_url():
    """URL de PostgreSQL desde env. None si no hay configuración."""
    from etl.config import get_database_url
    return get_database_url()


@pytest.fixture(scope="session")
def db_schema():
    """Schema de trabajo desde env. Por defecto 'l0_test' para no tocar datos reales."""
    return os.environ.get("DB_SCHEMA", "l0_test")


@pytest.fixture
def require_db(db_url):
    """Fixture que hace skip del test si no hay DB configurada."""
    if db_url is None:
        pytest.skip("Falta configuración DB_* (DB_HOST, DB_NAME, DB_USER)")


@pytest.fixture
def require_db_schema(db_schema):
    """Fixture que hace skip si no hay DB_SCHEMA (o l0_test por defecto)."""
    if not (db_schema and db_schema.replace("_", "").isalnum()):
        pytest.skip("DB_SCHEMA no válido o no definido")
