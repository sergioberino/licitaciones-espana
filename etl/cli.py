"""
CLI de este microservicio ETL. Punto de entrada: licitia-etl.

Comandos: status, init-db, generate_embedding.
Configuración: variables de entorno (p. ej. .env de este servicio): DB_*, EMBEDDING_SERVICE_URL, etc.
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
    get_embed_max_workers,
    get_embedding_service_url,
    get_ingest_batch_size,
)


def _load_env() -> None:
    """Carga el .env de este servicio (directorio actual y raíz de este workspace)."""
    workspace_root = Path(__file__).resolve().parent.parent
    load_dotenv()
    load_dotenv(workspace_root / ".env")


def _schema_dir() -> Path:
    """Directorio de esquemas: LICITACIONES_SCHEMAS_DIR o por defecto schemas/ en este workspace."""
    base = os.environ.get("LICITACIONES_SCHEMAS_DIR")
    if base:
        return Path(base)
    return Path(__file__).resolve().parent.parent / "schemas"


def _comprobar_base_datos() -> tuple[bool, str]:
    """
    Comprueba la conexión a PostgreSQL. Subrutina usada por status.
    Devuelve (éxito, mensaje). En error, mensaje describe el fallo.
    """
    url = get_database_url()
    if url is None:
        return False, "Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env)."
    host = os.environ.get("DB_HOST", "?")
    port = os.environ.get("DB_PORT", "5432")
    try:
        with psycopg2.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, f"ETL conectado correctamente a la base de datos en {host}:{port}."
    except psycopg2.Error as e:
        return False, f"ETL no puede conectar con la base de datos: {e}"


def _comprobar_embedding() -> tuple[bool, str]:
    """
    Comprueba que el servicio de embedding sea accesible (GET /openapi.json). Subrutina usada por status.
    Devuelve (éxito, mensaje). En error, mensaje describe el fallo.
    """
    base_url = get_embedding_service_url()
    url = f"{base_url}/openapi.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            return False, "El servicio de embedding devolvió openapi.json vacío."
        return True, f"ETL conectado correctamente al servicio de embedding en {base_url}."
    except requests.RequestException as e:
        return False, f"ETL no puede conectar con el servicio de embedding: {e}"
    except ValueError as e:
        return False, f"Respuesta inválida del servicio de embedding: {e}"


def cmd_status(_args: argparse.Namespace) -> int:
    """Comando status: comprueba base de datos y servicio de embedding; imprime mensajes en español."""
    ok_db, msg_db = _comprobar_base_datos()
    print(msg_db if ok_db else msg_db, file=sys.stderr if not ok_db else sys.stdout)
    if not ok_db:
        return 1
    ok_emb, msg_emb = _comprobar_embedding()
    print(msg_emb if ok_emb else msg_emb, file=sys.stderr if not ok_emb else sys.stdout)
    if not ok_emb:
        return 1
    print("ETL está operativo. Base de datos y servicio de embedding accesibles.")
    return 0


# Solo capa dim en esta iteración; 004–007 (nacional, catalunya, valencia, views) quedan para TDR/ingesta.
SCHEMA_FILES = [
    "001_dim_cpv.sql",
    "001b_dim_cpv_router.sql",
    "002_dim_ccaa.sql",
    "002b_dim_provincia.sql",
    "003_dim_dir3.sql",
]


# Schemas que el ETL es responsable de crear (solo si no existen).
ETL_SCHEMAS_SQL = "CREATE SCHEMA IF NOT EXISTS dim; CREATE SCHEMA IF NOT EXISTS raw;"


def cmd_init_db(args: argparse.Namespace) -> int:
    """Aplica las migraciones de esquema y rellena dim (p. ej. dim.cpv_dim). Crea los schemas dim y raw si no existen."""
    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    schema_dir = getattr(args, "schema_dir", None) or _schema_dir()
    if not schema_dir.exists():
        print(f"Directorio de esquemas no encontrado: {schema_dir}", file=sys.stderr)
        return 1
    url = get_database_url()
    try:
        with psycopg2.connect(url) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                for stmt in ETL_SCHEMAS_SQL.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cur.execute(stmt)
            conn.commit()
    except psycopg2.Error as e:
        print(f"Error creando schemas (dim, raw): {e}", file=sys.stderr)
        return 1
    print("Schemas dim, raw comprobados (CREATE IF NOT EXISTS).")
    for filename in SCHEMA_FILES:
        path = schema_dir / filename
        if not path.exists():
            print(f"Archivo de esquema no encontrado: {path}", file=sys.stderr)
            return 1
        sql = path.read_text(encoding="utf-8")
        try:
            with psycopg2.connect(url) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    if filename == "003_dim_dir3.sql":
                        cur.execute("DROP TABLE IF EXISTS dim.dim_dir3 CASCADE")
                    cur.execute(sql)
                conn.commit()
        except psycopg2.Error as e:
            # Objeto ya existente o datos ya cargados: loguear y continuar con el siguiente fichero.
            pgcode = getattr(e, "pgcode", None)
            msg = str(e).lower()
            skip_codes = ("42P07", "23505")
            if pgcode in skip_codes or "already exists" in msg or "duplicate key" in msg:
                print(f"Ya existe ({filename}): objeto o datos ya presentes. Omitiendo.", file=sys.stderr)
                continue
            print(f"Error aplicando {filename}: {e}", file=sys.stderr)
            return 1
        print(f"Aplicado {filename}")
        # Tras 003_dim_dir3.sql: ingesta DIR3 desde XLSX (Listado Unidades AGE).
        if filename == "003_dim_dir3.sql":
            try:
                from etl.dir3_ingest import run_dir3_ingest
                n = run_dir3_ingest(url)
                print(f"DIR3 ingerido: {n} filas.")
            except Exception as e:
                print(f"Error insertando DIR3: {e}", file=sys.stderr)
                return 1
    print("init-db completado.")
    return 0


def cmd_generate_embedding(args: argparse.Namespace) -> int:
    """Rellena dim.cpv_router desde dim.cpv_dim usando el servicio de embedding."""
    from etl.embed_cpv import run_cpv_embed

    if get_database_url() is None:
        print("Falta configuración de base de datos (definir DB_HOST, DB_NAME, DB_USER en .env).", file=sys.stderr)
        return 1
    target = getattr(args, "target", "cpv")
    if target != "cpv":
        print(f"Destino no soportado --target {target}; solo se admite 'cpv'.", file=sys.stderr)
        return 1
    embed_url = getattr(args, "embedding_service_url", None) or get_embedding_service_url()
    embed_batch = get_embed_batch_size()
    ingest_batch = get_ingest_batch_size()
    embed_workers = get_embed_max_workers()
    try:
        total, failed = run_cpv_embed(
            get_database_url(),
            embed_url,
            embed_batch_size=embed_batch,
            ingest_batch_size=ingest_batch,
            embed_max_workers=embed_workers,
        )
        print(f"Insertadas {total} filas en dim.cpv_router.")
        if failed:
            print(
                f"AVISO: {len(failed)} filas fallaron (num_codes): {failed[:20]}{'...' if len(failed) > 20 else ''}",
                file=sys.stderr,
            )
            return 1
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


_HELP_EPILOG = """
Atribución
  Basado en: https://github.com/BquantFinance/licitaciones-espana
  CLI desarrollado por: Sergio Berino, Grupo TOP Digital

Integraciones y configuración
  El ETL se integra con PostgreSQL (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
  y con el servicio de embedding (EMBEDDING_SERVICE_URL). Configure estas variables
  en el .env de este servicio. Opcionales: EMBED_BATCH_SIZE, INGEST_BATCH_SIZE,
  EMBED_MAX_WORKERS, LICITACIONES_SCHEMAS_DIR.

Orden recomendado
  status → init-db → generate_embedding --target cpv

Versión: ver CHANGELOG.md para el historial de cambios.
"""


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(
        prog="licitia-etl",
        description="CLI del microservicio ETL. Comprueba estado, aplica esquemas y rellena el índice CPV (embedding). Configuración solo por variables de entorno.",
        epilog=_HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}\nVer CHANGELOG.md para el historial.")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponibles")

    subparsers.add_parser(
        "status",
        help="Comprueba el estado del ETL (base de datos y servicio de embedding).",
        description="Comprueba la conexión a PostgreSQL y al servicio de embedding. Sale con 0 si todo es correcto.",
    ).set_defaults(func=cmd_status)

    init_parser = subparsers.add_parser(
        "init-db",
        help="Aplica migraciones de esquema y rellena dim (p. ej. dim.cpv_dim).",
        description="Aplica esquemas dim (001_dim_cpv → 001b_dim_cpv_router → 002_dim_ccaa → 002b_dim_provincia → 003_dim_dir3) e ingesta DIR3 desde XLSX. Idempotente. Requiere: DB_*.",
    )
    init_parser.add_argument(
        "--schema-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Directorio de esquemas (por defecto: schemas/ de este workspace o LICITACIONES_SCHEMAS_DIR)",
    )
    init_parser.set_defaults(func=cmd_init_db, schema_dir=None)

    gen_parser = subparsers.add_parser(
        "generate_embedding",
        help="Rellena dim.cpv_router desde dim.cpv_dim usando el servicio de embedding.",
        description=(
            "Lee etiquetas CPV, llama al servicio de embedding por lotes e inserta en dim.cpv_router. "
            "Tamaños de lote por env: EMBED_BATCH_SIZE, INGEST_BATCH_SIZE, EMBED_MAX_WORKERS. "
            "Requiere: DB_*, EMBEDDING_SERVICE_URL. Orden recomendado: ejecutar tras init-db."
        ),
    )
    gen_parser.add_argument(
        "--target",
        type=str,
        default="cpv",
        metavar="TARGET",
        help="Entidad a embeber (por defecto: cpv; solo se admite cpv)",
    )
    gen_parser.add_argument(
        "--embedding-service-url",
        type=str,
        default=None,
        metavar="URL",
        help="URL base del API de embedding (por defecto: EMBEDDING_SERVICE_URL del entorno)",
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
