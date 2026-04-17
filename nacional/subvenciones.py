import requests
import psycopg2
from psycopg2.extras import execute_batch
from dataclasses import dataclass, asdict, field
from datetime import datetime
import sys
from pathlib import Path
import time
import pandas as pd
import os
import argparse
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import json

LOG_PREFIX = "[subvenciones]"


def _log(level: str, msg: str) -> None:
    print(f"{LOG_PREFIX} [{level}] {msg}", flush=True)


# Add parent directory to path to import etl modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import get_database_url

# No API_KEY required - public access
API_BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias"
API_ENDPOINT_SEARCH = API_BASE_URL + "/busqueda"
API_ENDPOINT_LATEST = API_BASE_URL + "/ultimas"
API_ENDPOINT_DETAIL = (
    API_BASE_URL  # Same as base URL, uses query params vpd and numConv
)

# Default VPD parameter for detail endpoint
DEFAULT_VPD = "GE"

# Maximum records per parquet file
MAX_RECORDS_PER_PARQUET = 20000

# Directory configuration (similar to nacional/licitaciones.py)
_repo_root = Path(__file__).resolve().parent.parent
_tmp_base = Path(os.environ.get("LICITACIONES_TMP_DIR", _repo_root / "tmp"))
OUTPUT_DIR = _tmp_base / "output"


def _ensure_output_dir():
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class RateLimiter:
    """Manage API rate limiting to avoid exceeding request limits and get API ban.

    Thread-safe implementation using threading.Lock for atomic operations.
    """

    def __init__(self, max_requests: int = 49, time_window: int = 60):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed in time window (default 49 to stay safe)
            time_window: Time window in seconds (default 60 for 1 minute)
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = []
        self._lock = threading.Lock()  # Thread-safe lock

    def wait_if_needed(self):
        """Wait if necessary to avoid exceeding rate limit. Thread-safe."""
        with self._lock:  # Atomic operation
            now = time.time()

            # Remove requests outside the current time window
            self.request_times = [
                t for t in self.request_times if now - t < self.time_window
            ]

            # Check if we've reached the limit
            if len(self.request_times) >= self.max_requests:
                # Calculate how long to wait
                oldest_request = self.request_times[0]
                wait_time = self.time_window - (now - oldest_request)

                if wait_time > 0:
                    _log(
                        "WARN",
                        f"Límite de peticiones alcanzado. Esperando {wait_time:.1f}s...",
                    )
                    time.sleep(wait_time + 0.1)
                    now = time.time()
                    self.request_times = [
                        t for t in self.request_times if now - t < self.time_window
                    ]

            self.request_times.append(time.time())

    def reset(self):
        """Reset the rate limiter. Thread-safe."""
        with self._lock:
            self.request_times = []


@dataclass
class SearchParams:
    """Search parameters for grants (subvenciones) API."""

    page: int = 0
    pageSize: int = 10000
    fechaDesde: str | None = None  # Format: "DD/MM/YYYY"
    fechaHasta: str = datetime.now().strftime("%d/%m/%Y")  # Format: "DD/MM/YYYY"

    def to_dict(self) -> dict:
        """Convert to dictionary removing None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")
        if self.pageSize < 50 or self.pageSize > 10000:
            raise ValueError(
                f"pageSize debe estar entre 50-10000, recibido: {self.pageSize}"
            )

        # Validate date formats if present
        if self.fechaDesde:
            self._validate_date_format(self.fechaDesde, "fechaDesde")
        if self.fechaHasta:
            self._validate_date_format(self.fechaHasta, "fechaHasta")

        # Validate date order
        if self.fechaDesde and self.fechaHasta:
            if self.fechaDesde > self.fechaHasta:
                raise ValueError(
                    f"fechaDesde ({self.fechaDesde}) debe ser <= fechaHasta ({self.fechaHasta})"
                )

    @staticmethod
    def _validate_date_format(date_str: str, field_name: str) -> None:
        """Validate that a date has DD/MM/YYYY format."""
        try:
            datetime.strptime(date_str, "%d/%m/%Y")
        except ValueError:
            raise ValueError(
                f"{field_name} debe tener formato DD/MM/YYYY, recibido: {date_str}"
            )


@dataclass
class LatestParams:
    """Parameters for latest grants endpoint (ultimas). pageSize and order are fixed."""

    page: int = 0
    pageSize: int = 100
    order: str = field(default="numeroConvocatoria", init=False)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")

        if self.pageSize < 100 or self.pageSize > 1000:
            raise ValueError(f"pageSize debe estar entre [100, 1000]: {self.pageSize}")


def _flatten_organo(
    organo: Optional[dict],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Flatten organo nested object to nivel1, nivel2, nivel3."""
    if not organo:
        return None, None, None
    return (
        organo.get("nivel1"),
        organo.get("nivel2"),
        organo.get("nivel3"),
    )


def _convert_to_json_serializable(obj):
    """Convert objects to JSON serializable format for JSONB columns."""
    if obj is None:
        return None
    if isinstance(obj, (list, dict)):
        return obj
    # Handle any other complex types
    return str(obj)


def fetch_convocatoria_detalle(
    numero_convocatoria: str,
    rate_limiter: RateLimiter,
    vpd: str = DEFAULT_VPD,
) -> Optional[dict]:
    """
    Fetch detailed information for a single convocatoria.

    Args:
        numero_convocatoria: The convocatoria number to fetch details for
        rate_limiter: RateLimiter instance (thread-safe)
        vpd: VPD parameter (default: "GE")

    Returns:
        Dictionary with detailed convocatoria data or None if error
    """
    try:
        rate_limiter.wait_if_needed()

        params = {
            "vpd": vpd,
            "numConv": numero_convocatoria,
        }

        res = requests.get(API_ENDPOINT_DETAIL, params=params, timeout=30)

        if res.status_code != 200:
            _log(
                "WARN",
                f"Error al obtener detalle de convocatoria {numero_convocatoria}: HTTP {res.status_code}",
            )
            return None

        data = res.json()

        # Flatten organo
        organo = data.get("organo", {})
        nivel1, nivel2, nivel3 = _flatten_organo(organo)

        # Build flattened record
        record = {
            "id": data.get("id"),
            "nivel1": nivel1,
            "nivel2": nivel2,
            "nivel3": nivel3,
            "sede_electronica": data.get("sedeElectronica"),
            "codigo_bdns": data.get("codigoBDNS"),
            "fecha_recepcion": data.get("fechaRecepcion"),
            "instrumentos": _convert_to_json_serializable(data.get("instrumentos")),
            "tipo_convocatoria": data.get("tipoConvocatoria"),
            "presupuesto_total": data.get("presupuestoTotal"),
            "mrr": data.get("mrr"),
            "descripcion": data.get("descripcion"),
            "descripcion_leng": data.get("descripcionLeng"),
            "tipos_beneficiarios": _convert_to_json_serializable(
                data.get("tiposBeneficiarios")
            ),
            "sectores": _convert_to_json_serializable(data.get("sectores")),
            "regiones": _convert_to_json_serializable(data.get("regiones")),
            "descripcion_finalidad": data.get("descripcionFinalidad"),
            "descripcion_bases_reguladoras": data.get("descripcionBasesReguladoras"),
            "url_bases_reguladoras": data.get("urlBasesReguladoras"),
            "se_publica_diario_oficial": data.get("sePublicaDiarioOficial"),
            "abierto": data.get("abierto"),
            "fecha_inicio_solicitud": data.get("fechaInicioSolicitud"),
            "fecha_fin_solicitud": data.get("fechaFinSolicitud"),
            "text_inicio": data.get("textInicio"),
            "text_fin": data.get("textFin"),
            "ayuda_estado": data.get("ayudaEstado"),
            "url_ayuda_estado": data.get("urlAyudaEstado"),
            "fondos": _convert_to_json_serializable(data.get("fondos")),
            "reglamento": _convert_to_json_serializable(data.get("reglamento")),
            "objetivos": _convert_to_json_serializable(data.get("objetivos")),
            "sectores_productos": _convert_to_json_serializable(
                data.get("sectoresProductos")
            ),
            "documentos": _convert_to_json_serializable(data.get("documentos")),
            "anuncios": _convert_to_json_serializable(data.get("anuncios")),
            "advertencia": data.get("advertencia"),
        }

        return record

    except Exception as e:
        _log(
            "ERROR",
            f"Excepción al obtener detalle de convocatoria {numero_convocatoria}: {e}",
        )
        return None


def scrape_historico(params: SearchParams, max_workers: int = 10) -> list[Path]:
    """
    Scrape historical grants data and save to Parquet files.
    This is for initial bulk load - generates Parquet files for etl/ingest_l0.py to process.

    Uses ThreadPool to fetch detailed information for each convocatoria in parallel.
    Generates multiple parquet files with max 20000 records each.

    Args:
        params: SearchParams with date range (fechaDesde required, fechaHasta defaults to today)
        max_workers: Number of threads for parallel detail fetching (default: 10)

    Returns:
        List of paths to generated Parquet files
    """
    if not params.fechaDesde:
        raise ValueError("fechaDesde es requerido para scrape historico")

    params.validate()
    ano_inicio = int(params.fechaDesde[-4:])
    ano_fin = int(params.fechaHasta[-4:])

    _ensure_output_dir()

    # Step 1: Fetch all light convocatorias
    light_convocatorias = []
    rate_limiter = RateLimiter(max_requests=49, time_window=60)

    page = params.page
    is_last_page = False
    total_elements = 0

    print(f"\n{'='*60}", flush=True)
    print(f"SUBVENCIONES HISTÓRICAS (BDNS)", flush=True)
    print(f"   Rango: {params.fechaDesde} — {params.fechaHasta}", flush=True)
    print(f"   Endpoint: {API_ENDPOINT_SEARCH}", flush=True)
    print(f"{'='*60}", flush=True)

    try:
        # Fase 1: Obtener convocatorias ligeras
        _log("INFO", "Fase 1: Obteniendo convocatorias ligeras...")
        while not is_last_page:
            params.page = page

            rate_limiter.wait_if_needed()
            res = requests.get(API_ENDPOINT_SEARCH, params=params.to_dict())

            if res.status_code != 200:
                try:
                    error_data = res.json()
                    if "errores" in error_data:
                        error_msgs = "\n  - ".join(error_data["errores"])
                        raise ValueError(
                            f"Error de la API ({error_data.get('codigo', 'UNKNOWN')}):\n  - {error_msgs}"
                        )
                except ValueError:
                    raise
                except Exception:
                    res.raise_for_status()

            data = res.json()

            content = data.get("content", [])
            is_last_page = data.get("last", True)
            total_pages = data.get("totalPages", "?")

            if page == 0:
                total_elements = data.get("totalElements", 0)
                _log(
                    "INFO",
                    f"Total registros en API: {total_elements:,} ({total_pages} páginas)",
                )

            if not content:
                _log("WARN", f"Página {page + 1} vacía — fin de datos")
                break

            for item in content:
                light_convocatorias.append(
                    {
                        "id": item.get("id"),
                        "numero_convocatoria": item.get("numeroConvocatoria"),
                    }
                )

            pct = (
                (len(light_convocatorias) / total_elements * 100)
                if total_elements
                else 0
            )
            _log(
                "INFO",
                f"Página {page + 1}/{total_pages} — {len(light_convocatorias):,}/{total_elements:,} convocatorias ({pct:.0f}%)",
            )

            page += 1

        _log(
            "INFO",
            f"Fase 1 completada: {len(light_convocatorias):,} convocatorias obtenidas",
        )

        # Fase 2: Obtener detalles en paralelo usando ThreadPool
        _log(
            "INFO",
            f"Fase 2: Obteniendo detalles (ThreadPool con {max_workers} workers)...",
        )
        detailed_records = []
        failed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_conv = {
                executor.submit(
                    fetch_convocatoria_detalle,
                    conv["numero_convocatoria"],
                    rate_limiter,
                ): conv
                for conv in light_convocatorias
            }

            # Process completed tasks
            for i, future in enumerate(as_completed(future_to_conv), 1):
                try:
                    result = future.result()
                    if result:
                        detailed_records.append(result)
                    else:
                        failed_count += 1
                except Exception as e:
                    _log("ERROR", f"Excepción en thread: {e}")
                    failed_count += 1

                # Progress update every 100 records
                if i % 100 == 0 or i == len(light_convocatorias):
                    pct = i / len(light_convocatorias) * 100
                    _log(
                        "INFO",
                        f"Progreso: {i:,}/{len(light_convocatorias):,} ({pct:.1f}%) — Exitosos: {len(detailed_records):,}, Fallidos: {failed_count}",
                    )

        _log(
            "INFO",
            f"Fase 2 completada: {len(detailed_records):,} registros detallados obtenidos",
        )

        if failed_count > 0:
            _log("WARN", f"{failed_count} convocatorias fallaron al obtener detalles")

        if not detailed_records:
            raise ValueError("No se obtuvieron registros detallados")

        # Fase 3: Generar parquets en batches de MAX_RECORDS_PER_PARQUET
        _log(
            "INFO",
            f"Fase 3: Generando parquets (máximo {MAX_RECORDS_PER_PARQUET:,} registros por archivo)...",
        )
        parquet_paths = []

        for batch_idx in range(0, len(detailed_records), MAX_RECORDS_PER_PARQUET):
            batch_records = detailed_records[
                batch_idx : batch_idx + MAX_RECORDS_PER_PARQUET
            ]
            df = pd.DataFrame(batch_records)

            # Ensure JSONB columns are properly serialized
            jsonb_cols = [
                "instrumentos",
                "tipos_beneficiarios",
                "sectores",
                "regiones",
                "fondos",
                "reglamento",
                "objetivos",
                "sectores_productos",
                "documentos",
                "anuncios",
            ]
            for col in jsonb_cols:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if x is not None else None
                    )

            batch_num = batch_idx // MAX_RECORDS_PER_PARQUET
            parquet_filename = f"licitaciones_subvenciones_{ano_inicio}_{ano_fin}_batch{batch_num:03d}.parquet"
            parquet_path = OUTPUT_DIR / parquet_filename

            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            parquet_paths.append(parquet_path)

            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            _log(
                "INFO",
                f"Parquet generado: {parquet_path.name} ({len(df):,} registros, {size_mb:.1f} MB)",
            )

        print(f"\n✅ SCRAPING COMPLETADO: subvenciones históricas", flush=True)
        _log("INFO", f"Total archivos parquet: {len(parquet_paths)}")
        _log("INFO", f"Total registros: {len(detailed_records):,}")

        return parquet_paths

    except Exception as err:
        print(f"\n❌ ERROR: scraping histórico falló", flush=True)
        _log("ERROR", f"{err}")
        raise


def scrape_diario(params: LatestParams) -> dict[str, int]:
    """
    Scrape latest/daily grants and insert directly into database.
    This is for daily scheduler updates - small volume so direct insert is efficient.

    Fetches detailed information for each convocatoria sequentially (no ThreadPool needed
    for small volumes).

    Args:
        params: LatestParams for latest updates

    Returns:
        Dictionary with metrics: inserted, omitted, filtered, pages
    """
    params.validate()

    db_url = get_database_url()
    if not db_url:
        raise ValueError(
            "No se pudo obtener la URL de la base de datos. Verifica las variables de entorno."
        )

    conn = None
    total_new = 0
    total_duplicates = 0
    total_filtered = 0
    rate_limiter = RateLimiter(max_requests=49, time_window=60)
    subvencion_pattern = re.compile(r"subvenci[oó]n", re.IGNORECASE)

    print(f"\n{'='*60}", flush=True)
    print(f"SUBVENCIONES DIARIAS (BDNS)", flush=True)
    print(f"   Endpoint: {API_ENDPOINT_LATEST}", flush=True)
    print(f"   Modo: insert directo en BD con detalles completos", flush=True)
    print(f"{'='*60}", flush=True)

    try:
        conn = psycopg2.connect(db_url)
        _log("INFO", "Conexión a base de datos establecida")

        page = params.page
        is_last_page = False

        while not is_last_page:
            params.page = page

            rate_limiter.wait_if_needed()
            res = requests.get(API_ENDPOINT_LATEST, params=params.to_dict())

            if res.status_code != 200:
                try:
                    error_data = res.json()
                    if "errores" in error_data:
                        error_msgs = "\n  - ".join(error_data["errores"])
                        raise ValueError(
                            f"Error de la API ({error_data.get('codigo', 'UNKNOWN')}):\n  - {error_msgs}"
                        )
                except ValueError:
                    raise
                except Exception:
                    res.raise_for_status()

            data = res.json()

            content = data.get("content", [])
            is_last_page = data.get("last", True)
            total_pages = data.get("totalPages", "?")

            if not content:
                break

            # Filter and collect IDs
            page_filtered = 0
            light_convocatorias = []
            for item in content:
                descripcion = item.get("descripcion") or ""
                if not subvencion_pattern.search(descripcion):
                    page_filtered += 1
                    continue
                light_convocatorias.append(
                    {
                        "id": item.get("id"),
                        "numero_convocatoria": item.get("numeroConvocatoria"),
                    }
                )
            total_filtered += page_filtered

            if not light_convocatorias:
                page += 1
                continue

            # Check for duplicates
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM l0.nacional_subvenciones WHERE id = ANY(%s)",
                    ([conv["id"] for conv in light_convocatorias],),
                )
                existing_ids = {row[0] for row in cur.fetchall()}

                new_convocatorias = [
                    conv
                    for conv in light_convocatorias
                    if conv["id"] not in existing_ids
                ]
                page_dups = len(existing_ids)
                total_duplicates += page_dups

                if not new_convocatorias:
                    _log("INFO", f"Página {page + 1}: {page_dups} duplicados, 0 nuevos")
                    # If we found duplicates, stop searching (assuming chronological order)
                    if existing_ids:
                        break
                    page += 1
                    continue

                # Fetch details for new convocatorias (sequential, no ThreadPool)
                _log(
                    "INFO",
                    f"Obteniendo detalles para {len(new_convocatorias)} nuevas convocatorias...",
                )
                detailed_records = []
                for i, conv in enumerate(new_convocatorias, 1):
                    detail = fetch_convocatoria_detalle(
                        conv["numero_convocatoria"], rate_limiter
                    )
                    if detail:
                        detailed_records.append(detail)
                    else:
                        _log(
                            "WARN",
                            f"No se pudo obtener detalle para {conv['numero_convocatoria']}",
                        )

                    if i % 10 == 0:
                        _log("INFO", f"Progreso detalles: {i}/{len(new_convocatorias)}")

                if not detailed_records:
                    _log("WARN", "No se obtuvieron registros detallados válidos")
                    page += 1
                    continue

                # Insert detailed records
                insert_sql = """
                    INSERT INTO l0.nacional_subvenciones 
                    (id, nivel1, nivel2, nivel3, sede_electronica, codigo_bdns, fecha_recepcion,
                     instrumentos, tipo_convocatoria, presupuesto_total, mrr, descripcion, descripcion_leng,
                     tipos_beneficiarios, sectores, regiones, descripcion_finalidad, descripcion_bases_reguladoras,
                     url_bases_reguladoras, se_publica_diario_oficial, abierto, fecha_inicio_solicitud,
                     fecha_fin_solicitud, text_inicio, text_fin, ayuda_estado, url_ayuda_estado,
                     fondos, reglamento, objetivos, sectores_productos, documentos, anuncios, advertencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """

                records_to_insert = []
                for record in detailed_records:
                    # Serialize JSONB columns
                    jsonb_cols = [
                        "instrumentos",
                        "tipos_beneficiarios",
                        "sectores",
                        "regiones",
                        "fondos",
                        "reglamento",
                        "objetivos",
                        "sectores_productos",
                        "documentos",
                        "anuncios",
                    ]
                    for col in jsonb_cols:
                        if record.get(col) is not None:
                            record[col] = json.dumps(record[col])

                    records_to_insert.append(
                        (
                            record.get("id"),
                            record.get("nivel1"),
                            record.get("nivel2"),
                            record.get("nivel3"),
                            record.get("sede_electronica"),
                            record.get("codigo_bdns"),
                            record.get("fecha_recepcion"),
                            record.get("instrumentos"),
                            record.get("tipo_convocatoria"),
                            record.get("presupuesto_total"),
                            record.get("mrr"),
                            record.get("descripcion"),
                            record.get("descripcion_leng"),
                            record.get("tipos_beneficiarios"),
                            record.get("sectores"),
                            record.get("regiones"),
                            record.get("descripcion_finalidad"),
                            record.get("descripcion_bases_reguladoras"),
                            record.get("url_bases_reguladoras"),
                            record.get("se_publica_diario_oficial"),
                            record.get("abierto"),
                            record.get("fecha_inicio_solicitud"),
                            record.get("fecha_fin_solicitud"),
                            record.get("text_inicio"),
                            record.get("text_fin"),
                            record.get("ayuda_estado"),
                            record.get("url_ayuda_estado"),
                            record.get("fondos"),
                            record.get("reglamento"),
                            record.get("objetivos"),
                            record.get("sectores_productos"),
                            record.get("documentos"),
                            record.get("anuncios"),
                            record.get("advertencia"),
                        )
                    )

                execute_batch(cur, insert_sql, records_to_insert, page_size=100)
                conn.commit()
                total_new += len(records_to_insert)
                _log("INFO", f"Insertados {len(records_to_insert)} registros nuevos")

                # If we found duplicates, stop searching
                if existing_ids:
                    break

            page += 1

        print(f"\n✅ SCRAPING COMPLETADO: subvenciones diarias", flush=True)

        return {
            "inserted": total_new,
            "omitted": total_duplicates,
            "filtered": total_filtered,
            "pages": page + 1,
        }

    except Exception as err:
        print(f"\n❌ ERROR: scraping diario falló", flush=True)
        _log("ERROR", f"{err}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def main():
    """Main entry point compatible with nacional/licitaciones.py interface."""
    parser = argparse.ArgumentParser(description="Scraper de subvenciones públicas")
    parser.add_argument(
        "--anos", type=str, required=True, help="Rango de años (ej: 2020-2026 o 2023)"
    )
    parser.add_argument(
        "--conjunto",
        type=str,
        default="subvenciones",
        help="Conjunto de datos (subvenciones)",
    )
    parser.add_argument(
        "--solo-descargar",
        action="store_true",
        help="Solo descargar/generar Parquet, no cargar en BD",
    )

    args = parser.parse_args()

    # Parsear años (igual que licitaciones.py)
    partes = args.anos.split("-")
    ano_inicio = int(partes[0])

    if len(partes) > 1:
        ano_fin = int(partes[1])
        if ano_fin == datetime.now().year:
            fecha_hasta = datetime.now().strftime("%d/%m/%Y")
        else:
            fecha_hasta = f"31/12/{ano_fin}"
    else:
        fecha_hasta = datetime.now().strftime("%d/%m/%Y")

    fecha_desde = f"01/01/{ano_inicio}"

    print(f"   Fechas: {fecha_desde} - {fecha_hasta}")
    print(f"   Conjunto: {args.conjunto}")

    try:
        params = SearchParams(
            page=0,
            pageSize=10000,
            fechaDesde=fecha_desde,
            fechaHasta=fecha_hasta,
        )

        parquet_paths = scrape_historico(params)
        if args.solo_descargar:
            print(f"   (--solo-descargar: no se cargará en BD)")
            print(f"   Archivos generados: {len(parquet_paths)}")
            for path in parquet_paths:
                print(f"     - {path.name}")

        return 0

    except Exception:
        return 1


if __name__ == "__main__":
    exit(main())
