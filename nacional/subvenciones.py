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
import json
from tqdm import tqdm
import threading
from queue import Queue

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
API_ENDPOINT_DETAIL = API_BASE_URL

# Default VPD parameter for detail endpoint
DEFAULT_VPD = "GE"

# Maximum records per parquet file
MAX_RECORDS_PER_PARQUET = 100

# Directory configuration (similar to nacional/licitaciones.py)
_repo_root = Path(__file__).resolve().parent.parent
_tmp_base = Path(os.environ.get("LICITACIONES_TMP_DIR", _repo_root / "tmp"))
OUTPUT_DIR = _tmp_base / "output"

# Global adaptive delay for rate limit control (shared across all API calls)
_api_delay_seconds = 0  # Start with no delay, adjust only if rate limited
_api_delay_lock = threading.Lock()  # Thread-safe access to delay variable


def _ensure_output_dir():
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class SearchParams:
    """Search parameters for grants (subvenciones) API."""

    page: int = 0
    pageSize: int = 10000
    fechaDesde: str | None = None  # Format: "DD/MM/YYYY"
    fechaHasta: str = datetime.now().strftime("%d/%m/%Y")  # Format: "DD/MM/YYYY"
    vpd: str = field(default=DEFAULT_VPD, init=False)

    def to_dict(self) -> dict:
        """Convert to dictionary removing None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")
        if self.pageSize < 50 or self.pageSize > 10000:
            raise ValueError(f"pageSize debe estar entre 50-10000, recibido: {self.pageSize}")

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
            raise ValueError(f"{field_name} debe tener formato DD/MM/YYYY, recibido: {date_str}")


@dataclass
class LatestParams:
    """Parameters for latest grants endpoint (ultimas). pageSize and order are fixed."""

    page: int = 0
    pageSize: int = 100
    order: str = field(default="numeroConvocatoria", init=False)
    vpd: str = field(default=DEFAULT_VPD, init=False)

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
    organo: dict | None,
) -> tuple[str | None, str | None, str | None]:
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
) -> dict | None:
    """
    Fetch detailed information for a single convocatoria.
    Uses adaptive exponential delay to handle rate limiting automatically.
    Delay starts at 25ms and increases exponentially on 429 errors.

    Args:
        numero_convocatoria: The convocatoria number to fetch details for

    Returns:
        Dictionary with detailed convocatoria data or None if error
    """
    global _api_delay_seconds

    params = {
        "vpd": DEFAULT_VPD,
        "numConv": numero_convocatoria,
    }

    max_attempts = 10  # Maximum attempts before giving up

    for attempt in range(max_attempts):
        # Apply adaptive delay before request
        with _api_delay_lock:
            current_delay = _api_delay_seconds

        if current_delay > 0:
            time.sleep(current_delay)

        try:
            res = requests.get(API_ENDPOINT_DETAIL, params=params, timeout=30)

            if res.status_code == 429:
                # Increase delay on rate limit
                # First time: set to 25ms, then increase by ×1.6 factor
                with _api_delay_lock:
                    if _api_delay_seconds == 0:
                        _api_delay_seconds = 0.025  # First rate limit: start at 25ms
                    else:
                        _api_delay_seconds *= 1.6  # Subsequent: gradual exponential increase

                    new_delay = _api_delay_seconds

                _log(
                    "WARN",
                    f"HTTP 429 (rate limit), incrementando delay a {new_delay * 1000:.1f}ms (intento {attempt + 1}/{max_attempts})",
                )
                continue

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
                "sectores_productos": _convert_to_json_serializable(data.get("sectoresProductos")),
                "documentos": _convert_to_json_serializable(data.get("documentos")),
                "anuncios": _convert_to_json_serializable(data.get("anuncios")),
            }

            return record

        except Exception as e:
            _log(
                "ERROR",
                f"Excepción al obtener convocatoria {numero_convocatoria}: {e}",
            )
            return None

    # If we exhausted all attempts
    _log(
        "ERROR",
        f"No se pudo obtener convocatoria {numero_convocatoria} después de {max_attempts} intentos",
    )
    return None


def scrape_historico(params: SearchParams) -> list[Path]:
    """
    Scrape historical grants data and save to Parquet files.
    This is for initial bulk load - generates Parquet files for etl/ingest_l0.py to process.

    Generates multiple parquet files with max 20000 records each.
    Mode: Sequential (endpoint de detalles tiene rate limit de ~20 req/min).

    Args:
        params: SearchParams with date range (fechaDesde required, fechaHasta defaults to today)

    Returns:
        List of paths to generated Parquet files
    """
    if not params.fechaDesde:
        raise ValueError("fechaDesde es requerido para scrape historico")

    params.validate()

    _ensure_output_dir()

    _log("INFO", "=" * 60)
    _log("INFO", "SUBVENCIONES HISTÓRICAS (BDNS)")
    _log("INFO", f"Rango: {params.fechaDesde} — {params.fechaHasta}")
    _log("INFO", "=" * 60)

    try:
        light_convocatorias = []
        page = 0
        is_last_page = False

        _log("INFO", "Obteniendo lista de convocatorias...")
        with tqdm(desc="Páginas", unit="pag") as pbar:
            while not is_last_page:
                params.page = page
                res = requests.get(API_ENDPOINT_SEARCH, params=params.to_dict())

                if res.status_code != 200:
                    try:
                        error_data = res.json()
                        if "errores" in error_data:
                            error_msgs = "\n  - ".join(error_data["errores"])
                            raise ValueError(
                                f"Error de la API ({error_data.get('codigo', 'UNKNOWN')}):\n  {error_msgs}"
                            )
                    except ValueError:
                        raise
                    except Exception:
                        res.raise_for_status()

                data = res.json()
                content = data.get("content", [])
                is_last_page = data.get("last", True)

                if page == 0:
                    pbar.total = data.get("totalPages", 0)

                if not content:
                    break

                for item in content:
                    # Convert numeroConvocatoria to int for consistent type handling
                    numero_conv = item.get("numeroConvocatoria")
                    if numero_conv:
                        light_convocatorias.append(
                            {
                                "id": item.get("id"),
                                "numero_convocatoria": int(numero_conv),
                            }
                        )

                page += 1
                pbar.update(1)

        _log("INFO", f"{len(light_convocatorias):,} convocatorias obtenidas")
        _log(
            "INFO",
            "Obteniendo detalles y generando parquets (modo pipeline: 1 fetcher + 1 saver)...",
        )

        buffer = Queue(maxsize=100)
        stats_lock = threading.Lock()
        failed_count = 0
        parquet_paths = []
        batch_num = [0]  # Shared between threads
        total_saved = [0]  # Shared between threads
        fetching_done = threading.Event()

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

        def save_batch(records, batch_number):
            """Save current batch to parquet and clear buffer."""
            if not records:
                return None

            df = pd.DataFrame(records)

            for col in jsonb_cols:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

            parquet_filename = f"_part_subvenciones_{batch_number:03d}.parquet"
            parquet_path = OUTPUT_DIR / parquet_filename

            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            return parquet_path

        def fetcher():
            """Thread 1: fetch + preprocesar → buffer."""
            nonlocal failed_count
            for conv in tqdm(light_convocatorias, unit="conv", desc="Fetch+Preproc"):
                try:
                    # Convert back to string for API request
                    result = fetch_convocatoria_detalle(str(conv["numero_convocatoria"]))
                    if result:
                        buffer.put(result)
                    else:
                        with stats_lock:
                            failed_count += 1
                except Exception as e:
                    _log("ERROR", f"Excepción: {e}")
                    with stats_lock:
                        failed_count += 1
            fetching_done.set()

        def saver():
            """Thread 2: buffer → batch → guardar parquet."""
            batch_buffer = []

            while not (fetching_done.is_set() and buffer.empty()):
                try:
                    record = buffer.get(timeout=1)
                    batch_buffer.append(record)

                    if len(batch_buffer) >= MAX_RECORDS_PER_PARQUET:
                        with stats_lock:
                            current_batch = batch_num[0]
                            batch_num[0] += 1

                        parquet_path = save_batch(batch_buffer, current_batch)
                        if parquet_path:
                            parquet_paths.append(parquet_path)
                            with stats_lock:
                                total_saved[0] += len(batch_buffer)
                        batch_buffer = []

                    buffer.task_done()
                except:
                    pass

            # Guardar último batch
            if batch_buffer:
                with stats_lock:
                    current_batch = batch_num[0]
                    batch_num[0] += 1

                parquet_path = save_batch(batch_buffer, current_batch)
                if parquet_path:
                    parquet_paths.append(parquet_path)
                    with stats_lock:
                        total_saved[0] += len(batch_buffer)

        # Iniciar threads
        t1 = threading.Thread(target=fetcher)
        t2 = threading.Thread(target=saver)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        if failed_count > 0:
            _log("WARN", f"{failed_count} convocatorias fallaron")

        if not parquet_paths:
            raise ValueError("No se generaron parquets (sin registros válidos)")

        _log("INFO", f"{total_saved[0]:,} registros guardados en {len(parquet_paths)} parquets")

        return parquet_paths

    except Exception as err:
        _log("ERROR", str(err))
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
    cur = None
    total_new = 0
    total_duplicates = 0
    total_filtered = 0
    subvencion_pattern = re.compile(r"subvenci[oó]n", re.IGNORECASE)

    _log("INFO", "Actualizando subvenciones diarias...")

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()  # Open cursor once at the start

        page = params.page
        is_last_page = False

        while not is_last_page:
            params.page = page

            res = requests.get(API_ENDPOINT_LATEST, params=params.to_dict())

            if res.status_code != 200:
                try:
                    error_data = res.json()
                    if "errores" in error_data:
                        error_msgs = "\n  - ".join(error_data["errores"])
                        raise ValueError(
                            f"Error de la API ({error_data.get('codigo', 'UNKNOWN')}):\n msj error: {error_msgs}"
                        )
                except ValueError:
                    raise
                except Exception:
                    res.raise_for_status()

            data = res.json()

            content = data.get("content", [])
            is_last_page = data.get("last", True)

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
                # Convert numeroConvocatoria to int for consistent type comparison
                numero_conv = item.get("numeroConvocatoria")
                if numero_conv:
                    light_convocatorias.append(
                        {
                            "id": item.get("id"),
                            "numero_convocatoria": int(numero_conv),
                        }
                    )
            total_filtered += page_filtered

            if not light_convocatorias:
                page += 1
                continue

            # Check for duplicates using the persistent cursor
            cur.execute(
                "SELECT id FROM l0.nacional_subvenciones WHERE id = ANY(%s)",
                ([conv["numero_convocatoria"] for conv in light_convocatorias],),
            )
            existing_ids = {row[0] for row in cur.fetchall()}

            new_convocatorias = [
                conv
                for conv in light_convocatorias
                if conv["numero_convocatoria"] not in existing_ids
            ]
            page_dups = len(existing_ids)
            total_duplicates += page_dups

            # Show progress message for this page
            _log(
                "INFO",
                f"Para la página número {page + 1} se han encontrado {len(new_convocatorias)} convocatorias nuevas",
            )

            if not new_convocatorias:
                # If we found duplicates, stop searching
                if existing_ids:
                    _log("INFO", "Ya se encontraron todas las convocatorias recientes")
                    break
                page += 1
                continue

            detailed_records = []
            for conv in new_convocatorias:
                # Convert back to string for API request
                detail = fetch_convocatoria_detalle(str(conv["numero_convocatoria"]))
                if detail:
                    detailed_records.append(detail)

            if not detailed_records:
                page += 1
                continue

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
                        record.get("regiones"),
                        record.get("sectores"),
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
                    )
                )

            # Insert using the persistent cursor
            insert_sql = """
                INSERT INTO l0.nacional_subvenciones 
                (id, nivel1, nivel2, nivel3, sede_electronica, codigo_bdns, fecha_recepcion,
                 instrumentos, tipo_convocatoria, presupuesto_total, mrr, descripcion, descripcion_leng,
                 tipos_beneficiarios, sectores, regiones, descripcion_finalidad, descripcion_bases_reguladoras,
                 url_bases_reguladoras, se_publica_diario_oficial, abierto, fecha_inicio_solicitud,
                 fecha_fin_solicitud, text_inicio, text_fin, ayuda_estado, url_ayuda_estado,
                 fondos, reglamento, objetivos, sectores_productos, documentos, anuncios)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """

            execute_batch(cur, insert_sql, records_to_insert)
            conn.commit()

            _log("INFO", f"  Insertados {len(records_to_insert)} registros en BD")
            total_new += len(records_to_insert)

            if existing_ids:
                _log("INFO", "Ya se encontraron todas las convocatorias recientes")
                break

            page += 1

        _log(
            "INFO",
            f"COMPLETADO: {total_new} nuevos, {total_duplicates} duplicados, {total_filtered} filtrados",
        )

        return {
            "inserted": total_new,
            "omitted": total_duplicates,
            "filtered": total_filtered,
            "pages": page + 1,
        }

    except Exception as err:
        _log("ERROR", str(err))
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            if cur:
                cur.close()
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
            # fecha_hasta = datetime.now().strftime("%d/%m/%Y")
            fecha_hasta = "19/04/2026"
        else:
            fecha_hasta = f"31/12/{ano_fin}"
    else:
        fecha_hasta = datetime.now().strftime("%d/%m/%Y")

    # fecha_desde = f"01/01/{ano_inicio}"
    fecha_desde = f"16/04/2026"
    _log("INFO", f"Fechas: {fecha_desde} - {fecha_hasta}")
    _log("INFO", f"Conjunto: {args.conjunto}")

    try:
        params = SearchParams(
            page=0,
            pageSize=10000,
            fechaDesde=fecha_desde,
            fechaHasta=fecha_hasta,
        )

        parquet_paths = scrape_historico(params)
        if args.solo_descargar:
            _log("INFO", "(--solo-descargar: no se cargará en BD)")
            _log("INFO", f"Archivos generados: {len(parquet_paths)}")
            for path in parquet_paths:
                _log("INFO", f"  - {path.name}")

        return 0

    except Exception:
        return 1


if __name__ == "__main__":
    exit(main())
