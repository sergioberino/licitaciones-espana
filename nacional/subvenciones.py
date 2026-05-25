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
API_ENDPOINT_DETAIL = API_BASE_URL

# Default VPD parameter for detail endpoint
DEFAULT_VPD = "GE"

# Maximum records per parquet file
MAX_RECORDS_PER_PARQUET = 10000

# Directory configuration (similar to nacional/licitaciones.py)
_repo_root = Path(__file__).resolve().parent.parent
_tmp_base = Path(os.environ.get("LICITACIONES_TMP_DIR", _repo_root / "tmp"))
OUTPUT_DIR = _tmp_base / "output"

# Global adaptive delay for rate limit control (shared across all API calls)
_api_delay_seconds = 0  # Start with no delay, adjust only if rate limited
_api_delay_lock = threading.Lock()  # Thread-safe access to delay variable

# Cache para mapeo de instrumentos (cargado desde BD)
_instrumentos_map_cache = None
_instrumentos_map_lock = threading.Lock()

# Cache para mapeo de beneficiarios (cargado desde BD)
_beneficiarios_map_cache = None
_beneficiarios_map_lock = threading.Lock()

# Cache para mapeo de política de gastos (cargado desde BD)
_politica_gastos_map_cache = None
_politica_gastos_map_lock = threading.Lock()


def _load_instrumentos_map() -> dict[str, int]:
    """
    Load instrumentos mapping from database.
    Cached globally to avoid repeated queries.

    Returns:
        Dictionary mapping descripcion -> id
    """
    global _instrumentos_map_cache

    with _instrumentos_map_lock:
        if _instrumentos_map_cache is not None:
            return _instrumentos_map_cache

        try:
            conn = psycopg2.connect(get_database_url())
            cur = conn.cursor()
            cur.execute("SELECT id, descripcion FROM dim.instrumentos_subvenciones")
            rows = cur.fetchall()
            cur.close()
            conn.close()

            # Create mapping: descripcion (stripped) -> id
            _instrumentos_map_cache = {row[1].strip(): row[0] for row in rows}
            return _instrumentos_map_cache

        except Exception as e:
            _log("ERROR", f"Error cargando instrumentos desde BD: {e}")
            return {}


def _load_beneficiarios_map() -> dict[str, int]:
    """
    Load beneficiarios mapping from database.
    Cached globally to avoid repeated queries.

    Returns:
        Dictionary mapping descripcion -> id
    """
    global _beneficiarios_map_cache

    with _beneficiarios_map_lock:
        if _beneficiarios_map_cache is not None:
            return _beneficiarios_map_cache

        try:
            conn = psycopg2.connect(get_database_url())
            cur = conn.cursor()
            cur.execute("SELECT id, descripcion FROM dim.beneficiarios_subvenciones")
            rows = cur.fetchall()
            cur.close()
            conn.close()

            # Create mapping: descripcion (stripped) -> id
            _beneficiarios_map_cache = {row[1].strip(): row[0] for row in rows}
            return _beneficiarios_map_cache

        except Exception as e:
            _log("ERROR", f"Error cargando beneficiarios desde BD: {e}")
            return {}


def _load_politica_gastos_map() -> dict[str, int]:
    """
    Load politica_gastos mapping from database.
    Cached globally to avoid repeated queries.

    Returns:
        Dictionary mapping descripcion -> id
    """
    global _politica_gastos_map_cache

    with _politica_gastos_map_lock:
        if _politica_gastos_map_cache is not None:
            return _politica_gastos_map_cache

        try:
            conn = psycopg2.connect(get_database_url())
            cur = conn.cursor()
            cur.execute("SELECT id, descripcion FROM dim.politica_gastos")
            rows = cur.fetchall()
            cur.close()
            conn.close()

            # Create mapping: descripcion uppercased -> id (API returns Title Case, dim table is UPPERCASE)
            _politica_gastos_map_cache = {row[1].strip().upper(): row[0] for row in rows}
            return _politica_gastos_map_cache

        except Exception as e:
            _log("ERROR", f"Error cargando politica_gastos desde BD: {e}")
            return {}


def _extract_politica_gastos_id(descripcion_finalidad) -> int | None:
    """
    Map descripcionFinalidad text from API to ID in dim.politica_gastos.

    Args:
        descripcion_finalidad: Text string from API (e.g., "JUSTICIA")

    Returns:
        Integer ID or None if not found
    """
    if not descripcion_finalidad or not isinstance(descripcion_finalidad, str):
        return None

    politica_gastos_map = _load_politica_gastos_map()
    return politica_gastos_map.get(descripcion_finalidad.strip().upper())


def _extract_instrumento_id(instrumentos_array) -> int | None:
    """
    Extract instrumento_id from API instrumentos array.
    Takes the first element's descripcion and maps it to ID from database.

    Args:
        instrumentos_array: Array from API (e.g., [{"descripcion": "..."}])

    Returns:
        Integer ID or None if not found
    """
    if not instrumentos_array or not isinstance(instrumentos_array, list):
        return None

    if len(instrumentos_array) == 0:
        return None

    first_instrumento = instrumentos_array[0]
    if not isinstance(first_instrumento, dict):
        return None

    descripcion = first_instrumento.get("descripcion", "").strip()
    instrumentos_map = _load_instrumentos_map()
    return instrumentos_map.get(descripcion)


def _extract_beneficiarios_ids(beneficiarios_array) -> list[int] | None:
    """
    Extract beneficiarios IDs from API tipos_beneficiarios array.
    Maps all descriptions to their IDs.

    Args:
        beneficiarios_array: Array from API (e.g., [{"descripcion": "..."}])

    Returns:
        List of integer IDs or None if empty
    """
    if not beneficiarios_array or not isinstance(beneficiarios_array, list):
        return None

    if len(beneficiarios_array) == 0:
        return None

    beneficiarios_map = _load_beneficiarios_map()
    ids = []

    for item in beneficiarios_array:
        if isinstance(item, dict):
            descripcion = item.get("descripcion", "").strip()
            beneficiario_id = beneficiarios_map.get(descripcion)
            if beneficiario_id:
                ids.append(beneficiario_id)

    return ids if ids else None


def _extract_nut_codes(regiones_array) -> list[str] | None:
    """
    Extract NUT codes (geographic regions) from API regiones array.

    Args:
        regiones_array: Array from API (e.g., [{"descripcion": "ES41 - CASTILLA Y LEON"}])

    Returns:
        List of NUT codes or None if empty
    """
    if not regiones_array or not isinstance(regiones_array, list):
        return None

    if len(regiones_array) == 0:
        return None

    nut_codes = []
    for item in regiones_array:
        if isinstance(item, dict):
            descripcion = item.get("descripcion", "").strip()
            # Extract NUT code (format: "ES618 - Sevilla" -> "ES618")
            if " - " in descripcion:
                nut_code = descripcion.split(" - ")[0].strip()
                if nut_code:
                    nut_codes.append(nut_code)

    return nut_codes if nut_codes else None


def _extract_sector_codes(sectores_array) -> list[str] | None:
    """
    Extract sector CNAE codes (economic sectors) from API sectores array.
    Normalizes decimal codes: '96.4' -> '9640', '96.18' -> '9618'

    Args:
        sectores_array: Array from API (e.g., [{"codigo": "I", "descripcion": "HOSTELERÍA"}])

    Returns:
        List of normalized sector codes or None if empty
    """
    if not sectores_array or not isinstance(sectores_array, list):
        return None

    if len(sectores_array) == 0:
        return None

    sector_codes = []
    for item in sectores_array:
        if isinstance(item, dict):
            codigo = item.get("codigo", "").strip()
            if codigo:
                # Normalizar códigos con decimal: quitar el punto (ej: '86.6' -> '866', '96.18' -> '9618')
                if "." in codigo:
                    codigo = codigo.replace(".", "")

                sector_codes.append(codigo)

    return sector_codes if sector_codes else None


def _extract_reglamento_descripcion(reglamento_obj) -> str | None:
    """
    Extract descripcion from reglamento object.

    Args:
        reglamento_obj: Object from API (e.g., {"orden": null, "descripcion": "..."})

    Returns:
        String with descripcion or None
    """
    if not reglamento_obj or not isinstance(reglamento_obj, dict):
        return None

    descripcion = reglamento_obj.get("descripcion", "").strip()
    return descripcion if descripcion else None


def _extract_objetivos_descripcion(objetivos_array) -> str | None:
    """
    Extract descripcion from first objetivo in array.

    Args:
        objetivos_array: Array from API (e.g., [{"descripcion": "..."}])

    Returns:
        String with first descripcion or None
    """
    if not objetivos_array or not isinstance(objetivos_array, list):
        return None

    if len(objetivos_array) == 0:
        return None

    first_objetivo = objetivos_array[0]
    if isinstance(first_objetivo, dict):
        descripcion = first_objetivo.get("descripcion", "").strip()
        return descripcion if descripcion else None

    return None


def _normalize_empty_jsonb(value):
    """
    Convert empty arrays [] to None for cleaner database storage.

    Args:
        value: JSONB value (typically array or dict)

    Returns:
        None if empty array, otherwise the original value
    """
    if isinstance(value, list) and len(value) == 0:
        return None
    return value


def _ensure_output_dir():
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class SearchParams:
    """Search parameters for grants (subvenciones) API."""

    page: int = 0
    pageSize: int = 10000
    fechaDesde: str | None = None  # Format: "DD/MM/YYYY"
    fechaHasta: str | None = None  # Format: "DD/MM/YYYY", defaults to today if None
    direccion: str = "asc"  # Sort direction: "asc" or "desc"
    vpd: str = field(default=DEFAULT_VPD, init=False)

    def __post_init__(self):
        """Set default fechaHasta to current date if not provided."""
        if self.fechaHasta is None:
            self.fechaHasta = datetime.now().strftime("%d/%m/%Y")

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


def _is_expired_grant(record: dict) -> bool:
    """
    Returns True if the grant should be discarded:
    - abierto is False AND fecha_fin_solicitud < today
    Records without fecha_fin_solicitud are kept.
    """
    if record.get("abierto"):
        return False
    fecha_fin = record.get("fecha_fin_solicitud")
    if not fecha_fin:
        return False
    try:
        # API returns YYYY-MM-DD, compare as ISO string (lexicographic order works)
        return fecha_fin < datetime.now().strftime("%Y-%m-%d")
    except Exception:
        return False


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
                # codigoBDNS = numeroConvocatoria en datos ligeros
                "id": data.get("codigoBDNS"),
                "nivel1": nivel1,
                "nivel2": nivel2,
                "nivel3": nivel3,
                "sede_electronica": data.get("sedeElectronica"),
                "fecha_recepcion": data.get("fechaRecepcion"),
                "instrumento_id": _extract_instrumento_id(data.get("instrumentos")),
                "tipo_convocatoria": data.get("tipoConvocatoria"),
                "presupuesto_total": data.get("presupuestoTotal"),
                "mrr": data.get("mrr"),
                "descripcion": data.get("descripcion"),
                "descripcion_leng": data.get("descripcionLeng"),
                # Extract arrays
                "tipos_beneficiarios": _extract_beneficiarios_ids(data.get("tiposBeneficiarios")),
                "sectores": _extract_sector_codes(
                    data.get("sectores")
                ),  # Extract CNAE codes from API 'sectores'
                "regiones": _extract_nut_codes(
                    data.get("regiones")
                ),  # Extract NUT codes from API 'regiones'
                "politica_gastos": _extract_politica_gastos_id(data.get("descripcionFinalidad")),
                "descripcion_bases_reguladoras": data.get("descripcionBasesReguladoras"),
                "url_bases_reguladoras": data.get("urlBasesReguladoras"),
                "se_publica_diario_oficial": data.get("sePublicaDiarioOficial"),
                "abierto": data.get("abierto"),
                "fecha_inicio_solicitud": data.get("fechaInicioSolicitud"),
                "fecha_fin_solicitud": data.get("fechaFinSolicitud"),
                "fecha_inicio_solicitud_texto": data.get("textInicio"),
                "fecha_fin_solicitud_texto": data.get("textFin"),
                "ayuda_estado": data.get("ayudaEstado"),
                "url_ayuda_estado": data.get("urlAyudaEstado"),
                "fondos": _normalize_empty_jsonb(_convert_to_json_serializable(data.get("fondos"))),
                "reglamento": _extract_reglamento_descripcion(data.get("reglamento")),
                "objetivos": _extract_objetivos_descripcion(data.get("objetivos")),
                "sectores_productos": _normalize_empty_jsonb(
                    _convert_to_json_serializable(data.get("sectoresProductos"))
                ),
                "documentos": _normalize_empty_jsonb(
                    _convert_to_json_serializable(data.get("documentos"))
                ),
                "anuncios": _normalize_empty_jsonb(
                    _convert_to_json_serializable(data.get("anuncios"))
                ),
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

    _log("INFO", "SUBVENCIONES HISTÓRICAS (BDNS)")
    _log("INFO", f"Rango: {params.fechaDesde} — {params.fechaHasta}")

    try:
        light_convocatorias = []
        page = 0
        is_last_page = False

        _log("INFO", "Obteniendo lista de convocatorias...")
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

            if not content:
                break

            for item in content:
                # Convert numeroConvocatoria to int as id
                numero_conv = item.get("numeroConvocatoria")
                if numero_conv:
                    light_convocatorias.append({"id": int(numero_conv)})

            page += 1

        _log("INFO", f"{len(light_convocatorias):,} convocatorias obtenidas\n")
        _log(
            "INFO",
            "Obteniendo detalles y generando parquets...",
        )

        buffer = Queue(maxsize=100)
        stats_lock = threading.Lock()
        failed_count = 0
        parquet_paths = []
        batch_num = [0]  # Shared between threads
        total_saved = [0]  # Shared between threads
        skipped_expired = [0]  # Shared between threads
        fetching_done = threading.Event()

        # Only these columns need JSON serialization now
        jsonb_cols = [
            "fondos",
            "sectores_productos",
            "documentos",
            "anuncios",
        ]

        def save_batch(records, batch_number):
            """Save current batch to parquet and clear buffer."""
            if not records:
                return None

            df = pd.DataFrame(records)

            # Serialize JSONB columns
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
            for conv in tqdm(light_convocatorias, unit="conv"):
                try:
                    # Convert id to string for API request
                    result = fetch_convocatoria_detalle(str(conv["id"]))
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
                    if _is_expired_grant(record):
                        skipped_expired[0] += 1
                        buffer.task_done()
                        continue
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

        if skipped_expired[0] > 0:
            _log("INFO", f"{skipped_expired[0]} convocatorias descartadas (cerradas y caducadas)")

        if not parquet_paths:
            raise ValueError("No se generaron parquets (sin registros válidos)")

        _log("INFO", f"{total_saved[0]:,} registros guardados en {len(parquet_paths)} parquets")

        return parquet_paths

    except Exception as err:
        _log("ERROR", str(err))
        raise


# ---------------------------------------------------------------------------
# Helpers compartidos de inserción directa en BD
# ---------------------------------------------------------------------------

_INSERT_SQL = """
    INSERT INTO l0.nacional_subvenciones
    (id, nivel1, nivel2, nivel3, sede_electronica, fecha_recepcion,
     instrumento_id, tipo_convocatoria, presupuesto_total, mrr, descripcion, descripcion_leng,
     tipos_beneficiarios, sectores, regiones, politica_gastos, descripcion_bases_reguladoras,
     url_bases_reguladoras, se_publica_diario_oficial, abierto, fecha_inicio_solicitud,
     fecha_fin_solicitud, fecha_inicio_solicitud_texto, fecha_fin_solicitud_texto, ayuda_estado, url_ayuda_estado,
     fondos, reglamento, objetivos, sectores_productos, documentos, anuncios)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
"""

_JSONB_COLS = ["fondos", "sectores_productos", "documentos", "anuncios"]


def _record_to_row(record: dict) -> tuple:
    """Serializa las columnas JSONB y devuelve la tupla lista para INSERT."""
    for col in _JSONB_COLS:
        if record.get(col) is not None:
            record[col] = json.dumps(record[col])
    return (
        record.get("id"),
        record.get("nivel1"),
        record.get("nivel2"),
        record.get("nivel3"),
        record.get("sede_electronica"),
        record.get("fecha_recepcion"),
        record.get("instrumento_id"),
        record.get("tipo_convocatoria"),
        record.get("presupuesto_total"),
        record.get("mrr"),
        record.get("descripcion"),
        record.get("descripcion_leng"),
        record.get("tipos_beneficiarios"),
        record.get("sectores"),
        record.get("regiones"),
        record.get("politica_gastos"),
        record.get("descripcion_bases_reguladoras"),
        record.get("url_bases_reguladoras"),
        record.get("se_publica_diario_oficial"),
        record.get("abierto"),
        record.get("fecha_inicio_solicitud"),
        record.get("fecha_fin_solicitud"),
        record.get("fecha_inicio_solicitud_texto"),
        record.get("fecha_fin_solicitud_texto"),
        record.get("ayuda_estado"),
        record.get("url_ayuda_estado"),
        record.get("fondos"),
        record.get("reglamento"),
        record.get("objetivos"),
        record.get("sectores_productos"),
        record.get("documentos"),
        record.get("anuncios"),
    )


def _fetch_and_insert(cur, conn, convocatorias: list[dict]) -> int:
    """
    Obtiene el detalle de cada convocatoria (lista de dicts con clave 'id'),
    construye las filas y las inserta en BD. Devuelve el número de filas insertadas.

    Se omiten automáticamente las convocatorias expiradas y las que fallen en la API.
    La deduplicación previa (filtrar IDs ya existentes) es responsabilidad del llamante.
    """
    detailed_records = []
    for conv in tqdm(convocatorias, unit="conv"):
        detail = fetch_convocatoria_detalle(str(conv["id"]))
        if detail:
            detailed_records.append(detail)

    if not detailed_records:
        return 0

    rows = [_record_to_row(r) for r in detailed_records]
    execute_batch(cur, _INSERT_SQL, rows, page_size=100)
    conn.commit()
    return len(rows)


def scrape_especificas(ids: list[int]) -> dict[str, int]:
    """
    Inserta en BD los numConv que aún no existan.
    Los que ya estén en la tabla se descartan silenciosamente.

    Args:
        ids: Lista de números de convocatoria (numConv / id en BD).

    Returns:
        Diccionario con métricas: inserted, omitted.
    """
    if not ids:
        return {"inserted": 0, "omitted": 0}

    db_url = get_database_url()
    if not db_url:
        raise ValueError("No se pudo obtener la URL de la base de datos.")

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute(
            "SELECT id FROM l0.nacional_subvenciones WHERE id = ANY(%s)",
            (ids,),
        )
        existing_ids = {row[0] for row in cur.fetchall()}
        nuevos = [{"id": i} for i in ids if i not in existing_ids]

        omitted = len(existing_ids)
        if not nuevos:
            _log("INFO", f"Todos los IDs ya existen en BD ({omitted} omitidos)")
            return {"inserted": 0, "omitted": omitted}

        _log("INFO", f"{len(nuevos)} convocatorias nuevas, {omitted} ya existentes omitidas")
        inserted = _fetch_and_insert(cur, conn, nuevos)
        return {"inserted": inserted, "omitted": omitted}

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


def scrape_diario(params: SearchParams) -> dict[str, int]:
    """
    Scrape latest/daily grants and insert directly into database.
    This is for daily scheduler updates - small volume so direct insert is efficient.

    Uses the same search endpoint as scrape_historico but without date filters,
    fetching the most recent records until duplicates are found.

    Args:
        params: SearchParams for search (fechaDesde should be None for daily updates)

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

    _log("INFO", "Obteniendo convocatorias nuevas...")
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Get the most recent fecha_recepcion from DB to use as fechaDesde
        # Order by id DESC (indexed) for fast lookup - higher id = more recent
        cur.execute("SELECT fecha_recepcion FROM l0.nacional_subvenciones ORDER BY id DESC LIMIT 1")
        result = cur.fetchone()
        max_fecha = result[0] if result and result[0] else None

        if max_fecha:
            # Set fechaDesde to the most recent date in DB
            params.fechaDesde = max_fecha.strftime("%d/%m/%Y")
            _log("INFO", f"Buscando desde fecha más reciente en BD: {params.fechaDesde}")
        else:
            raise ValueError(
                "No hay registros en la base de datos. "
                "Ejecuta primero '... licitia-etl ingest nacional subvenciones --anos YYYY-YYYY' "
                "para cargar datos históricos antes de realizar el scrape diario."
            )

        # Set fechaHasta to current date
        params.fechaHasta = datetime.now().strftime("%d/%m/%Y")

        # Set direccion to desc to get most recent first
        params.direccion = "desc"

        all_new_convocatorias = []
        page = params.page
        is_last_page = False
        found_duplicates = False

        while not is_last_page and not found_duplicates:
            params.page = page

            res = requests.get(API_ENDPOINT_SEARCH, params=params.to_dict())

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
                numero_conv = item.get("numeroConvocatoria")
                if numero_conv:
                    light_convocatorias.append({"id": int(numero_conv)})
            total_filtered += page_filtered

            if not light_convocatorias:
                page += 1
                continue

            # Check for duplicates
            cur.execute(
                "SELECT id FROM l0.nacional_subvenciones WHERE id = ANY(%s)",
                ([conv["id"] for conv in light_convocatorias],),
            )
            existing_ids = {row[0] for row in cur.fetchall()}

            new_convocatorias = [
                conv for conv in light_convocatorias if conv["id"] not in existing_ids
            ]
            total_duplicates += len(existing_ids)

            # Add new convocatorias to the accumulated list
            all_new_convocatorias.extend(new_convocatorias)

            # Stop if we found duplicates (means we reached already processed data)
            if existing_ids:
                found_duplicates = True
                break

            page += 1

        if not all_new_convocatorias:
            _log("INFO", "No hay convocatorias nuevas")
            return {
                "inserted": 0,
                "omitted": total_duplicates,
                "filtered": total_filtered,
                "pages": page + 1,
            }

        _log(
            "INFO",
            f"{len(all_new_convocatorias)} convocatorias nuevas encontradas",
        )

        # FASE 2: Obtener detalles de todas las convocatorias e insertar
        _log("INFO", "Obteniendo detalles de las nuevas convocatorias encontradas...")
        total_new = _fetch_and_insert(cur, conn, all_new_convocatorias)

        _log("INFO", f"Actualizado con {total_new} convocatorias más recientes en BD")

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
            fecha_hasta = datetime.now().strftime("%d/%m/%Y")
        else:
            fecha_hasta = f"31/12/{ano_fin}"
    else:
        fecha_hasta = datetime.now().strftime("%d/%m/%Y")

    fecha_desde = f"01/01/{ano_inicio}"

    try:
        params = SearchParams(
            page=0,
            pageSize=10000,
            fechaDesde=fecha_desde,
            fechaHasta=fecha_hasta,
            direccion="asc",
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
