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


def _ensure_output_dir():
    """Ensure output directory exists."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class RateLimiter:
    """Manage API rate limiting to avoid exceeding request limits and get API ban."""

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

    def wait_if_needed(self):
        """Wait if necessary to avoid exceeding rate limit."""
        now = time.time()

        # Remove requests outside the current time window
        self.request_times = [t for t in self.request_times if now - t < self.time_window]

        # Check if we've reached the limit
        if len(self.request_times) >= self.max_requests:
            # Calculate how long to wait
            oldest_request = self.request_times[0]
            wait_time = self.time_window - (now - oldest_request)

            if wait_time > 0:
                time.sleep(wait_time + 0.1)
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < self.time_window]

        self.request_times.append(time.time())

    def reset(self):
        """Reset the rate limiter."""
        self.request_times = []


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
    rate_limiter: RateLimiter,
) -> dict | None:
    """
    Fetch detailed information for a single convocatoria.

    Args:
        numero_convocatoria: The convocatoria number to fetch details for
        rate_limiter: RateLimiter instance

    Returns:
        Dictionary with detailed convocatoria data or None if error
    """
    params = {
        "vpd": DEFAULT_VPD,
        "numConv": numero_convocatoria,
    }

    try:
        rate_limiter.wait_if_needed()
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
            "tipos_beneficiarios": _convert_to_json_serializable(data.get("tiposBeneficiarios")),
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
            "advertencia": data.get("advertencia"),
        }

        return record

    except Exception as e:
        _log(
            "ERROR",
            f"Excepción al obtener detalle de convocatoria {numero_convocatoria}: {e}",
        )
        return None


def scrape_historico(params: SearchParams) -> list[Path]:
    """
    Scrape historical grants data and save to Parquet files.
    This is for initial bulk load - generates Parquet files for etl/ingest_l0.py to process.

    Fetches detailed information for each convocatoria sequentially (no parallelism needed
    due to strict API rate limiting of 50 req/min).
    Generates multiple parquet files with max 20000 records each.

    Args:
        params: SearchParams with date range (fechaDesde required, fechaHasta defaults to today)

    Returns:
        List of paths to generated Parquet files
    """
    if not params.fechaDesde:
        raise ValueError("fechaDesde es requerido para scrape historico")

    params.validate()

    _ensure_output_dir()
    rate_limiter = RateLimiter(max_requests=59, time_window=60)

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
                rate_limiter.wait_if_needed()
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
                    light_convocatorias.append(
                        {
                            "id": item.get("id"),
                            "numero_convocatoria": item.get("numeroConvocatoria"),
                        }
                    )

                page += 1
                pbar.update(1)

        _log("INFO", f"{len(light_convocatorias):,} convocatorias obtenidas")

        _log("INFO", f"Obteniendo detalles y generando parquets...")

        batch_buffer = []
        failed_count = 0
        parquet_paths = []
        batch_num = 0
        total_saved = 0

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

            size_mb = parquet_path.stat().st_size / (1024 * 1024)
            _log(
                "INFO",
                f"{parquet_path.name} ({len(df):,} registros guardados, {size_mb:.1f} MB)",
            )
            return parquet_path

        for conv in tqdm(light_convocatorias, unit="conv"):
            try:
                result = fetch_convocatoria_detalle(conv["numero_convocatoria"], rate_limiter)
                if result:
                    batch_buffer.append(result)

                    if len(batch_buffer) >= MAX_RECORDS_PER_PARQUET:
                        parquet_path = save_batch(batch_buffer, batch_num)
                        if parquet_path:
                            parquet_paths.append(parquet_path)
                            total_saved += len(batch_buffer)
                        batch_buffer = []
                        batch_num += 1
                else:
                    failed_count += 1
            except Exception as e:
                _log("ERROR", f"Excepción: {e}")
                failed_count += 1

        if batch_buffer:
            parquet_path = save_batch(batch_buffer, batch_num)
            if parquet_path:
                parquet_paths.append(parquet_path)
                total_saved += len(batch_buffer)

        if failed_count > 0:
            _log("WARN", f"{failed_count} convocatorias fallaron")

        if not parquet_paths:
            raise ValueError("No se generaron parquets (sin registros válidos)")

        _log("INFO", f"{total_saved:,} registros guardados en {len(parquet_paths)} parquets")

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
    total_new = 0
    total_duplicates = 0
    total_filtered = 0
    rate_limiter = RateLimiter(max_requests=49, time_window=60)
    subvencion_pattern = re.compile(r"subvenci[oó]n", re.IGNORECASE)

    _log("INFO", "Actualizando subvenciones diarias...")

    try:
        conn = psycopg2.connect(db_url)

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
                    conv for conv in light_convocatorias if conv["id"] not in existing_ids
                ]
                page_dups = len(existing_ids)
                total_duplicates += page_dups

                if not new_convocatorias:
                    # If we found duplicates, stop searching
                    if existing_ids:
                        break
                    page += 1
                    continue

                # Fetch details for new convocatorias (sequential, no ThreadPool)
                detailed_records = []
                _log("INFO", "Obteniendo detalles de convocatorias...")
                for conv in tqdm(new_convocatorias, unit="conv"):
                    detail = fetch_convocatoria_detalle(conv["numero_convocatoria"], rate_limiter)
                    if detail:
                        detailed_records.append(detail)

                if not detailed_records:
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

                execute_batch(cur, insert_sql, records_to_insert)
                conn.commit()
                total_new += len(records_to_insert)

                if existing_ids:
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
            fecha_hasta = "18/04/2026"
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
