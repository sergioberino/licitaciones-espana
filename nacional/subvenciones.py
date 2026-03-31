import requests
import psycopg2
from psycopg2.extras import execute_batch
from dataclasses import dataclass, asdict, field
from datetime import datetime
import sys
from pathlib import Path
import time
from tqdm import tqdm
import pandas as pd
import os
import argparse

# Add parent directory to path to import etl modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import get_database_url

# No API_KEY required - public access
API_BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias"
API_ENDPOINT_SEARCH = API_BASE_URL + "/busqueda"
API_ENDPOINT_LATEST = API_BASE_URL + "/ultimas"

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
        self.request_times = [
            t for t in self.request_times if now - t < self.time_window
        ]

        # Check if we've reached the limit
        if len(self.request_times) >= self.max_requests:
            # Calculate how long to wait
            oldest_request = self.request_times[0]
            wait_time = self.time_window - (now - oldest_request)

            if wait_time > 0:
                print(
                    f"[WARN] Limite de peticiones alcanzado. Esperando {wait_time:.1f}s..."
                )
                time.sleep(wait_time + 0.1)
                now = time.time()
                self.request_times = [
                    t for t in self.request_times if now - t < self.time_window
                ]

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
    pageSize: int = 50
    order: str = field(default="numeroConvocatoria", init=False)  # Fixed order field

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")

        if self.pageSize < 50 or self.pageSize > 100:
            raise ValueError(f"pageSize debe estar entre [50, 100]: {self.pageSize}")


def scrape_historico(params: SearchParams) -> Path:
    """
    Scrape historical grants data and save to Parquet file.
    This is for initial bulk load - generates Parquet for etl/ingest_l0.py to process.

    Args:
        params: SearchParams with date range (fechaDesde required, fechaHasta defaults to today)

    Returns:
        Path to generated Parquet file
    """
    if not params.fechaDesde:
        raise ValueError("fechaDesde es requerido para scrape historico")

    params.validate()
    ano_inicio = int(params.fechaDesde[-4:])
    ano_fin = int(params.fechaHasta[-4:])

    _ensure_output_dir()

    all_records = []
    rate_limiter = RateLimiter(max_requests=49, time_window=60)

    page = params.page
    is_last_page = False
    pbar = None

    print("[INFO] Iniciando scraping historico...")

    try:
        while not is_last_page:
            params.page = page

            rate_limiter.wait_if_needed()
            res = requests.get(API_ENDPOINT_SEARCH, params=params.to_dict())

            # Check for errors before parsing JSON
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
                    res.raise_for_status()  # Fallback to default error

            data = res.json()

            content = data.get("content", [])
            is_last_page = data.get("last", True)

            # Initialize progress bar on first iteration
            if pbar is None:
                total_elements = data.get("totalElements", 0)
                print(f"Progreso descarga de subvenciones\n")
                pbar = tqdm(
                    total=total_elements,
                    unit="reg",
                )

            if not content:
                break

            # Collect records as dictionaries for DataFrame
            for item in content:
                all_records.append(
                    {
                        "id": item.get("id"),
                        "numeroConvocatoria": item.get("numeroConvocatoria"),
                        "mrr": item.get("mrr"),
                        "descripcion": item.get("descripcion"),
                        "descripcionLeng": item.get("descripcionLeng"),
                        "fechaRecepcion": item.get("fechaRecepcion"),
                        "nivel1": item.get("nivel1"),
                        "nivel2": item.get("nivel2"),
                        "nivel3": item.get("nivel3"),
                        "codigoINVENTE": item.get("codigoInvente"),
                    }
                )

            if pbar:
                pbar.update(len(content))

            page += 1

        if pbar:
            pbar.close()

        # Convert to DataFrame and save as Parquet
        df = pd.DataFrame(all_records)

        parquet_filename = f"licitaciones_subvenciones_{ano_inicio}_{ano_fin}.parquet"
        parquet_path = OUTPUT_DIR / parquet_filename

        df.to_parquet(parquet_path, engine="pyarrow", index=False)

        print(f"[INFO] Parquet generado: {parquet_path}")
        print(f"[INFO] Total registros: {len(df):,}")
        print(
            f"[INFO] Tamaño archivo: {parquet_path.stat().st_size / (1024 * 1024):.2f} MB"
        )

        return parquet_path

    except Exception as err:
        print(f"[ERROR] Error durante scraping historico: {err}")
        raise


def scrape_diario(params: LatestParams) -> None:
    """
    Scrape latest/daily grants and insert directly into database.
    This is for daily scheduler updates - small volume so direct insert is efficient.

    Args:
        params: LatestParams for latest updates

    Returns:
        Total number of new records inserted
    """
    params.validate()

    db_url = get_database_url()
    if not db_url:
        raise ValueError(
            "No se pudo obtener la URL de la base de datos. Verifica las variables de entorno."
        )

    conn = None
    total_records = 0
    rate_limiter = RateLimiter(max_requests=49, time_window=60)

    try:
        conn = psycopg2.connect(db_url)
        print("[INFO] Conexion a base de datos establecida (modo: diario)")

        page = params.page
        is_last_page = False

        while not is_last_page:
            params.page = page

            rate_limiter.wait_if_needed()
            res = requests.get(API_ENDPOINT_LATEST, params=params.to_dict())

            # Check for errors before parsing JSON
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
                    res.raise_for_status()  # Fallback to default error

            data = res.json()

            content = data.get("content", [])
            is_last_page = data.get("last", True)

            if not content:
                break

            records = []
            for item in content:
                records.append(
                    (
                        item.get("id"),
                        item.get("numeroConvocatoria"),
                        item.get("mrr"),
                        item.get("descripcion"),
                        item.get("descripcionLeng"),
                        item.get("fechaRecepcion"),
                        item.get("nivel1"),
                        item.get("nivel2"),
                        item.get("nivel3"),
                        item.get("codigoInvente"),
                    )
                )

            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    """
                    INSERT INTO l0.nacional_subvenciones 
                    (id, numeroConvocatoria, mrr, descripcion, descripcionLeng,
                     fechaRecepcion, nivel1, nivel2, nivel3, codigoINVENTE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    records,
                    page_size=params.pageSize,
                )
                conn.commit()

            total_records += len(content)

            page += 1

        print(f"[INFO] Scraping diario completado: {total_records} registros nuevos")

    except Exception as err:
        print(f"[ERROR] Error durante scraping diario: {err}")
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

        parquet_path = scrape_historico(params)
        if args.solo_descargar:
            print("   (--solo-descargar: no se cargará en BD)")

        return 0

    except Exception:
        return 1


if __name__ == "__main__":
    exit(main())
