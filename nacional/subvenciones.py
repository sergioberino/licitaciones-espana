import requests
import psycopg2
from psycopg2.extras import execute_batch
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from typing import List, Dict, Any
import sys
from pathlib import Path
import time
from tqdm import tqdm

# Add parent directory to path to import etl modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import get_database_url

# No API_KEY required - public access
API_BASE_URL = "https://www.infosubvenciones.es/bdnstrans/api/convocatorias"
API_ENDPOINT_SEARCH = API_BASE_URL + "/busqueda"
API_ENDPOINT_LATEST = API_BASE_URL + "/ultimas"


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
    pageSize: int = 1000
    fechaDesde: str | None = None  # Format: "DD-MM-YYYY"
    fechaHasta: str | None = None  # Format: "DD-MM-YYYY"

    def to_dict(self) -> dict:
        """Convert to dictionary removing None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def validate(self) -> None:
        """Validate parameters."""
        if self.page < 0:
            raise ValueError(f"page debe ser >= 0, recibido: {self.page}")
        if self.pageSize < 50 or self.pageSize > 1000:
            raise ValueError(
                f"pageSize debe estar entre 50-1000, recibido: {self.pageSize}"
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
        """Validate that a date has DD-MM-YYYY format."""
        try:
            datetime.strptime(date_str, "%d-%m-%Y")
        except ValueError:
            raise ValueError(
                f"{field_name} debe tener formato DD-MM-YYYY, recibido: {date_str}"
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


def scrape_historico(params: SearchParams) -> int:
    """
    Fetch all historical grants data and save to database.
    Returns the total number of records processed.
    """
    params.validate()

    db_url = get_database_url()
    if not db_url:
        raise ValueError(
            "No se pudo obtener la URL de la base de datos. Verifica las variables de entorno."
        )

    conn = None
    total_records = 0
    rate_limiter = RateLimiter(max_requests=50, time_window=60)

    try:
        conn = psycopg2.connect(db_url)
        print("[INFO] Conexion a base de datos establecida")

        page = params.page
        is_last_page = False
        pbar = None

        while not is_last_page:
            params.page = page

            try:
                rate_limiter.wait_if_needed()
                res = requests.get(API_ENDPOINT_SEARCH, params=params.to_dict())
                res.raise_for_status()
                data = res.json()

                content = data.get("content", [])
                is_last_page = data.get("last", True)

                # Initialize progress bar on first iteration
                if pbar is None:
                    total_elements = data.get("totalElements", 0)
                    print(f"[INFO] Total de registros a procesar: {total_elements}")
                    pbar = tqdm(
                        total=total_elements,
                        desc="Descargando subvenciones",
                        unit="reg",
                    )

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
                        INSERT INTO l0.nacional_subvenciones_minimo 
                        (id, numeroConvocatoria, mrr, descripcion, descripcionLeng,
                         fechaRecepcion, nivel1, nivel2, nivel3, codigoINVENTE)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        records,
                        page_size=1000,
                    )
                    conn.commit()

                total_records += len(content)
                if pbar:
                    pbar.update(len(content))

                page += 1

            except requests.exceptions.RequestException as err:
                print(f"\n[ERROR] Error en peticion HTTP: {err}")
                raise
            except psycopg2.Error as err:
                print(f"\n[ERROR] Error en base de datos: {err}")
                conn.rollback()
                raise
            except Exception as err:
                print(f"\n[ERROR] Error inesperado: {err}")
                raise

        if pbar:
            pbar.close()

        print(
            f"[INFO] Scraping historico completado: {total_records} registros procesados"
        )
        return total_records

    except Exception as err:
        print(f"[ERROR] Error fatal durante scraping: {err}")
        raise
    finally:
        if conn:
            conn.close()


def main():
    """Example usage of the scraper."""
    params = SearchParams(
        page=0, pageSize=50, fechaDesde="01-01-2024", fechaHasta="31-12-2024"
    )

    try:
        total = scrape_historico(params)
        print(f"[INFO] Total procesado: {total} registros")
        return 0
    except Exception as err:
        print(f"[ERROR] {err}")
        return 1


if __name__ == "__main__":
    main()
