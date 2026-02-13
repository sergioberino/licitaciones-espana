"""
Ingesta de DIR3 (Listado Unidades AGE) desde XLSX de datos.gob.es / administracionelectronica.gob.es.

Descarga el fichero, parsea con pandas (openpyxl), mapea columnas a dim.dim_dir3 e inserta
(TRUNCATE + INSERT). Idempotente por ejecución: cada init-db deja el catálogo actualizado.
La FK recursiva parent_code -> code se respeta con constraint DEFERRABLE (se comprueba al commit).
"""

import io
import logging
import os
import sys
import time
from typing import Any, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

LOG_PREFIX = "[dir3_ingest]"
logger = logging.getLogger("etl.dir3_ingest")

# URL por defecto: Listado de información básica de unidades orgánicas de la AGE (datos.gob.es).
DEFAULT_DIR3_XLSX_URL = (
    "https://administracionelectronica.gob.es/ctt/resources/Soluciones/238/Descargas/"
    "Listado%20Unidades%20AGE.xlsx?idIniciativa=238&idElemento=2741"
)
DOWNLOAD_TIMEOUT = 60
DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_SEC = 2.0
USER_AGENT = "licitia-etl/1.0 (datos abiertos; no seguimiento)"


def _configure_logging() -> None:
    if logger.handlers:
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(LOG_PREFIX + " %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _get_xlsx_url() -> str:
    """URL del XLSX DIR3; variable DIR3_XLSX_URL o valor por defecto."""
    return os.environ.get("DIR3_XLSX_URL", "").strip() or DEFAULT_DIR3_XLSX_URL


# Mapeo cabecera XLSX (Listado Unidades AGE) -> columna en dim.dim_dir3.
XLSX_TO_DIM = {
    "num_code": "C_ID_UD_ORGANICA",
    "label": "C_DNM_UD_ORGANICA",
    "parent_code": "C_ID_DEP_UD_SUPERIOR",
    "nivel_admon": "C_ID_NIVEL_ADMON",
    "tipo_ent_publica": "C_ID_TIPO_ENT_PUBLICA",
    "nivel_jerarquico": "N_NIVEL_JERARQUICO",
    "estado": "C_ID_ESTADO",
    "vig_alta_oficial": "D_VIG_ALTA_OFICIAL",
    "nif_cif": "NIF_CIF",
}
# Cabeceras que deben aparecer en la fila para considerarla fila de encabezado (evita falsos positivos).
REQUIRED_HEADER_NAMES = frozenset({"C_ID_UD_ORGANICA", "C_DNM_UD_ORGANICA"})
MAX_HEADER_SCAN_ROWS = 25


def _find_header_row(raw: pd.DataFrame) -> int:
    """Devuelve el índice de la primera fila que contiene las cabeceras conocidas (p. ej. C_ID_UD_ORGANICA).
    Tolerante a que la fuente ponga el encabezado en fila 1, 2, etc.
    """
    for i in range(min(MAX_HEADER_SCAN_ROWS, len(raw))):
        row = raw.iloc[i]
        cells = {str(c).strip() for c in row.values if pd.notna(c)}
        if REQUIRED_HEADER_NAMES.issubset(cells):
            return i
    raise ValueError(
        "No se encontró la fila de cabeceras (C_ID_UD_ORGANICA, C_DNM_UD_ORGANICA) en las primeras %s filas."
        % MAX_HEADER_SCAN_ROWS
    )


def _read_dir3_xlsx(content: bytes) -> pd.DataFrame:
    """Lee el XLSX DIR3 detectando la fila de cabecera (puede ser 0, 1, 2, ...)."""
    with pd.ExcelFile(io.BytesIO(content), engine="openpyxl") as xl:
        raw = pd.read_excel(xl, sheet_name=0, header=None)
    header_row = _find_header_row(raw)
    logger.info("Cabeceras detectadas en fila %s (0-based).", header_row)
    columns = [str(c).strip() for c in raw.iloc[header_row]]
    df = raw.iloc[header_row + 1 :].copy()
    df.columns = columns
    df.reset_index(drop=True, inplace=True)
    return df


def _to_scalar(val: Any) -> Any:
    """Si val es una Series de pandas (p. ej. por columnas duplicadas), devuelve el primer valor; si no, val."""
    if hasattr(val, "iloc"):
        return val.iloc[0] if len(val) else None
    return val


def _map_row_to_columns(row: pd.Series, columns_map: dict[str, Optional[str]]) -> dict[str, Any]:
    """Convierte una fila del DataFrame a un dict con claves de dim_dir3 (num_code, label, ...)."""
    out: dict[str, Any] = {}
    for dim_col, xlsx_col in columns_map.items():
        if xlsx_col is None:
            continue
        val = _to_scalar(row.get(xlsx_col))
        if pd.isna(val):
            if dim_col == "parent_code":
                out[dim_col] = None
            elif dim_col in ("nivel_admon", "nivel_jerarquico"):
                out[dim_col] = None
            else:
                out[dim_col] = None if dim_col in ("vig_alta_oficial",) else ""
        else:
            if dim_col in ("nivel_admon", "nivel_jerarquico"):
                try:
                    out[dim_col] = int(float(val)) if val != "" else None
                except (ValueError, TypeError):
                    out[dim_col] = None
            elif dim_col == "vig_alta_oficial":
                if isinstance(val, pd.Timestamp):
                    out[dim_col] = val.date()
                else:
                    try:
                        out[dim_col] = pd.to_datetime(val).date() if val else None
                    except Exception:
                        out[dim_col] = None
            else:
                s = str(val).strip()
                if dim_col == "estado" and len(s) > 1:
                    s = s[0]
                out[dim_col] = s[:16] if dim_col == "num_code" else (s[:8] if dim_col == "tipo_ent_publica" else (s[:16] if dim_col == "nif_cif" else s))
    return out


def _build_columns_map(df: pd.DataFrame) -> dict[str, Optional[str]]:
    """Mapeo columna dim -> columna real del DataFrame (por XLSX_TO_DIM)."""
    columns_map: dict[str, Optional[str]] = {dim_col: None for dim_col in XLSX_TO_DIM}
    for dim_col, xlsx_header in XLSX_TO_DIM.items():
        for real in df.columns:
            if isinstance(real, str) and real.strip() == xlsx_header:
                columns_map[dim_col] = real
                break
    return columns_map


def run_dir3_ingest(database_url: str, xlsx_url: Optional[str] = None) -> int:
    """
    Descarga el XLSX DIR3, parsea e inserta en dim.dim_dir3. TRUNCATE previo (idempotente).
    Devuelve el número de filas insertadas. Lanza excepción en error de descarga o DB.
    """
    _configure_logging()
    url = (xlsx_url or _get_xlsx_url()).strip()
    if not url:
        raise ValueError("URL del XLSX DIR3 no configurada (DIR3_XLSX_URL).")

    logger.info("Descargando Listado Unidades AGE (datos.gob.es)...")
    logger.info("URL: %s", url)
    last_exc = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            r = requests.get(
                url,
                timeout=DOWNLOAD_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            r.raise_for_status()
            logger.info("Respuesta: status=%s, Content-Type=%s, size=%s bytes", r.status_code, r.headers.get("Content-Type", ""), len(r.content))
            ct = (r.headers.get("Content-Type") or "").lower()
            if "spreadsheet" not in ct and "vnd.openxmlformats" not in ct and "octet-stream" not in ct:
                logger.warning("Content-Type no es XLSX (posible bloqueo o redirección): %s", r.headers.get("Content-Type", "(vacío)"))
            if len(r.content) < 1000:
                logger.warning("Respuesta muy pequeña (%s bytes); podría ser página de error o bloqueo.", len(r.content))
            break
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Intento %s de descarga fallido: %s", attempt + 1, e)
            if attempt < DOWNLOAD_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SEC * (attempt + 1))
    else:
        logger.error("Error descargando DIR3: %s", last_exc)
        raise last_exc  # type: ignore[misc]

    df = _read_dir3_xlsx(r.content)

    rows = len(df)
    logger.info("Descargado. Filas parseadas: %s.", rows)

    columns_map = _build_columns_map(df)
    if columns_map.get("num_code") is None:
        raise ValueError(
            "No se encontró columna para 'num_code' (C_ID_UD_ORGANICA) en el XLSX DIR3. "
            "Cabeceras encontradas: %s" % list(df.columns[:15])
        )

    # Construir listas de valores para INSERT: num_code, label, parent_code, ...
    to_insert = []
    for _, row in df.iterrows():
        mapped = _map_row_to_columns(row, columns_map)
        num_code = _to_scalar(mapped.get("num_code"))
        if num_code is None or (isinstance(num_code, str) and not num_code.strip()):
            continue
        to_insert.append(
            (
                str(num_code)[:16],
                (mapped.get("label") or "")[:65535] if mapped.get("label") else None,
                (str(mapped.get("parent_code") or "").strip()[:16] or None),
                mapped.get("nivel_admon"),
                (mapped.get("tipo_ent_publica") or "")[:8] or None,
                mapped.get("nivel_jerarquico"),
                (str(mapped.get("estado") or "").strip()[:1] or None),
                mapped.get("vig_alta_oficial"),
                (str(mapped.get("nif_cif") or "").strip()[:16] or None),
            )
        )

    if not to_insert:
        logger.warning("No hay filas válidas para insertar en dim.dim_dir3.")
        return 0

    logger.info("Insertando en dim.dim_dir3...")
    with psycopg2.connect(database_url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE dim.dim_dir3 RESTART IDENTITY CASCADE")
            # FK parent_code es DEFERRABLE INITIALLY DEFERRED; se comprueba al commit.
            insert_sql = """
                INSERT INTO dim.dim_dir3
                (num_code, label, parent_code, nivel_admon, tipo_ent_publica, nivel_jerarquico, estado, vig_alta_oficial, nif_cif)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            psycopg2.extras.execute_batch(cur, insert_sql, to_insert, page_size=1000)
        conn.commit()

    logger.info("DIR3 ingerido: %s filas.", len(to_insert))
    return len(to_insert)


# Re-export para uso desde cli
__all__ = ["run_dir3_ingest"]
