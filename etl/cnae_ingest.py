"""CNAE dimension ingest from ISTAC SDMX API (CNAE-2025 codelist)."""

import logging
import os

import psycopg2
from psycopg2 import sql
import requests

logger = logging.getLogger("etl.cnae_ingest")

DEFAULT_CNAE_URL = (
    "https://datos.canarias.es/api/estadisticas/structural-resources/v1.0"
    "/codelists/ISTAC/CL_CNAE_2025/01.001/codes.json"
)

PAGE_SIZE = 500


def _get_cnae_url() -> str:
    return os.environ.get("CNAE_SOURCE_URL", "").strip() or DEFAULT_CNAE_URL


def _fetch_all_codes(base_url: str) -> list[dict]:
    """Fetch all codes from the paginated SDMX API. Returns raw code dicts."""
    all_codes: list[dict] = []
    offset = 0
    while True:
        sep = "&" if "?" in base_url else "?"
        url = f"{base_url}{sep}limit={PAGE_SIZE}&offset={offset}"
        logger.info("CNAE fetch: %s", url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        codes = data.get("code", [])
        all_codes.extend(codes)
        total = data.get("total", 0)
        if offset + len(codes) >= total or not codes:
            break
        offset += len(codes)
    logger.info("CNAE fetch: %d total codes retrieved", len(all_codes))
    return all_codes


def _extract_label_es(name_block: dict | None) -> str | None:
    """Extract Spanish label from SDMX name.text array."""
    if not name_block:
        return None
    for entry in name_block.get("text", []):
        if entry.get("lang") == "es":
            return entry.get("value")
    texts = name_block.get("text", [])
    return texts[0].get("value") if texts else None


def _filter_numeric_codes(raw_codes: list[dict]) -> list[tuple[str, str]]:
    """Filter to numeric-only CNAE codes and return (code, label) tuples."""
    result = []
    for item in raw_codes:
        code_id = item.get("id", "")
        if not code_id.isdigit():
            continue
        label = _extract_label_es(item.get("name"))
        if not label:
            continue
        result.append((code_id, label))
    return result


def run_cnae_ingest(url: str | None = None) -> dict:
    """Fetch CNAE codes from ISTAC API and upsert into dim.cnae_dim.

    Returns dict with keys: ok, rows, message.
    """
    from etl.config import get_database_url

    db_url = url or get_database_url()
    if not db_url:
        return {"ok": False, "rows": 0, "message": "Database URL not configured"}

    source_url = _get_cnae_url()
    logger.info("CNAE ingest: fetching from %s", source_url)

    raw_codes = _fetch_all_codes(source_url)
    codes = _filter_numeric_codes(raw_codes)
    logger.info("CNAE ingest: %d numeric codes after filtering", len(codes))

    if not codes:
        return {"ok": True, "rows": 0, "message": "No numeric CNAE codes found in API response"}

    conn = psycopg2.connect(db_url)
    try:
        conn.autocommit = False
        upsert = sql.SQL(
            "INSERT INTO dim.cnae_dim (code, label) VALUES (%s, %s) "
            "ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label"
        )
        with conn.cursor() as cur:
            for code, label in codes:
                cur.execute(upsert, (code, label))
        conn.commit()
        logger.info("CNAE ingest: upserted %d codes into dim.cnae_dim", len(codes))
        return {"ok": True, "rows": len(codes), "message": f"Upserted {len(codes)} CNAE codes"}
    except Exception as e:
        conn.rollback()
        logger.exception("CNAE ingest failed")
        return {"ok": False, "rows": 0, "message": str(e)}
    finally:
        conn.close()
