"""CNAE dimension ingest from ISTAC SDMX API (CNAE-2025 codelist)."""

import logging
import os
import re

import psycopg2
import requests

logger = logging.getLogger("etl.cnae_ingest")

DEFAULT_CNAE_URL = (
    "https://datos.canarias.es/api/estadisticas/structural-resources/v1.0"
    + "/codelists/ISTAC/CL_CNAE_2025/01.001/codes.json"
)

PAGE_SIZE = 500


def _get_cnae_url() -> str:
    return DEFAULT_CNAE_URL


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


def _is_official_code(code_id: str) -> bool:
    """Return True only for official CNAE codes: single uppercase letter (section)
    or digits-only (division/group/class). Excludes aggregates like _T, _N, etc."""
    return bool(re.match(r"^[A-Z]$", code_id) or re.match(r"^\d+$", code_id))


def _build_rows(raw_codes: list[dict]) -> list[tuple[int, str, str, int | None]]:
    """Build (id, code, label, parent_id) rows with manually assigned sequential IDs.

    The API returns codes in hierarchical order (section letter, then 2-digit, 3-digit,
    4-digit), so parent_id is resolved by tracking the last assigned id per level:
      level 1 = single letter (A-U)
      level 2 = 2-digit code
      level 3 = 3-digit code
      level 4 = 4-digit code
    A code of N digits has parent = last id seen at level N-1.
    """
    rows: list[tuple[int, str, str, int | None]] = []
    counter = 0
    last_id_by_level: dict[int, int] = {}

    for item in raw_codes:
        code_id = item.get("id", "")
        if not _is_official_code(code_id):
            continue
        label = _extract_label_es(item.get("name"))
        if not label:
            continue

        counter += 1

        if re.match(r"^[A-Z]$", code_id):
            level = 1
            parent_id = None
        else:
            level = len(code_id)  # 2, 3 or 4
            parent_id = last_id_by_level.get(level - 1)

        last_id_by_level[level] = counter
        rows.append((counter, code_id, label, parent_id))

    return rows


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
    rows = _build_rows(raw_codes)
    logger.info("CNAE ingest: %d codes extracted", len(rows))

    if not rows:
        return {"ok": True, "rows": 0, "message": "No CNAE codes found in API response"}

    conn = psycopg2.connect(db_url)
    try:
        conn.autocommit = False
        upsert = (
            "INSERT INTO dim.cnae_dim (id, code, label, parent_id) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (code) DO UPDATE SET label = EXCLUDED.label, parent_id = EXCLUDED.parent_id"
        )
        with conn.cursor() as cur:
            cur.executemany(upsert, rows)

        conn.commit()
        logger.info("CNAE ingest: upserted %d codes into dim.cnae_dim", len(rows))
        return {"ok": True, "rows": len(rows), "message": f"Upserted {len(rows)} CNAE codes"}
    except Exception as e:
        conn.rollback()
        logger.exception("CNAE ingest failed")
        return {"ok": False, "rows": 0, "message": str(e)}
    finally:
        conn.close()
