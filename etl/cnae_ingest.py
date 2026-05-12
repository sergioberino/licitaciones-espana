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


def _is_official_code(code_id: str) -> bool:
    """Return True only for official CNAE codes: single uppercase letter (section)
    or digits-only (division/group/class). Excludes aggregates like _T, _N, etc."""
    return bool(re.match(r"^[A-Z]$", code_id) or re.match(r"^\d+$", code_id))


def _extract_parent_letter(item: dict) -> str | None:
    """Extract the section letter from the SDMX parent URN, e.g. '...CL_CNAE_2025(01.001).A' → 'A'."""
    parent_urn = item.get("parent", "")
    if not parent_urn:
        return None
    candidate = parent_urn.rsplit(".", 1)[-1]
    return candidate if re.match(r"^[A-Z]$", candidate) else None


def _build_rows(raw_codes: list[dict]) -> list[tuple[int, str, str, int | None]]:
    """Build (id, code, label, parent_id) rows with manually assigned sequential IDs.

    The API delivers numeric codes first (ordered by depth: 2→3→4 digits) and
    section letters at the end, so parent_id for 2-digit codes cannot be resolved
    inline. Strategy:
      - Single-letter sections  → insert with parent_id=None, record id in section_ids.
      - 2-digit codes           → insert with parent_id=None, record (row_index, letter)
                                   in `deferred` for a patch after the main loop.
      - 3/4-digit codes         → parent resolved inline via last_id_by_level (numeric
                                   codes come in depth order so the parent is always seen first).
    The deferred patch is a micro-loop over only ~88 two-digit codes.
    """
    rows: list[tuple[int, str, str, int | None]] = []
    counter = 0
    last_id_by_level: dict[int, int] = {}  # level → last assigned id
    section_ids: dict[str, int] = {}  # letter → assigned id
    deferred: list[tuple[int, str]] = []  # (row_index, section_letter) for 2-digit codes

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
            section_ids[code_id] = counter
        elif len(code_id) == 2:
            level = 2
            parent_id = None  # patched below
            letter = _extract_parent_letter(item)
            if letter:
                deferred.append((len(rows), letter))
            last_id_by_level[level] = counter
        else:
            level = len(code_id)  # 3 or 4
            parent_id = last_id_by_level.get(level - 1)
            last_id_by_level[level] = counter

        rows.append((counter, code_id, label, parent_id))

    # Patch parent_id for 2-digit codes now that all section ids are known
    for row_index, letter in deferred:
        section_id = section_ids.get(letter)
        if section_id is None:
            logger.warning("CNAE build_rows: section %r not found for row %d", letter, row_index)
            continue
        id_, code, label, _ = rows[row_index]
        rows[row_index] = (id_, code, label, section_id)

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
