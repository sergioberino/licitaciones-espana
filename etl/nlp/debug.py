"""Helpers de logging para el modo debug del pipeline NLP.

Activado con ``run_batch(..., debug=True)`` (propagado desde CLI ``--debug`` y
API ``POST /nlp/analizar {debug: true}``). Solo emite logs por stdout, no
persiste nada en BBDD: el objetivo es diagnosticar problemas de heurística o
extracción (p.ej. ``input_char_count`` sospechosamente bajo) sin tener que
añadir tablas nuevas.

Estilo: ``[nlp][debug] id=<subv_id> <fase> ...`` con banners ascii ligeros,
imitando ``nacional/subvenciones.py``. Cada bloque autoexplicativo en una sola
pasada del log.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional


_DEBUG_PREFIX = "[nlp][debug]"
_KEYWORDS_NORMATIVAS = (
    "artículo",
    "articulo",
    "beneficiarios",
    "beneficiario",
    "cuantía",
    "cuantia",
    "convocatoria",
    "bases reguladoras",
    "subvención",
    "subvencion",
    "ayuda",
    "régimen",
    "regimen",
    "plazo",
    "requisitos",
)


def text_preview(text: str, *, head: int = 500, tail: int = 200) -> tuple[str, str]:
    """Devuelve (head_chars, tail_chars) sin solapamiento.

    Si ``len(text) <= head+tail`` devuelve (todo, "") para no duplicar contenido.
    """
    if not text:
        return ("", "")
    if len(text) <= head + tail:
        return (text, "")
    return (text[:head], text[-tail:])


def html_signals(text: str) -> dict[str, Any]:
    """Calcula heurísticas baratas para distinguir bases reguladoras de portales.

    Métricas:
      - ``links`` / ``tables`` / ``forms``: si el texto extraído conserva tags
        (no debería tras `_extract_html`, pero las palabras 'inicio sesión',
        'menú', etc. delatan). Para detectar densidad usamos los marcadores
        de navegación habituales.
      - ``newlines_density``: tras el strip de tags, los portales suelen
        quedar como listas con muchas líneas cortas separadas. Una densidad
        > 1 línea cada 80 chars sugiere listado.
      - ``normative_keyword_hits``: cuántas palabras normativas aparecen.
      - ``suspected_portal``: True si pocas keywords + alta densidad de líneas.
    """
    if not text:
        return {
            "char_count": 0,
            "lines": 0,
            "newline_density_per_kchar": 0.0,
            "normative_keyword_hits": 0,
            "normative_keywords_found": [],
            "suspected_portal": False,
        }

    lower = text.lower()
    lines = text.count("\n") + 1
    char_count = len(text)
    density = round(lines / (char_count / 1000), 2) if char_count else 0.0

    found = sorted({kw for kw in _KEYWORDS_NORMATIVAS if kw in lower})
    hits = len(found)

    # Sospecha portal: pocas keywords (<=2) y muchas líneas cortas (>15 líneas/kchar).
    suspected = hits <= 2 and density > 15.0

    return {
        "char_count": char_count,
        "lines": lines,
        "newline_density_per_kchar": density,
        "normative_keyword_hits": hits,
        "normative_keywords_found": found,
        "suspected_portal": suspected,
    }


def summarize_documentos_array(documentos: Any) -> dict[str, Any]:
    """Resumen del array ``documentos`` BDNS para el log de resolución."""
    if not isinstance(documentos, list):
        return {"size": 0, "items": []}
    items = []
    for d in documentos:
        if not isinstance(d, dict):
            continue
        items.append({
            "id": d.get("id"),
            "descripcion": (d.get("descripcion") or "")[:80],
            "nombreFic": (d.get("nombreFic") or "")[:80],
            "datPublicacion": d.get("datPublicacion"),
        })
    return {"size": len(items), "items": items}


def _box_text(text: str, width: int = 78) -> list[str]:
    """Empaqueta texto en lineas con prefijo  '│ ', max width."""
    if not text:
        return ["│ (vacío)"]
    # Sustituye whitespace excesivo (PDF/HTML extraído suele tener mucho).
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return ["│ (solo whitespace)"]
    lines: list[str] = []
    inner_w = width - 2
    for i in range(0, len(cleaned), inner_w):
        chunk = cleaned[i:i + inner_w]
        lines.append(f"│ {chunk}")
    return lines


def log_resolve(
    logger: logging.Logger,
    *,
    subv_id: int,
    url_bases_reguladoras: Optional[str],
    documentos: Any,
    resolved,  # ResolvedDocument | None
) -> None:
    logger.info("=" * 78)
    logger.info("%s id=%d — Resolución de documento", _DEBUG_PREFIX, subv_id)
    logger.info("-" * 78)
    logger.info(
        "  url_bases_reguladoras: %s",
        url_bases_reguladoras if url_bases_reguladoras else "(NULL)",
    )
    summary = summarize_documentos_array(documentos)
    logger.info("  documentos[]: %d candidatos en BDNS", summary["size"])
    for item in summary["items"][:8]:
        logger.info(
            "      • id=%s pub=%s descr='%s' fic='%s'",
            item["id"], item["datPublicacion"],
            item["descripcion"], item["nombreFic"],
        )
    if summary["size"] > 8:
        logger.info("      ... y %d más", summary["size"] - 8)

    if resolved is None:
        logger.info("  → SIN RESOLUCIÓN (skipped_no_doc=true)")
        return
    logger.info("  → step=%d (%s)", resolved.heuristic_step, resolved.document_source)
    logger.info("  → document_ref=%s", resolved.document_ref)
    logger.info("  → document_key=%s", resolved.document_key)
    if resolved.document_name:
        logger.info("  → document_name='%s'", resolved.document_name[:100])


def log_fetch(
    logger: logging.Logger,
    *,
    subv_id: int,
    url: str,
    status_code: Optional[int],
    content_type: Optional[str],
    content_length: Optional[int],
    redirects: int = 0,
    error: Optional[str] = None,
) -> None:
    logger.info("%s id=%d — Descarga del documento", _DEBUG_PREFIX, subv_id)
    logger.info("  GET %s", url)
    if error:
        logger.info("  → ERROR: %s", error)
        return
    logger.info("  Status: %s", status_code if status_code is not None else "?")
    logger.info("  Content-Type: %s", content_type or "(sin cabecera)")
    if content_length is not None:
        logger.info(
            "  Content-Length: %s bytes (%.1f kB)",
            f"{content_length:,}", content_length / 1024,
        )
    if redirects:
        logger.info("  Redirects: %d", redirects)


def log_extract(
    logger: logging.Logger,
    *,
    subv_id: int,
    extraction_mode: str,
    content_type: Optional[str],
    text: str,
) -> None:
    head, tail = text_preview(text, head=500, tail=200)
    signals = html_signals(text)
    logger.info("%s id=%d — Extracción de texto", _DEBUG_PREFIX, subv_id)
    logger.info("  Modo: %s · Content-Type: %s", extraction_mode, content_type or "—")
    logger.info(
        "  Caracteres extraídos: %s · líneas: %s · densidad: %s líneas/kchar",
        f"{signals['char_count']:,}", f"{signals['lines']:,}",
        signals["newline_density_per_kchar"],
    )
    logger.info(
        "  Keywords normativas detectadas (%d/%d): %s",
        signals["normative_keyword_hits"],
        len(_KEYWORDS_NORMATIVAS),
        ", ".join(signals["normative_keywords_found"]) or "(ninguna)",
    )
    if signals["suspected_portal"]:
        logger.info(
            "  ⚠ HEURÍSTICA: el contenido parece un PORTAL/LISTADO "
            "(pocas keywords + alta densidad de líneas) — posible falso positivo step=1"
        )
    logger.info("  ┌─ preview HEAD (500 chars) " + "─" * 49)
    for line in _box_text(head):
        logger.info("  %s", line)
    logger.info("  └" + "─" * 76)
    if tail:
        logger.info("  ┌─ preview TAIL (200 chars) " + "─" * 49)
        for line in _box_text(tail):
            logger.info("  %s", line)
        logger.info("  └" + "─" * 76)
    logger.info("=" * 78)
