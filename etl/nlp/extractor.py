"""Extracción de texto desde URL o texto plano.

Política httpx (``extract_from_url``):
  - GET con httpx, follow redirects, User-Agent identificable, timeout.
  - Content-Type:
      application/pdf       → pypdf
      text/html             → BeautifulSoup, strip tags
      text/xml application/xml → BeautifulSoup (parser xml), strip tags
      text/plain            → response.text
      otros                 → intentar pypdf primero, luego texto plano
  - Resultado: string con texto extraído. Errores → ExtractionError.

Política escalonada (``extract_for_url_bases_reguladoras``, Task 7):
  Solo para ``document_source='url_bases_reguladoras'`` (resolver step=3).
  1) URL con extensión descargable (.pdf, .doc, …) → httpx directo.
  2) URL fragment-only (#/path sin path real) → browser-service directo.
  3) Resto → httpx + ``should_escalate_to_browser``; si calidad baja → browser.
  4) Browser caído → último intento httpx sobre la URL original.
  Devuelve ``None`` si browser responde ``unresolvable`` → pipeline marca
  ``skipped_no_doc`` sin llamar al LLM.
"""
from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, replace
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from pypdf import PdfReader


logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    pass


@dataclass(frozen=True)
class ExtractedText:
    text: str
    content_type: Optional[str]
    char_count: int
    # Hito 3.1.f — branch que ganó dentro de extract_from_url. Útil para distinguir
    # PDFs servidos como application/pdf (pdf_native) de los servidos como
    # application/octet-stream (BDNS), que requieren fallback magic-byte → pypdf.
    # Valores: 'pdf_native' | 'html' | 'xml' | 'text'
    #          'octet_fallback_pdf' | 'octet_fallback_html' | 'octet_fallback_text'
    extraction_mode: str = "unknown"
    # Hito 3.2 debug — metadatos HTTP de la respuesta original. Solo poblados
    # cuando se loguean en modo debug; opcionales para no romper consumers viejos.
    http_status: Optional[int] = None
    content_length: Optional[int] = None
    redirects: int = 0
    final_url: Optional[str] = None
    raw_content: bytes | None = None


_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
_DEFAULT_HEADERS = {
    "User-Agent": "licitia-etl-nlp/1.0 (+https://github.com/sergioberino/licitaciones-espana)",
    "Accept": "*/*",
}


def _extract_pdf(content: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(content))
        return "\n".join(page.extract_text() or "" for page in reader.pages).strip()
    except Exception as e:
        raise ExtractionError(f"PDF parse failed: {e}") from e


def _extract_html(content: bytes, encoding: Optional[str] = None) -> str:
    soup = BeautifulSoup(content, "html.parser", from_encoding=encoding)
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def extract_from_url(url: str) -> ExtractedText:
    with httpx.Client(timeout=_DEFAULT_TIMEOUT, headers=_DEFAULT_HEADERS, follow_redirects=True) as client:
        resp = client.get(url)
    if resp.status_code >= 400:
        raise ExtractionError(f"HTTP {resp.status_code} fetching {url}")
    ctype = (resp.headers.get("content-type") or "").lower()
    content = resp.content
    http_status = resp.status_code
    content_length = len(content)
    redirects = len(resp.history) if hasattr(resp, "history") else 0
    final_url = str(resp.url) if hasattr(resp, "url") else url

    if "application/pdf" in ctype or url.lower().endswith(".pdf"):
        text = _extract_pdf(content)
        mode = "pdf_native"
    elif "text/html" in ctype:
        text = _extract_html(content, encoding=resp.encoding)
        mode = "html"
    elif "text/xml" in ctype or "application/xml" in ctype:
        text = _extract_html(content, encoding=resp.encoding)
        mode = "xml"
    elif "text/plain" in ctype:
        text = resp.text
        mode = "text"
    else:
        # Fallback típico de BDNS, que sirve PDFs como application/octet-stream.
        # Probamos pypdf primero (magic byte %PDF-), HTML si no, texto plano si no.
        try:
            text = _extract_pdf(content)
            mode = "octet_fallback_pdf"
        except ExtractionError:
            try:
                text = _extract_html(content)
                mode = "octet_fallback_html"
            except Exception:
                text = resp.text
                mode = "octet_fallback_text"

    text = text.strip()
    if not text:
        raise ExtractionError(f"Empty text extracted from {url}")
    return ExtractedText(
        text=text,
        content_type=ctype,
        char_count=len(text),
        raw_content=content,
        extraction_mode=mode,
        http_status=http_status,
        content_length=content_length,
        redirects=redirects,
        final_url=final_url,
    )


# ---------------------------------------------------------------------------
# URL gates + extracción escalonada (url_bases_reguladoras / step=3)
# ---------------------------------------------------------------------------

_DESCARGABLE_EXTENSIONS = (".pdf", ".doc", ".docx", ".rtf", ".txt")
_DESCARGABLE_IN_URL_RE = re.compile(
    r"\.(?:pdf|docx?|rtf|txt)(?:[?#]|$)",
    re.IGNORECASE,
)
_QUALITY_CHARS_MIN = 500
_LOG_TEXT_MAX_CHARS = 200


def _safe_log_value(value: object) -> str:
    squashed = re.sub(r"\s+", " ", str(value)).strip()
    if len(squashed) > _LOG_TEXT_MAX_CHARS:
        return f"{squashed[:_LOG_TEXT_MAX_CHARS]}..."
    return squashed


def is_direct_downloadable(url: str) -> bool:
    """True si la URL apunta a un binario/texto descargable (path o query)."""
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    if path.endswith(_DESCARGABLE_EXTENSIONS):
        return True
    # BOE/BDNS a veces sirven PDFs como ``/dl?f=bases.pdf``
    return bool(_DESCARGABLE_IN_URL_RE.search(url))


def is_fragment_only_routing(url: str) -> bool:
    """Hash-routing SPA: fragmento con path significativo y sin path HTTP real.

    Positivos: ``https://borm.es/#/anuncio/6349``, ``https://x.es#/foo``.
    Negativos: ``https://boe.es/.../act.php?id=...#articulo-5`` (anchor interno).
    """
    parsed = urlparse(url)
    if not parsed.fragment:
        return False
    path = (parsed.path or "").strip("/")
    if path:
        return False
    frag = parsed.fragment
    return frag.startswith("/") or "/" in frag


def should_escalate_to_browser(extracted: ExtractedText) -> tuple[bool, str | None]:
    """Tras httpx, decide si el texto parece portal/wrapper y hay que renderizar.

    Usa las mismas señales que ``debug.html_signals`` para coherencia con logs
    de ``--debug``.
    """
    if extracted.char_count < _QUALITY_CHARS_MIN:
        return True, f"short_body_{extracted.char_count}_chars"

    from etl.nlp.debug import html_signals

    sig = html_signals(extracted.text)
    if sig["normative_keyword_hits"] == 0:
        return True, "no_normative_keywords"
    if sig["suspected_portal"]:
        return True, "suspected_portal"
    return False, None


def _is_usable_static_anchor_extraction(extracted: ExtractedText) -> tuple[bool, str | None]:
    if extracted.extraction_mode in {"pdf_native", "octet_fallback_pdf"}:
        return True, None

    escalate, reason = should_escalate_to_browser(extracted)
    return not escalate, reason


def _extracted_from_browser_text(
    text: str,
    *,
    final_url: Optional[str],
) -> ExtractedText:
    body = (text or "").strip()
    if not body:
        raise ExtractionError("Empty text from browser-service")
    return ExtractedText(
        text=body,
        content_type="text/html",
        char_count=len(body),
        extraction_mode="headless_body",
        final_url=final_url,
    )


def extract_for_url_bases_reguladoras(
    url: str,
    *,
    logger: Optional[logging.Logger] = None,
    subv_id: Optional[int] = None,
) -> Optional[ExtractedText]:
    """Extracción escalonada para ``url_bases_reguladoras`` (resolver step=3).

    Returns:
        ExtractedText si hay contenido utilizable.
        None si browser-service declara ``unresolvable`` (→ skipped_no_doc).
    Raises:
        ExtractionError si todos los caminos fallan (→ items_invalid).
    """
    log = logger or logging.getLogger(__name__)
    prefix = f"[nlp] id={subv_id} " if subv_id is not None else "[nlp] "

    def _apply_browser_outcome(bro, escalation_reason: Optional[str]) -> Optional[ExtractedText]:
        from etl.nlp.browser_client import BrowserOutcome

        assert isinstance(bro, BrowserOutcome)
        esc = f" esc_reason={escalation_reason}" if escalation_reason else ""
        if bro.outcome == "pdf_url" and bro.pdf_url:
            extracted = extract_from_url(bro.pdf_url)
            extracted = ExtractedText(
                text=extracted.text,
                content_type=extracted.content_type,
                char_count=extracted.char_count,
                extraction_mode="headless_pdf",
                http_status=extracted.http_status,
                content_length=extracted.content_length,
                redirects=extracted.redirects,
                final_url=bro.final_url or extracted.final_url,
                raw_content=extracted.raw_content,
            )
            log.info(
                "%sstep=extract source=headless_pdf url=%s chars=%d%s",
                prefix,
                bro.pdf_url,
                extracted.char_count,
                esc,
            )
            return extracted
        if bro.outcome == "text" and bro.text:
            extracted = _extracted_from_browser_text(bro.text, final_url=bro.final_url)
            log.info(
                "%sstep=extract source=headless_body chars=%d%s",
                prefix,
                extracted.char_count,
                esc,
            )
            return extracted
        log.info(
            "%sskipped_no_doc=true reason=%s%s",
            prefix,
            bro.reason or "unresolvable",
            esc,
        )
        return None

    escalation_reason: Optional[str] = None

    if is_direct_downloadable(url):
        extracted = extract_from_url(url)
        log.info(
            "%sstep=extract source=url_direct_download chars=%d mode=%s",
            prefix,
            extracted.char_count,
            extracted.extraction_mode,
        )
        return extracted

    if is_fragment_only_routing(url):
        escalation_reason = "fragment_only_routing"
    else:
        try:
            tentative = extract_from_url(url)
        except ExtractionError as exc:
            escalation_reason = f"httpx_error:{exc}"
        else:
            if tentative.extraction_mode == "html" and tentative.raw_content is not None:
                from etl.nlp.html_anchor_picker import pick_static_html_document_anchor

                candidate = pick_static_html_document_anchor(
                    tentative.raw_content,
                    base_url=tentative.final_url or url,
                )
                if candidate is not None:
                    candidate_href = _safe_log_value(candidate.href)
                    candidate_text = _safe_log_value(candidate.text)
                    try:
                        linked = extract_from_url(candidate.href)
                    except ExtractionError as exc:
                        log.info(
                            "%shtml_anchor_fallback reason=linked_extract_error:%s href=%s picked_anchor_text=%r",
                            prefix,
                            _safe_log_value(exc),
                            candidate_href,
                            candidate_text,
                        )
                    else:
                        linked_usable, linked_reason = _is_usable_static_anchor_extraction(linked)
                        if not linked_usable:
                            log.info(
                                "%shtml_anchor_fallback reason=%s href=%s picked_anchor_text=%r",
                                prefix,
                                _safe_log_value(linked_reason or "linked_low_quality"),
                                candidate_href,
                                candidate_text,
                            )
                        else:
                            extracted = replace(
                                linked,
                                extraction_mode=f"html_anchor_{linked.extraction_mode}",
                            )
                            log.info(
                                "%sstep=extract source=html_anchor href=%s picked_anchor_text=%r chars=%d mode=%s",
                                prefix,
                                candidate_href,
                                candidate_text,
                                extracted.char_count,
                                extracted.extraction_mode,
                            )
                            return extracted

            escalate, reason = should_escalate_to_browser(tentative)
            if escalate:
                escalation_reason = reason
                log.info(
                    "%shttpx_low_quality reason=%s — escalating to browser-service",
                    prefix,
                    reason,
                )
            else:
                log.info(
                    "%sstep=extract source=url_httpx chars=%d mode=%s",
                    prefix,
                    tentative.char_count,
                    tentative.extraction_mode,
                )
                return tentative

    from etl.nlp.browser_client import BrowserUnavailable, resolve_via_browser

    try:
        bro = resolve_via_browser(url)
    except BrowserUnavailable as exc:
        log.warning(
            "%sbrowser-service unavailable: %s — last-resort httpx",
            prefix,
            exc,
        )
        return extract_from_url(url)

    return _apply_browser_outcome(bro, escalation_reason)


def extract_from_text(text: str) -> ExtractedText:
    text = text.strip()
    if not text:
        raise ExtractionError("Empty input text")
    return ExtractedText(
        text=text, content_type="text/plain", char_count=len(text), extraction_mode="text"
    )
