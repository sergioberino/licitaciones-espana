"""Extracción de texto desde URL o texto plano.

Política:
  - GET con httpx, follow redirects, User-Agent identificable, timeout.
  - Content-Type:
      application/pdf       → pypdf
      text/html             → BeautifulSoup, strip tags
      text/xml application/xml → BeautifulSoup (parser xml), strip tags
      text/plain            → response.text
      otros                 → intentar pypdf primero, luego texto plano
  - Resultado: string con texto extraído. Errores → ExtractionError.
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from typing import Optional

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
        text=text, content_type=ctype, char_count=len(text), extraction_mode=mode
    )


def extract_from_text(text: str) -> ExtractedText:
    text = text.strip()
    if not text:
        raise ExtractionError("Empty input text")
    return ExtractedText(
        text=text, content_type="text/plain", char_count=len(text), extraction_mode="text"
    )
