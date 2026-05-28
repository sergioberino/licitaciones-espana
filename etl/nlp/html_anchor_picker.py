from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class StaticDocumentAnchor:
    href: str
    text: str
    score: int


_WHITESPACE_RE = re.compile(r"\s+")
_IGNORED_TEXT_PARENTS = {"script", "style", "noscript"}
_NEGATIVE_TERMS = (
    "admision",
    "admisiones",
    "denegacion",
    "denegaciones",
    "listado",
    "sorteo",
    "fe de erratas",
)


def pick_static_html_document_anchor(
    html: str | bytes, *, base_url: str
) -> StaticDocumentAnchor | None:
    soup = BeautifulSoup(html, "html.parser")
    best: StaticDocumentAnchor | None = None

    for anchor in soup.find_all("a", href=True):
        href = _normalized_fetchable_href(str(anchor["href"]), base_url=base_url)
        if href is None:
            continue

        text = _anchor_text(anchor)
        score = _score_anchor_text(text)
        if score <= 0:
            continue

        candidate = StaticDocumentAnchor(
            href=href,
            text=text,
            score=score,
        )
        if best is None or candidate.score > best.score:
            best = candidate

    return best


def _normalized_fetchable_href(href: str, *, base_url: str) -> str | None:
    href = href.strip()
    if not href or href.startswith("#"):
        return None

    normalized = urljoin(base_url, href)
    if urlparse(normalized).scheme not in {"http", "https"}:
        return None
    return normalized


def _anchor_text(anchor) -> str:
    parts: list[str] = []
    parts.extend(
        string.strip()
        for string in anchor.find_all(string=True)
        if string.parent and string.parent.name not in _IGNORED_TEXT_PARENTS
    )

    for attr in ("aria-label", "title"):
        value = anchor.get(attr)
        if value:
            parts.append(str(value))

    for image in anchor.find_all("img"):
        for attr in ("alt", "title"):
            value = image.get(attr)
            if value:
                parts.append(str(value))

    for svg_title in anchor.find_all("title"):
        title_text = svg_title.get_text(" ", strip=True)
        if title_text:
            parts.append(title_text)

    return _squash_whitespace(" ".join(part for part in parts if part))


def _score_anchor_text(text: str) -> int:
    normalized = _normalize_for_matching(text)
    if not normalized:
        return 0
    if any(term in normalized for term in _NEGATIVE_TERMS):
        return 0
    if (
        "bases reguladoras" in normalized
        or "norma reguladora" in normalized
        or "regulad" in normalized
        or _has_word(normalized, "bases")
    ):
        return 500
    if "convocatoria" in normalized:
        return 400
    if "descripcion del programa" in normalized:
        return 300
    if "tipologia de gastos elegibles" in normalized:
        return 200
    if "declaracion responsable" in normalized:
        return 100
    return 0


def _normalize_for_matching(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    without_accents = "".join(
        char for char in decomposed if not unicodedata.combining(char)
    )
    return _squash_whitespace(without_accents.casefold())


def _squash_whitespace(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def _has_word(text: str, word: str) -> bool:
    return re.search(rf"\b{re.escape(word)}\b", text) is not None
