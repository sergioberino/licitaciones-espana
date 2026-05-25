"""Heurística de resolución de documento normativo para Batch B.

Orden:
  1. url_bases_reguladoras   → step=1, source='url_bases_reguladoras'
  2. documentos[] con tipo/descripción "bases reguladoras" → step=2
  3. texto_reguladora        → step=3, source='texto_reguladora'
  Fallback: None → pendiente, no error.

document_key:
  - source url_*: 'url:' + sha256(normalized_url)[:32]
  - source texto: 'text:' + sha256(texto)[:32]
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class ResolvedDocument:
    document_key: str
    document_source: str   # 'url_bases_reguladoras' | 'documentos_array' | 'texto_reguladora'
    heuristic_step: int    # 1 | 2 | 3
    document_ref: Optional[str]  # URL o None si source=texto
    document_name: Optional[str] = None  # Solo poblado en step=2 (documentos_array). Informativo.


_BASES_REGULADORAS_HINT = re.compile(r"bases?\s+regulad", re.IGNORECASE)


def _normalize_url(url: str) -> str:
    return url.strip().rstrip("/").lower()


def _hash_key(prefix: str, payload: str) -> str:
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"{prefix}:{digest}"


def _is_descargable(url: Optional[str]) -> bool:
    if not url:
        return False
    url = url.strip()
    if not url.lower().startswith(("http://", "https://")):
        return False
    return True


def _scan_documentos(documentos: Any) -> Optional[tuple[str, str]]:
    """Busca un documento que sugiera bases reguladoras.
    Devuelve (url, name) si encuentra; None si no.
    name = primer campo no vacío entre {nombre, titulo, descripcion, tipo}.
    """
    if not documentos or not isinstance(documentos, list):
        return None
    for doc in documentos:
        if not isinstance(doc, dict):
            continue
        haystack = " ".join(
            str(doc.get(k, "")) for k in ("tipo", "descripcion", "nombre", "titulo")
        )
        if _BASES_REGULADORAS_HINT.search(haystack):
            url = doc.get("url") or doc.get("urlDescarga") or doc.get("href")
            if _is_descargable(url):
                name = next(
                    (str(doc[k]).strip() for k in ("nombre", "titulo", "descripcion", "tipo")
                     if doc.get(k)),
                    "documento sin nombre",
                )
                return (url, name)
    return None


def resolve_document(
    *,
    url_bases_reguladoras: Optional[str],
    documentos: Any,
    texto_reguladora: Optional[str],
) -> Optional[ResolvedDocument]:
    if _is_descargable(url_bases_reguladoras):
        normalized = _normalize_url(url_bases_reguladoras)
        return ResolvedDocument(
            document_key=_hash_key("url", normalized),
            document_source="url_bases_reguladoras",
            heuristic_step=1,
            document_ref=url_bases_reguladoras,
            document_name=None,
        )

    doc_match = _scan_documentos(documentos)
    if doc_match:
        doc_url, doc_name = doc_match
        normalized = _normalize_url(doc_url)
        return ResolvedDocument(
            document_key=_hash_key("url", normalized),
            document_source="documentos_array",
            heuristic_step=2,
            document_ref=doc_url,
            document_name=doc_name,
        )

    if texto_reguladora and texto_reguladora.strip():
        return ResolvedDocument(
            document_key=_hash_key("text", texto_reguladora.strip()),
            document_source="texto_reguladora",
            heuristic_step=3,
            document_ref=None,
            document_name=None,
        )

    return None
