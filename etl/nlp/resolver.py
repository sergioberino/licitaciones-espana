"""Heurística de resolución de documento normativo para Batch B.

Orden:
  1. url_bases_reguladoras                                  → step=1, source='url_bases_reguladoras'
  2. documentos[] con descripcion/nombreFic 'bases reguladoras'  → step=2, source='documentos_array'
  3. documentos[] con descripcion/nombreFic 'texto/documento de la convocatoria'
                                                            → step=3, source='texto_convocatoria'
  Fallback: None → pendiente, no error.

Estructura real de documentos[] (BDNS SNPSAP):
  {
    "id": <int>,                  # ID descargable. URL: /convocatorias/documentos?idDocumento=ID
    "long": <int>,                # tamaño en bytes
    "datMod": "YYYY-MM-DD",
    "nombreFic": <str>,           # nombre original del fichero
    "descripcion": <str>,         # categoría semántica oficial
    "datPublicacion": "YYYY-MM-DD"
  }

Notas:
  - Todos los steps producen un document_key con prefijo 'url:' porque siempre
    apuntan a un documento descargable.
  - Selección entre múltiples candidatos válidos: max(datPublicacion) — preferimos la
    versión vigente más reciente. Justificación: las bases reguladoras no suelen
    modificarse, pero los textos de convocatoria sí se readaptan cada línea, y queremos
    siempre la versión vigente.
  - Anti-anexo: descartamos cualquier documento con 'anexo' o 'solicitud' como rol
    semántico en descripcion/nombreFic. Estos NO representan las bases ni la convocatoria.
  - 'descripcion_bases_reguladoras' (TEXT en l0.nacional_subvenciones) NO se usa como
    fuente NLP — su contenido suele ser solo referencia BOE.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Optional

BDNS_DOCUMENTO_URL_PATTERN = (
    "https://www.infosubvenciones.es/bdnstrans/api/convocatorias/documentos"
    "?idDocumento={id}"
)


@dataclass(frozen=True)
class ResolvedDocument:
    document_key: str
    document_source: str   # 'url_bases_reguladoras' | 'documentos_array' | 'texto_convocatoria'
    heuristic_step: int    # 1 | 2 | 3
    document_ref: str      # URL siempre poblada
    document_name: Optional[str] = None  # Solo en step=2/3 (informativo)


_BASES_REGULADORAS_HINT = re.compile(r"bases?\s+regulad", re.IGNORECASE)
_TEXTO_CONVOCATORIA_HINT = re.compile(
    r"(?:texto|documento)\s+\S+(?:\s+\S+){0,5}\s+convocatoria", re.IGNORECASE
)
# Antianexo: rol semántico de anexo / solicitud / formulario / extracto, no documento canónico.
# El anti-match aplica si la palabra aparece como token autónomo en el haystack.
_ANEXO_BLACKLIST = re.compile(
    r"\b(?:anexo|anexos|solicitud|formulario|extracto|extracte)\b", re.IGNORECASE
)


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


_SEPARATOR_RE = re.compile(r"[^\w]+")


def _doc_haystack(doc: dict) -> str:
    """Concatena los campos textuales del documento BDNS para los matchers.

    Normaliza cualquier separador no alfanumérico a espacio para que las regex
    funcionen igual con 'Bases reguladoras' que con 'bases-reguladoras-2024.pdf'.
    """
    raw = " ".join(str(doc.get(k, "")) for k in ("descripcion", "nombreFic"))
    return _SEPARATOR_RE.sub(" ", raw).strip()


def _doc_is_anexo(doc: dict) -> bool:
    return bool(_ANEXO_BLACKLIST.search(_doc_haystack(doc)))


def _pick_best_candidate(candidates: list[dict]) -> Optional[dict]:
    """Selecciona el candidato más reciente por datPublicacion (versión vigente).

    Tolerante a ausencia de datPublicacion: los items sin fecha quedan al final
    (cadena vacía es menor que cualquier ISO date no vacía).
    """
    if not candidates:
        return None
    return max(candidates, key=lambda d: d.get("datPublicacion") or "")


def _scan_documentos(
    documentos: Any, hint: re.Pattern[str]
) -> Optional[tuple[int, str]]:
    """Busca documentos BDNS cuyo haystack match con `hint` y NO sean anexos.

    Devuelve `(id, descripcion)` del candidato vigente seleccionado por
    max(datPublicacion), o None si no hay match válido.
    """
    if not documentos or not isinstance(documentos, list):
        return None

    candidates: list[dict] = []
    for doc in documentos:
        if not isinstance(doc, dict):
            continue
        haystack = _doc_haystack(doc)
        if not haystack:
            continue
        if not hint.search(haystack):
            continue
        if _doc_is_anexo(doc):
            continue
        if not isinstance(doc.get("id"), int):
            continue
        candidates.append(doc)

    best = _pick_best_candidate(candidates)
    if best is None:
        return None

    name = str(best.get("descripcion") or best.get("nombreFic") or "documento sin nombre").strip()
    return (best["id"], name)


def resolve_document(
    *,
    url_bases_reguladoras: Optional[str],
    documentos: Any,
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

    bases_match = _scan_documentos(documentos, _BASES_REGULADORAS_HINT)
    if bases_match:
        doc_id, doc_name = bases_match
        url = BDNS_DOCUMENTO_URL_PATTERN.format(id=doc_id)
        return ResolvedDocument(
            document_key=_hash_key("url", _normalize_url(url)),
            document_source="documentos_array",
            heuristic_step=2,
            document_ref=url,
            document_name=doc_name,
        )

    texto_match = _scan_documentos(documentos, _TEXTO_CONVOCATORIA_HINT)
    if texto_match:
        doc_id, doc_name = texto_match
        url = BDNS_DOCUMENTO_URL_PATTERN.format(id=doc_id)
        return ResolvedDocument(
            document_key=_hash_key("url", _normalize_url(url)),
            document_source="texto_convocatoria",
            heuristic_step=3,
            document_ref=url,
            document_name=doc_name,
        )

    return None
