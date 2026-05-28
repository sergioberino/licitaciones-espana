"""Adaptador HTTP del microservicio `browser-service`.

Este módulo es un *anti-corruption layer* del lado consumidor: encapsula el
contrato del endpoint `POST /resolve` y traduce su respuesta a un tipo
inmutable que el resto del pipeline NLP puede consumir.

NO contiene lógica de scraping (eso vive en `services/browser-service/`):
  - No conoce `link_picker`, regex normativos, `decide_outcome`, ni Playwright.
  - Solo serializa la petición, valida la respuesta y mapea errores HTTP a
    excepciones del ETL.

Contrato (alineado con `services/browser-service/app.py::ResolveResponse`):

    Request:   POST {BROWSER_SERVICE_URL}/resolve   {"url": "<URL>"}
    Response:  200  {"outcome": "pdf_url"|"text"|"unresolvable",
                     "pdf_url":   <str | null>,
                     "text":      <str | null>,
                     "final_url": <str | null>,
                     "render_ms": <int | null>,
                     "reason":    <str | null>}

Errores de transporte / 4xx / 5xx → `BrowserUnavailable`. El caller decide
si caer al fallback httpx o marcar el item como no resoluble.

Modo degradado: si `BROWSER_SERVICE_URL` no está configurada (compose sin
overlay del servicio, ejecución local) `resolve_via_browser` lanza
`BrowserUnavailable` antes de tocar la red, para que el caller pueda saltar
limpio sin colgar segundos en timeouts.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Literal, Optional

import httpx

logger = logging.getLogger(__name__)

Outcome = Literal["pdf_url", "text", "unresolvable"]
_VALID_OUTCOMES: frozenset[str] = frozenset({"pdf_url", "text", "unresolvable"})

# Timeout amplio: render headless + decide_outcome puede llegar a ~45s en
# sitios con WAF (BORM/Radware). El servidor tiene su propio cap a 60s
# (`RESOLVE_TIMEOUT_SECONDS`); aquí cogemos un poco menos para que el cliente
# corte antes que el servidor y devolvamos un error claro.
_DEFAULT_TIMEOUT = httpx.Timeout(50.0, connect=5.0)


class BrowserUnavailable(Exception):
    """`browser-service` no está alcanzable, no está configurado o devuelve un
    error que el ETL no puede recuperar. El caller debe tratarlo como señal
    para usar fallback (httpx puro) o marcar el item como `skipped_no_doc`.
    """


@dataclass(frozen=True)
class BrowserOutcome:
    """Respuesta normalizada del browser-service."""

    outcome: Outcome
    pdf_url: Optional[str] = None
    text: Optional[str] = None
    final_url: Optional[str] = None
    render_ms: Optional[int] = None
    reason: Optional[str] = None


def _service_url() -> Optional[str]:
    url = os.environ.get("BROWSER_SERVICE_URL")
    return url.rstrip("/") if url else None


def is_configured() -> bool:
    """True si `BROWSER_SERVICE_URL` está definido. Útil para que el pipeline
    decida si intentar el step=headless o saltarlo directamente."""
    return _service_url() is not None


def resolve_via_browser(url: str) -> BrowserOutcome:
    """Llama a `browser-service POST /resolve` y devuelve el outcome.

    Raises:
        BrowserUnavailable: servicio no configurado, no alcanzable, 4xx/5xx,
            timeout, o respuesta con `outcome` desconocido.
    """
    base = _service_url()
    if not base:
        raise BrowserUnavailable("BROWSER_SERVICE_URL not configured")

    try:
        with httpx.Client(timeout=_DEFAULT_TIMEOUT) as client:
            resp = client.post(f"{base}/resolve", json={"url": url})
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        raise BrowserUnavailable(f"browser-service unreachable: {exc}") from exc

    if resp.status_code >= 500:
        raise BrowserUnavailable(
            f"browser-service 5xx: {resp.status_code} {resp.text[:200]}"
        )
    if resp.status_code >= 400:
        raise BrowserUnavailable(
            f"browser-service 4xx: {resp.status_code} {resp.text[:200]}"
        )

    try:
        data = resp.json()
    except ValueError as exc:
        raise BrowserUnavailable(f"browser-service non-JSON response: {exc}") from exc

    outcome = data.get("outcome")
    if outcome not in _VALID_OUTCOMES:
        raise BrowserUnavailable(f"browser-service unknown outcome: {outcome!r}")

    return BrowserOutcome(
        outcome=outcome,
        pdf_url=data.get("pdf_url"),
        text=data.get("text"),
        final_url=data.get("final_url"),
        render_ms=data.get("render_ms"),
        reason=data.get("reason"),
    )
