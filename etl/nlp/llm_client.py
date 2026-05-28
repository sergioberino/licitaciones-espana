"""Cliente LLM para Batch B con structured output v5.0.2.

Backend: OpenAI Chat Completions con response_format={"type": "json_schema", ...}
        o Anthropic con tools/JSON mode, según LLM_PROVIDER env.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx


@dataclass
class LLMResult:
    raw_text: str
    input_tokens: int
    output_tokens: int
    duration_ms: int
    model: str


def _load_prompt() -> str:
    p = Path(__file__).parent / "prompts" / "v5_0_2.md"
    return p.read_text(encoding="utf-8")


SYSTEM_PROMPT: str = _load_prompt()


def load_schema() -> dict:
    p = Path(__file__).parent / "schema_v5_0_2.json"
    return json.loads(p.read_text())


def _build_user_prompt(
    text: str,
    *,
    tipo_documento: str | None = None,
    convocatoria_detalle: Mapping[str, Any] | None = None,
) -> str:
    """Construye el prompt de usuario con contexto documental semántico.

    No incluye trazas técnicas de resolución (heuristic_step, document_source,
    document_key); esas pertenecen a logs/observabilidad, no al LLM.
    """
    if not tipo_documento and not convocatoria_detalle:
        return text

    parts = ["CONTEXTO_DOCUMENTAL"]
    if tipo_documento:
        parts.append(f"tipo_documento: {tipo_documento}")

    if convocatoria_detalle:
        bdns = json.dumps(
            dict(convocatoria_detalle),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
        parts.extend(["", "CONVOCATORIA_DETALLE_BDNS", bdns])

    parts.extend(["", "TEXTO_DOCUMENTAL", text])
    return "\n".join(parts)


def _openai_call(prompt: str, schema: dict, model: str, api_key: str, timeout_s: float) -> LLMResult:
    started = time.time()
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "bases_reguladoras_analisis_v5_0_2",
                "strict": True,
                "schema": schema,
            },
        },
        "temperature": 0.0,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    with httpx.Client(timeout=timeout_s) as client:
        resp = client.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return LLMResult(
        raw_text=data["choices"][0]["message"]["content"],
        input_tokens=data["usage"]["prompt_tokens"],
        output_tokens=data["usage"]["completion_tokens"],
        duration_ms=int((time.time() - started) * 1000),
        model=data["model"],
    )


def analyze_document(
    text: str,
    schema: dict,
    *,
    tipo_documento: str | None = None,
    convocatoria_detalle: Mapping[str, Any] | None = None,
) -> LLMResult:
    """Envia text al LLM con structured output v5.0.2."""
    provider = os.environ.get("NLP_LLM_PROVIDER", "openai")
    model = os.environ.get("NLP_LLM_MODEL", "gpt-5.4-nano")
    timeout_s = float(os.environ.get("NLP_LLM_TIMEOUT_S", "180"))
    prompt = _build_user_prompt(
        text,
        tipo_documento=tipo_documento,
        convocatoria_detalle=convocatoria_detalle,
    )
    if provider == "openai":
        api_key = os.environ["OPENAI_API_KEY"]
        return _openai_call(prompt, schema, model, api_key, timeout_s)
    raise NotImplementedError(f"LLM provider {provider} not implemented")
