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
from typing import Optional

import httpx


@dataclass
class LLMResult:
    raw_text: str
    input_tokens: int
    output_tokens: int
    duration_ms: int
    model: str


SYSTEM_PROMPT = """Eres un analista legal especializado en bases reguladoras
de subvenciones españolas. Extrae la información estructurada según el esquema
JSON v5.0.2 proporcionado. Sigue exactamente el schema; no inventes campos.
Cuando un dato no aparezca, usa estado_extraccion='no_encontrado'."""


def load_schema() -> dict:
    p = Path(__file__).parent / "schema_v5_0_2.json"
    return json.loads(p.read_text())


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


def analyze_document(text: str, schema: dict) -> LLMResult:
    """Envia text al LLM con structured output v5.0.2."""
    provider = os.environ.get("NLP_LLM_PROVIDER", "openai")
    model = os.environ.get("NLP_LLM_MODEL", "gpt-4.1-mini")
    timeout_s = float(os.environ.get("NLP_LLM_TIMEOUT_S", "180"))
    if provider == "openai":
        api_key = os.environ["OPENAI_API_KEY"]
        return _openai_call(text, schema, model, api_key, timeout_s)
    raise NotImplementedError(f"LLM provider {provider} not implemented")
