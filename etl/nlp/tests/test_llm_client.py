import json
import os

import pytest
from pytest_httpx import HTTPXMock

from etl.nlp.llm_client import SYSTEM_PROMPT, analyze_document, load_schema


def test_load_schema_returns_dict():
    schema = load_schema()
    assert isinstance(schema, dict)
    assert "$defs" in schema or "type" in schema


def test_analyze_document_openai(httpx_mock: HTTPXMock, monkeypatch):
    monkeypatch.setenv("NLP_LLM_PROVIDER", "openai")
    monkeypatch.setenv("NLP_LLM_MODEL", "gpt-test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("NLP_LLM_TIMEOUT_S", "30")
    httpx_mock.add_response(
        method="POST",
        url="https://api.openai.com/v1/chat/completions",
        json={
            "model": "gpt-test",
            "choices": [{"message": {"content": '{"foo": "bar"}'}}],
            "usage": {"prompt_tokens": 1234, "completion_tokens": 567},
        },
    )
    result = analyze_document("texto cualquiera", {"type": "object"})
    assert result.raw_text == '{"foo": "bar"}'
    assert result.input_tokens == 1234
    assert result.output_tokens == 567


def test_load_prompt_returns_canonical_content():
    assert len(SYSTEM_PROMPT) > 25_000
    assert "## Requisitos geográficos" in SYSTEM_PROMPT
    assert "codigos_nuts" in SYSTEM_PROMPT
    assert "codigos_ine" in SYSTEM_PROMPT
    assert "Reglas anti-sobrerrevisión" in SYSTEM_PROMPT
    assert "beneficiario formal vs miembros internos" in SYSTEM_PROMPT
    assert "modalidad_lgs_refuerzo" in SYSTEM_PROMPT
    assert "TBD" not in SYSTEM_PROMPT
    assert "(truncated" not in SYSTEM_PROMPT


def test_analyze_document_envia_prompt_canonico(httpx_mock: HTTPXMock, monkeypatch):
    monkeypatch.setenv("NLP_LLM_PROVIDER", "openai")
    monkeypatch.setenv("NLP_LLM_MODEL", "gpt-test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("NLP_LLM_TIMEOUT_S", "30")
    httpx_mock.add_response(
        method="POST",
        url="https://api.openai.com/v1/chat/completions",
        json={
            "model": "gpt-test",
            "choices": [{"message": {"content": "{}"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
        },
    )
    analyze_document("texto", {"type": "object"})
    request = httpx_mock.get_requests()[0]
    body = json.loads(request.content)
    system = body["messages"][0]
    assert system["role"] == "system"
    assert "codigos_nuts" in system["content"]
    assert "## Requisitos geográficos" in system["content"]
