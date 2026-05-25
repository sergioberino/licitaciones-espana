import os

import pytest
from pytest_httpx import HTTPXMock

from etl.nlp.llm_client import analyze_document, load_schema


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
