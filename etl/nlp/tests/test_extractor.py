import io

import pytest
import httpx
from pytest_httpx import HTTPXMock

from etl.nlp.extractor import extract_from_url, extract_from_text, ExtractionError


def test_extract_from_text():
    r = extract_from_text("hola mundo")
    assert r.text == "hola mundo"
    assert r.content_type == "text/plain"
    assert r.char_count == 10


def test_extract_from_text_empty_raises():
    with pytest.raises(ExtractionError):
        extract_from_text("   ")


def test_extract_html(httpx_mock: HTTPXMock):
    html = b"<html><body><p>texto util</p><script>x()</script></body></html>"
    httpx_mock.add_response(
        url="https://example.org/a",
        content=html,
        headers={"content-type": "text/html"},
    )
    r = extract_from_url("https://example.org/a")
    assert "texto util" in r.text
    assert "x()" not in r.text


def test_extract_pdf_with_minimal_pdf(httpx_mock: HTTPXMock):
    """Generamos un PDF mínimo en memoria con pypdf."""
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buf = io.BytesIO()
    writer.write(buf)
    pdf_bytes = buf.getvalue()
    httpx_mock.add_response(
        url="https://example.org/b.pdf",
        content=pdf_bytes,
        headers={"content-type": "application/pdf"},
    )
    with pytest.raises(ExtractionError):
        extract_from_url("https://example.org/b.pdf")


def test_http_error_raises(httpx_mock: HTTPXMock):
    httpx_mock.add_response(url="https://example.org/c", status_code=404)
    with pytest.raises(ExtractionError):
        extract_from_url("https://example.org/c")
