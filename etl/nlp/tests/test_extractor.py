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


# -----------------------------------------------------------------------------
# Hito 3.1.f — ExtractedText.extraction_mode (logging explícito BDNS octet→PDF)
# -----------------------------------------------------------------------------


def _make_pdf_bytes() -> bytes:
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def test_extraction_mode_pdf_native(httpx_mock: HTTPXMock):
    """application/pdf → modo 'pdf_native'. PDF vacío sigue lanzando ExtractionError."""
    pdf_bytes = _make_pdf_bytes()
    httpx_mock.add_response(
        url="https://example.org/native.pdf",
        content=pdf_bytes,
        headers={"content-type": "application/pdf"},
    )
    # En este caso el PDF está vacío y ExtractionError se levanta antes de poder leer
    # el modo del resultado, así que solo verificamos que el branch elegido fue PDF
    # nativo (no octet fallback) parcheando la función de extracción.
    from unittest.mock import patch

    with patch("etl.nlp.extractor._extract_pdf", return_value="contenido pdf nativo") as mocked:
        r = extract_from_url("https://example.org/native.pdf")
    assert mocked.called
    assert r.extraction_mode == "pdf_native"
    assert r.text == "contenido pdf nativo"


def test_extraction_mode_octet_fallback_pdf(httpx_mock: HTTPXMock):
    """application/octet-stream + bytes PDF reales → modo 'octet_fallback_pdf'."""
    httpx_mock.add_response(
        url="https://example.org/bdns.bin",
        content=_make_pdf_bytes(),
        headers={"content-type": "application/octet-stream"},
    )
    from unittest.mock import patch

    with patch("etl.nlp.extractor._extract_pdf", return_value="contenido extraido") as mocked:
        r = extract_from_url("https://example.org/bdns.bin")
    assert mocked.called
    assert r.extraction_mode == "octet_fallback_pdf"


def test_extraction_mode_octet_fallback_html(httpx_mock: HTTPXMock):
    """application/octet-stream + bytes HTML → modo 'octet_fallback_html'."""
    html = b"<html><body><p>texto util</p></body></html>"
    httpx_mock.add_response(
        url="https://example.org/bdns.bin",
        content=html,
        headers={"content-type": "application/octet-stream"},
    )
    r = extract_from_url("https://example.org/bdns.bin")
    assert "texto util" in r.text
    assert r.extraction_mode == "octet_fallback_html"


def test_extraction_mode_html_direct(httpx_mock: HTTPXMock):
    """text/html → modo 'html'."""
    html = b"<html><body><p>texto html directo</p></body></html>"
    httpx_mock.add_response(
        url="https://example.org/page",
        content=html,
        headers={"content-type": "text/html"},
    )
    r = extract_from_url("https://example.org/page")
    assert r.extraction_mode == "html"
