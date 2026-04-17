"""Tests for descargar_archivo() download behavior (no stale ZIP cache)."""

from unittest.mock import MagicMock

import pytest
import requests

from nacional.licitaciones import descargar_archivo


def test_descargar_archivo_redownloads_existing_file(tmp_path):
    """Existing file >1KB must be replaced by a fresh download, not skipped."""
    filepath = tmp_path / "monthly.zip"
    old_content = b"x" * 2048
    filepath.write_bytes(old_content)

    new_content = b"y" * 2048
    response = MagicMock()
    response.status_code = 200
    response.headers.get = lambda key, default=None: (
        str(len(new_content)) if key == "Content-Length" else default
    )
    response.raise_for_status = MagicMock()
    response.iter_content = MagicMock(side_effect=lambda chunk_size=65536: iter([new_content]))

    session = MagicMock()
    session.get = MagicMock(return_value=response)

    result = descargar_archivo(session, "https://example.com/file.zip", filepath)

    session.get.assert_called_once_with("https://example.com/file.zip", timeout=600, stream=True)
    assert result == filepath
    assert filepath.read_bytes() == new_content


def test_descargar_archivo_cleans_stale_file_on_failure(tmp_path):
    """On 404, a stale file from a previous run must not remain."""
    filepath = tmp_path / "monthly.zip"
    stale = b"z" * 2048
    filepath.write_bytes(stale)

    response = MagicMock()
    response.status_code = 404

    def raise_for_status():
        err = requests.exceptions.HTTPError()
        err.response = response
        raise err

    response.raise_for_status = raise_for_status

    session = MagicMock()
    session.get = MagicMock(return_value=response)

    result = descargar_archivo(session, "https://example.com/missing.zip", filepath)

    assert result is None
    assert not filepath.exists()
