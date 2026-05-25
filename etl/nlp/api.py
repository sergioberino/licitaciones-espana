"""Helpers para los endpoints HTTP del módulo NLP."""
from __future__ import annotations

import os
from pathlib import Path


def nlp_log_path() -> Path:
    base = Path(os.environ.get("LICITACIONES_TMP_DIR", "/tmp/licitaciones"))
    base.mkdir(parents=True, exist_ok=True)
    return base / "nlp_analizar.log"


def nlp_pid_path() -> Path:
    return nlp_log_path().with_suffix(".pid")
