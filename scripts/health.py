#!/usr/bin/env python3
"""
P1 health script: Python version, virtualenv (host), required packages, optional tmp/staging dir.
Runnable from repo root: python scripts/health.py or python -m scripts.health
"""
import os
import sys

MIN_PYTHON = (3, 10)
REQUIRED = ("pandas", "pyarrow", "requests")


def _ok(msg: str) -> None:
    print(f"OK {msg}")


def _fail(msg: str) -> None:
    print(f"FAIL {msg}")


def main() -> int:
    failed = False

    # Python version
    if sys.version_info >= MIN_PYTHON:
        _ok(f"python {sys.version_info.major}.{sys.version_info.minor}")
    else:
        _fail(f"python {sys.version_info.major}.{sys.version_info.minor} (need >= {MIN_PYTHON[0]}.{MIN_PYTHON[1]})")
        failed = True

    # Virtualenv on host; skip or warn in Docker (no venv expected)
    in_docker = os.path.exists("/.dockerenv") or os.environ.get("DOCKER") == "1"
    if in_docker:
        pass  # skip venv check
    else:
        venv = os.environ.get("VIRTUAL_ENV")
        if venv:
            _ok("virtualenv")
        else:
            _fail("virtualenv (activate .venv when running on host)")
            failed = True

    # Required packages
    for name in REQUIRED:
        try:
            __import__(name)
            _ok(name)
        except ImportError as e:
            _fail(name)
            failed = True

    # Optional: tmp/staging dir exists and writable
    tmp_dir = os.environ.get("LICITACIONES_TMP_DIR")
    if tmp_dir:
        from pathlib import Path
        p = Path(tmp_dir)
        if p.exists() and os.access(p, os.W_OK):
            _ok(f"tmp dir writable: {tmp_dir}")
        elif not p.exists():
            try:
                p.mkdir(parents=True, exist_ok=True)
                _ok(f"tmp dir created: {tmp_dir}")
            except OSError:
                _fail(f"tmp dir: {tmp_dir} (cannot create)")
                failed = True
        else:
            _fail(f"tmp dir: {tmp_dir} (not writable)")
            failed = True
    # if not set, skip

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
