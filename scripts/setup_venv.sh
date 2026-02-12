#!/usr/bin/env sh
# P1: create .venv at repo root and install from requirements.txt (Linux/WSL/macOS).
set -e
cd "$(dirname "$0")/.."
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
echo "Done. Activate with: . .venv/bin/activate"
