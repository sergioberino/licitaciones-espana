# P1 deployment: Python app for licitaciones-espana (WSL/Linux)
# Single stage; no Windows paths.

FROM python:3.12-slim

WORKDIR /app

# Default tmp/staging base so DATA_DIR and OUTPUT_DIR resolve under it in Linux
ENV LICITACIONES_TMP_DIR=/app/tmp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Install ETL CLI so licitia-etl is available in container
RUN pip install --no-cache-dir -e .

# Default: keep container up; validation uses: docker compose run --rm app python scripts/health.py
CMD ["tail", "-f", "/dev/null"]
