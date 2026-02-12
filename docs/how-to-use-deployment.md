# How to run the ETL microservice (Docker)

This doc describes how to run **this repo** in Docker only. You need a Postgres instance (local or remote); set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` in `.env` (see `.env.example`).

## Prerequisites

- **Docker** and **Docker Compose**

## Setup and run

From this repo root:

```bash
cp .env.example .env
# Edit .env: set DB_HOST (e.g. localhost or postgres service name), DB_NAME, DB_USER, DB_PASSWORD

docker compose build
docker compose up -d
```

This runs only the **etl** service. The compose mounts the repo as `/app` and `./tmp` as `/app/tmp`. Postgres must be running elsewhere; point `DB_HOST` in `.env` to it.

## ETL CLI (licitia-etl)

Entrypoint: **`licitia-etl`**. Use it for connection checks, DB initialization, and populating the CPV router (embedding index). The service is intended to be run by **scheduled tasks** (e.g. cron, orchestrator); batch sizes are configured via environment variables.

### Command reference and intended usage

| Command | Intended usage | Notes |
|--------|----------------|-------|
| **check-connection** | Validate PostgreSQL connectivity (e.g. before init-db or in health checks). | Exit 0 if OK. Uses `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. |
| **check-embedding** | Validate that the embedding service is reachable (e.g. before generate_embedding). | GET `/openapi.json`. Uses `EMBEDDING_SERVICE_URL`. |
| **init-db** | Apply schema migrations and populate **dim** (e.g. dim.cpv_dim). Run once per environment or after schema changes. | Applies SQL files in order: 001_dim_cpv → 001b_dim_cpv_router → 002 → 003 → 004 → 005. Idempotent. Optional: `--schema-dir` or `LICITACIONES_SCHEMAS_DIR`. |
| **generate_embedding** | Populate **dim.cpv_router** from dim.cpv_dim using the embedding service. Intended for **scheduled runs** (e.g. after init-db or when CPV taxonomy is updated). | Reads `EMBED_BATCH_SIZE` (batch size for embedding API) and `INGEST_BATCH_SIZE` (rows per bulk INSERT). Optional: `--target cpv`, `--embedding-service-url`. |

### Default environment variables for generate_embedding

| Variable | Default | Description |
|----------|---------|-------------|
| **EMBED_BATCH_SIZE** | `256` | Number of texts sent per batch to the embedding service. 256 is a reasonable value for **multilingual-e5-large** in production (throughput vs memory). |
| **INGEST_BATCH_SIZE** | `10000` | Number of rows per bulk INSERT into Postgres (dim.cpv_router). Balances transaction size and round-trips. |

These are **environment variables only** (no CLI flags), so scheduled/orchestrated runs use a single configuration source.

### One-off run (new container, then removed)

```bash
docker compose run --rm etl licitia-etl --help
docker compose run --rm etl licitia-etl check-connection
docker compose run --rm etl licitia-etl init-db
docker compose run --rm etl licitia-etl generate_embedding --target cpv
```

### Inside the running container

```bash
docker compose exec etl licitia-etl check-connection
docker compose exec etl licitia-etl init-db
docker compose exec etl licitia-etl generate_embedding --target cpv
```

## Health script

```bash
docker compose run --rm etl python scripts/health.py
```

Exit code 0 = all checks passed.

## Interacting with the container

**Shell in the running etl container:**

```bash
docker compose exec etl bash
# You are at /app. Examples:
#   licitia-etl check-connection
#   python scripts/health.py
#   python nacional/licitaciones.py --help
```

**Other commands:**

```bash
docker compose ps
docker compose logs -f etl
docker compose down
```

## Environment variables

- **`.env`** — Copy from `.env.example`, set at least `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. The compose uses it via `env_file: - .env`.
- **`LICITACIONES_TMP_DIR`** — Base for staging/output (default in container: `/app/tmp`). Optional: `LICITACIONES_DATA_DIR`, `LICITACIONES_OUTPUT_DIR` for the nacional script.
- **`EMBEDDING_SERVICE_URL`** — Base URL for the embedding API (default: `http://embedding:8000`). Used by `generate_embedding`.
- **`EMBED_BATCH_SIZE`** — Batch size for embedding service calls (default: `256`). Used by `generate_embedding`.
- **`INGEST_BATCH_SIZE`** — Rows per bulk INSERT into Postgres (default: `10000`). Used by `generate_embedding`.

## Quick start

```bash
cp .env.example .env
# Edit .env with DB_* and optional LICITACIONES_TMP_DIR

docker compose build
docker compose up -d
docker compose run --rm etl licitia-etl check-connection
```

## Operational notes

See [operational-notes.md](operational-notes.md) for audit tasks and known issues (e.g. Postgres errors during `generate_embedding` cpv_router ingestion).
