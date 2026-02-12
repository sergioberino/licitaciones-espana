# How to run the ETL microservice

This microservice is an **atomic plug-and-play unit**: it works in isolation (set `DB_*` and `EMBEDDING_SERVICE_URL` to your endpoints) or as part of a larger stack. All configuration is via environment variables.

This section describes running **this repo** in Docker. You need a Postgres instance (local or remote); set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` in `.env` (see `.env.example`).

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

Entrypoint: **`licitia-etl`**. Use it for connection checks, DB initialization, and populating the CPV router (embedding index). Suitable for **scheduled tasks** (e.g. cron or job runner); batch sizes are configured via environment variables.

### Command reference and intended usage

| Command | Intended usage | Notes |
|--------|----------------|-------|
| **status** | Comprueba el estado del ETL (conexión a PostgreSQL y al servicio de embedding). | Exit 0 si todo OK. Usa `DB_*` y `EMBEDDING_SERVICE_URL`. Mensajes en español. |
| **init-db** | Aplica migraciones de esquema y rellena **dim** (p. ej. dim.cpv_dim). Ejecutar una vez por entorno o tras cambios de esquema. | Aplica SQL en orden: 001_dim_cpv → 001b_dim_cpv_router → 002 → 003 → 004 → 005. Idempotente. Opcional: `--schema-dir` o `LICITACIONES_SCHEMAS_DIR`. |
| **generate_embedding** | Rellena **dim.cpv_router** desde dim.cpv_dim usando el servicio de embedding. Para **ejecuciones programadas** (tras init-db o al actualizar la taxonomía CPV). | Lee `EMBED_BATCH_SIZE`, `INGEST_BATCH_SIZE`, `EMBED_MAX_WORKERS`. Opcional: `--target cpv`, `--embedding-service-url`. |

### Default environment variables for generate_embedding

| Variable | Default | Description |
|----------|---------|-------------|
| **EMBED_BATCH_SIZE** | `256` | Number of texts sent per batch to the embedding service. 256 is a reasonable value for **multilingual-e5-large** in production (throughput vs memory). |
| **INGEST_BATCH_SIZE** | `10000` | Number of rows per bulk INSERT into Postgres (dim.cpv_router). Balances transaction size and round-trips. |

These are **environment variables only** (no CLI flags), so scheduled runs use a single configuration source.

### One-off run (new container, then removed)

```bash
docker compose run --rm etl licitia-etl --help
docker compose run --rm etl licitia-etl status
docker compose run --rm etl licitia-etl init-db
docker compose run --rm etl licitia-etl generate_embedding --target cpv
```

### Inside the running container

```bash
docker compose exec etl licitia-etl status
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
#   licitia-etl status
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
docker compose run --rm etl licitia-etl status
```

## Operational notes

See [operational-notes.md](operational-notes.md) for audit tasks and known issues (e.g. Postgres errors during `generate_embedding` cpv_router ingestion).
