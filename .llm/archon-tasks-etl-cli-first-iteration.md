# Archon tasks: ETL microservice CLI (first iteration)

Use this document to create the following tasks in Archon. Aligned with **.llm/status-and-etl-next-steps.md** and the **ETL Microservice TDR** (Archon RAG). This service will be **deployed in a Docker container**; the CLI must work when run inside the container (env from `.env` / `env_file`).

**Context (TDR):** The **dim** schema holds **static reference data** that provides semantic support for the rest of the architecture (Indexer microservice, Backend service). ETL is responsible for creation/maintenance of schemas and indexes (via migrations) and for populating dim (e.g. **dim.cpv_dim**). First iteration focuses on CLI for connection check and DB initialization (schemas + dim ingestion).

---

## Suggested CLI surface (first iteration)

- **Entrypoint:** `etl` or `python -m etl` (runnable on host and inside Docker).
- **Commands (v1):**
  1. **check-connection** (or **check-db**) — Validate PostgreSQL connection. For now Postgres is the only DB. Exit 0 if reachable, non-zero otherwise; optional short message (e.g. "OK" / "FAIL").
  2. **init-db** — Idempotent schema creation and **dim** tables ingestion: run `schemas/*.sql` in order **001 → 001b → 002 → 003 → 004 → 005**. This creates public + dim schemas and populates **dim.cpv_dim** (~9.45k rows from 001b). Safe to re-run (CREATE IF NOT EXISTS; INSERT may need upsert/skip logic if 001b is re-applied, or rely on 001b being idempotent by design).
- **Config:** Use `DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (see `.env.example`).

**Later (out of scope for this batch):** `status`, `ingest`, scheduler checks.

---

## Task 1: Create Python CLI for the ETL microservice (this repo)

**Title:** Create Python CLI for ETL microservice (licitaciones-espana)

**Description:** Add a Python CLI as the single entrypoint for the ETL microservice in this repo. The CLI will be used for initialization, connection checks, and (later) ingest and scheduler checks. It must run on host and inside the Docker container; configuration via environment variables (DATABASE_URL or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD per .env.example). Prefer a subcommand-based interface (e.g. `etl <command> [options]`). Implementation can use argparse, click, or typer. Reference: .llm/status-and-etl-next-steps.md (section 5), ETL TDR (Archon RAG).

**Acceptance criteria:**
- One entrypoint: `etl` or `python -m etl` with subcommands.
- CLI runs successfully when invoked from repo root (host) and from inside the app container (Docker).
- Environment variables for DB connection are read from the process environment (no hardcoded credentials).
- Documentation or --help describes available commands.

**Notes:** Docker deployment: ensure the image includes the CLI and that `docker compose run app etl --help` (or equivalent) works when .env is loaded via env_file.

---

## Task 2: Implement `check-connection` (validate Postgres)

**Title:** Implement CLI command: check-connection (validate Postgres)

**Description:** Implement the `check-connection` (or `check-db`) subcommand. It must validate that the ETL service can connect to PostgreSQL using the configured credentials (DATABASE_URL or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD). Postgres is the only database in scope for this iteration. Align with ETL TDR: ETL connects to PostgreSQL over the network. Reference: .llm/status-and-etl-next-steps.md, .env.example.

**Acceptance criteria:**
- Command `etl check-connection` (or `etl check-db`) attempts a connection to Postgres.
- Exit code 0 if connection succeeds, non-zero on failure (e.g. unreachable, wrong credentials).
- Clear output (e.g. "OK" / "Connection successful" or "FAIL" / error message).
- Uses the same env vars as init-db (so that a successful check-connection implies init-db can run).

**Notes:** Useful for Docker health checks and for Server/DevOps to verify connectivity before running init-db or ingest.

---

## Task 3: Implement `init-db` (schema creation and dim ingestion)

**Title:** Implement CLI command: init-db (schema creation and dim ingestion)

**Description:** Implement the `init-db` subcommand. It must apply all schema SQL files in order so that (1) schemas (public, dim) and tables exist, and (2) **dim** schema is populated with static reference data. Per ETL TDR, the **dim** schema holds static data that serves as semantic support for the Indexer and Backend. Apply order: **001_reference.sql → 001b_dim_cpv.sql → 002_nacional.sql → 003_catalunya.sql → 004_valencia.sql → 005_views.sql** (see schemas/README.md). This creates authorities, geography, **dim.cpv_dim** (with ~9.45k CPV rows from 001b), and domain tables. Idempotent where possible (CREATE TABLE IF NOT EXISTS; 001b is DDL + INSERT). Reference: .llm/status-and-etl-next-steps.md, schemas/README.md, ETL TDR (creation/maintenance of schemas via migrations).

**Acceptance criteria:**
- Command `etl init-db` runs the SQL files from `schemas/` in the correct order (001 → 001b → 002 → 003 → 004 → 005).
- After a successful run, PostgreSQL has public schema (authorities, geography, nacional_*, catalunya_*, valencia_*, v_opportunities_all) and **dim** schema with **dim.cpv_dim** populated.
- Safe to run against an empty database (fresh volume). Behavior when objects already exist: no failure (idempotent); if 001b re-inserts cause constraint errors, document or use upsert/skip logic.
- Uses DB connection from environment (same as check-connection).
- Path to schemas: configurable or default to repo-relative `schemas/` so it works in Docker (e.g. /app/schemas).

**Notes:** This fulfils the "CLI for initialization" and "ETL applies migrations" contract in the status doc. Docker: typically run once after container start or after Postgres volume recreate (e.g. `docker compose run app etl init-db`).

---

## Task 4: Quick guide / README for CLI and service initialization

**Title:** Add quick guide or README for ETL CLI and service initialization

**Description:** Add a short guide (or section in README) that explains how to use the ETL CLI and how to initialize the service, including when running in Docker. Align with .llm/status-and-etl-next-steps.md and the ETL TDR. Audience: developers and Server/DevOps.

**Acceptance criteria:**
- Document how to run the CLI (host and Docker): e.g. `etl --help`, `etl check-connection`, `etl init-db`.
- Document required environment variables (DB connection) and point to .env.example.
- Document the recommended initialization flow: e.g. (1) ensure Postgres is running, (2) set .env or DB_* / DATABASE_URL, (3) `etl check-connection`, (4) `etl init-db`.
- Mention that the service is deployed in a Docker container and how to run the CLI from the container (e.g. `docker compose run app etl init-db` or `docker compose exec app etl check-connection`).
- Optional: one-line "quick start" for local and Docker.

**Notes:** Can live in docs/etl-cli.md or a new section in the repo README. Keep it short; link to schemas/README.md for schema apply order and dim.cpv_dim.

---

## Summary table (for Archon)

| # | Title | Summary |
|---|--------|----------|
| 1 | Create Python CLI for ETL microservice (licitaciones-espana) | Single entrypoint `etl` with subcommands; works on host and in Docker; config from env. |
| 2 | Implement CLI command: check-connection (validate Postgres) | Validate Postgres connectivity; exit 0/1; same env as init-db. |
| 3 | Implement CLI command: init-db (schema creation and dim ingestion) | Run schemas 001→001b→002→003→004→005; create public + dim and populate dim.cpv_dim; idempotent. |
| 4 | Add quick guide / README for ETL CLI and service initialization | How to use CLI (host + Docker), env vars, init flow, Docker usage. |

**Suggested task order:** 1 → 2, 3 (2 and 3 can be parallel after 1), then 4.

**Project:** Create or use an "ETL Microservice (licitaciones-espana)" project in Archon; these tasks are the first iteration (CLI + init + docs).
