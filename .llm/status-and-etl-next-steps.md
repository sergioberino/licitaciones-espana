# Repository status & ETL next steps (Hito-6 first layer)

This document summarizes: (1) status of this repository against the ETL TR in Archon RAG, (2) in-scope tasks (ETL migration responsibility, cpv_dim integration), (3) possible refactorizations and alignment with TDR/README, (4) server-management / CLI / scheduler needs for the first layer, and (5) open questions to narrow scope and create Archon tasks.

**References:** Archon RAG holds the **Technical Requirements – ETL & Repository Service** (PDF), **adr-hito6**, the **Indexer** TDR, and the **TRD – Data Segmentation & Ingestion Strategy** (HOT/COLD layers). This repo is the **first layer** of the hito-6 pipeline (data + schemas + future ETL service that applies schemas and ingests L0).

**ETL TR (Archon) in short:** The ETL microservice is responsible for **pushing raw data (L0)** into PostgreSQL **alongside derived attributes**: append-only L0 tables, lightweight CPV parsing and derivation (raw + validated numeric + prefixes such as principal_cpv8, principal_prefix4/6, secondary_prefix6), **creation/maintenance of required database schemas and indexes (via migrations)**, and watermarks for idempotent runs. **L1/L2 materialization is not ETL**—it is performed by the **Indexer**. Downstream (Indexer) reads **raw** (L0) tables and **dim.cpv_dim** to build L1 and L2.

**Schema naming (adr-hito6):** Three data layers map to three schemas: **L0 → raw**, **L1 → entity**, **L2 → canonical**. Plus **dim** (reference data, e.g. CPV) and **ops** (watermarks, logs). The **schemas/** folder does not yet define these layer schemas; current SQL uses **public** and **dim** (dim.cpv_dim only).

**Cold vs Hot (TRD – Data Segmentation & Ingestion):** The TRD defines a **Cold layer** (L0) and a **Hot layer** (data that, given the timespan, exists in L1/L2). Cold = **raw** schema (append-only, ETL-populated). Hot = **entity** (L1) and **canonical** (L2), populated by the **Indexer**, not by the ETL microservice.

**ETL boundary (no L1/L2 data in this service):** The ETL microservice is responsible for (1) **creating** the schemas according to the data hierarchy (raw, entity, canonical, dim, ops) and (2) **populating L0 (raw)** and **dim** (e.g. dim.cpv_dim). It does **not** populate or transform data into **entity** (L1) or **canonical** (L2). Those layers are populated by the **Indexer** microservice, which reads from raw and dim. So we do **not** work inside the ETL service on L1/L2 data—that respects the project architecture.

---

## 1. What’s done from the ETL TR (Archon RAG)

Alignment with **Technical Requirements – ETL & Repository Service** in Archon RAG:

| ETL TR requirement | Repo status | Notes |
|--------------------|-------------|--------|
| **Schemas live in codebase** | Done | `schemas/` with 001, 001b, 002–005; single source of truth for DDL. |
| **raw / entity / canonical / dim / ops** | Partial | **dim** and **dim.cpv_dim** are in place (001b). Schema naming adopted: **L0→raw**, **L1→entity**, **L2→canonical** (adr-hito6). The **schemas/** folder does not yet define **raw**, **entity**, **canonical**, or **ops**; current domain tables live in **public**. ETL creates schemas and populates only **raw** (L0) and **dim**; Indexer populates **entity** (L1) and **canonical** (L2). |
| **CPV dimension table** | Done | **dim.cpv_dim** in `001b_dim_cpv.sql`: **num_code** (INTEGER PK, 8-digit, control digit stripped), **code** (VARCHAR, original with control digit), **label** (TEXT). ~9.45k rows. Domain tables (002–004) reference `dim.cpv_dim(num_code)` (Option B). Source: `schemas/cpv_code.sql`; regeneration: `scripts/cpv_dim_convert.py`. TR’s cpv_code8/label satisfied via num_code/label. |
| **Creation/maintenance of schemas (migrations)** | Not documented | TR says ETL is responsible for “Creation/maintenance of required database schemas and indexes (via migrations)”. Repo README/schemas say “apply manually or via initdb”. Contract that **ETL applies migrations on connect/start** is not yet written. |
| **L0 append-only, bulk COPY, watermarks** | Not implemented | No ETL service code yet. Extraction scripts produce Parquet; no ingestion into Postgres in this repo. TR: ETL pushes raw data (L0) + CPV-derived attributes; L0 tables should have indexes btree(principal_prefix4), btree(principal_prefix6), GIN(secondary_prefix6) when raw schema is introduced. |
| **Environment for DB** | Done | `.env.example` and `.env` updated with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (and/or `DATABASE_URL`) for Postgres. |
| **Docker / run environment** | Done | `docker-compose.yml`, `Dockerfile`, `scripts/health.py` (Python, venv, packages, tmp dir). No DB connectivity check in health. |

**Summary:** Extraction/Parquet pipeline and **dim.cpv_dim** are in place. ETL **service** (apply migrations, ingest L0 with raw + CPV attributes, watermarks) is not implemented. Remaining doc task: document ETL migration responsibility. **Schema refactor** to align current domain tables with TR’s “raw L0 + attributes” and with the repo README is to be defined as a **later Archon task** (see section 4).

---

## 2. In-scope tasks (this repo)

### 2.1 Document ETL migration responsibility (re-create tables on connect/start) — *pending*

**Goal:** Make it explicit that the ETL & Repository Service is responsible for creating/updating database objects when connecting to Postgres.

**Suggested implementation:**

- **Where:** Either extend `schemas/README.md` and/or add a short doc under `.llm/` (e.g. `etl-schema-responsibility.md`), and optionally add a line in the ETL TR copy or a “TR summary” in repo if it exists.
- **Content to add:**
  - ETL must run migrations (the contents of `schemas/*.sql` in order 001→001b→002→003→004→005, and any future files) **on startup** or **before first ingest** when it connects to Postgres.
  - If the Postgres volume was recreated (or tables are missing), ETL re-creates tables by applying these scripts; no DDL is stored in the Postgres container/repo.
  - Optionally: “ensure schema” step contract (e.g. idempotent apply of `schemas/*.sql`; scripts already use `CREATE TABLE IF NOT EXISTS` so re-run is safe).
- **Open:** Should “run migrations” be a dedicated CLI subcommand (e.g. `etl init-db` or `etl ensure-schema`) or only an automatic step on ETL startup? (Affects Server/CLI design below.)

### 2.2 Integrate cpv_dim into licitaciones-espana/schemas per ETL TR — **Done**

**Goal:** Have a CPV dimension table that matches the ETL TR and aligns with Parquet/L0 (8-digit numeric, no control digit), while preserving the original public source (with control digit).

**Implemented (Option B):**

- **dim.cpv_dim** in `schemas/001b_dim_cpv.sql`: **num_code** (INTEGER PK, 8-digit, control digit stripped), **code** (VARCHAR, original with control digit), **label** (TEXT). Apply order: 001 → **001b** → 002 → 003 → 004 → 005.
- **FKs migrated** to `dim.cpv_dim(num_code)`: 002_nacional, 003_catalunya, 004_valencia reference `REFERENCES dim.cpv_dim(num_code)`; `cpv_codes` removed from 001_reference.sql.
- **Source and regeneration:** Source dump is `schemas/cpv_code.sql`. Script `scripts/cpv_dim_convert.py` strips the control digit and writes `001b_dim_cpv.sql` (DDL + INSERT ~9.45k rows). Run `python3 scripts/cpv_dim_convert.py` to regenerate after source changes.
- TR’s “cpv_code8 (INTEGER, PK), label (TEXT)” is satisfied by **num_code** and **label**; **code** keeps original for auditability.

---

## 3. Repository status document (summary)

- **Extraction (Phase 2):** Scripts for nacional, Catalunya, Valencia, comunidad_madrid; output to Parquet under tmp; extraction contract in `docs/extraction-contract.md`. **Done for current scope.**
- **Schemas:** 001_reference (authorities, geography), **001b_dim_cpv** (dim.cpv_dim + data), 002_nacional, 003_catalunya, 004_valencia, 005_views; validated against Parquet; apply order **001 → 001b → 002 → 003 → 004 → 005**. **dim.cpv_dim aligned with ETL TR (Option B).**
- **PostgreSQL:** Not in this repo; separate Postgres workspace with docker-compose/.env/health. **Done.**
- **ETL service:** Not implemented. **First iteration:** Python CLI with `check-connection` and `init-db` (see **.llm/archon-tasks-etl-cli-first-iteration.md**). Planned per Archon TR: apply schemas on connect/start, push raw data (L0) alongside CPV-derived attributes, watermarks. Service will be deployed in a Docker container.
- **Deployment:** Docker Compose for app (this repo), health script (Python, deps, tmp). **Done for P1.** CLI and init flow to be added (first iteration).

---

## 4. Possible refactorizations and alignment with TDR / README

- **Schema layout (raw / entity / canonical / dim / ops):** **Adopted:** L0→**raw**, L1→**entity**, L2→**canonical** (adr-hito6), plus **dim** (reference), **ops** (watermarks). **dim** and dim.cpv_dim are done. The **schemas/** folder does not yet define **raw**, **entity**, **canonical**, or **ops**; current domain tables (nacional_*, etc.) are in **public**. ETL responsibility: **create** these schemas (by level) and **populate only raw (L0)** and **dim**. ETL does **not** populate entity (L1) or canonical (L2)—the **Indexer** does. **Next:** Define DDL for raw, entity, canonical, ops in schemas/ and align with TRD Cold/Hot and ETL TR (raw + CPV attributes, indexes). Refactor of existing public tables vs raw shape to be defined as needed (e.g. Archon task).
- **health.py:** Add optional Postgres connectivity check (e.g. try connect with `DATABASE_URL` or `DB_*` if set; report OK/FAIL). Low risk; improves operability.
- **.env.example:** Add ETL/Postgres variables (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, and/or DATABASE_URL) so Server/DevOps can configure the ETL microservice without guessing.

---

## 5. Server management / Server department (first layer)

Because this repo is the **first layer** of hito-6 (adr-hito6), the following are useful for “server management” and the Server department. **First iteration (Archon task batch):** Python CLI with `check-connection` and `init-db`; see **.llm/archon-tasks-etl-cli-first-iteration.md**.

- **CLI for initialization**  
  **First task.** `etl init-db`: runs `schemas/*.sql` in order (001→001b→002→003→004→005) against Postgres. Ensures schema and **dim** (including dim.cpv_dim). Idempotent. Docker: CLI must work in container (env from .env). Ensures schema exists after volume recreate or new environment. May be the same as “ensure schema on ETL startup” or a separate command for manual/automated setup.

- **Check connection**  
  **First iteration.** `etl check-connection` (or `etl check-db`): validate Postgres; exit 0/1. Same DB env as init-db. For Docker health checks and Server/DevOps.

- **Check scheduler**  
  If ETL runs on a schedule (cron, external scheduler), a “scheduler status” or “last run” check helps operations. Could be a small CLI subcommand that reads from ops table or a simple file/artifact written by ETL.

- **Single entrypoint CLI**  
  `etl` or `python -m etl`. First iteration: `check-connection`, `init-db`. Later: `status`, `ingest`, scheduler.

- **Dim schema (TDR)**  
  Per ETL TDR, **dim** holds static reference data (semantic support for Indexer and Backend). init-db creates and populates dim (e.g. dim.cpv_dim).

- **Check status / scheduler**  
  Later: status (Postgres + tables, last watermark), scheduler (last run). Not in first batch.

**CLI location:** This repo (licitaciones-espana) is the ETL microservice; the CLI lives here. First iteration: see .llm/archon-tasks-etl-cli-first-iteration.md.

---

## 6. Open questions (for narrowing scope and Archon tasks)

*Do not remove these; expand or add as needed.*

1. **cpv_dim integration:** Option B is implemented. `.env.example` and `.env` are updated with DB_* / DATABASE_URL. Remaining: any other cpv_dim follow-ups (e.g. hierarchy metadata in dim.cpv_dim)?
2. **Migrations:** Should “apply schemas” be only automatic on ETL startup, or also a dedicated CLI command (e.g. `etl init-db`) for manual/Server use?
3. **.env.example:** Done. `.env.example` and `.env` are set up with Postgres connection variables for the ETL microservice.
4. **CLI location:** Is the ETL microservice (and thus init/status/scheduler CLI) to be implemented in **this** repo, or in a separate ETL repo? If separate, this repo may only need docs and schemas; if here, we need a task batch for the CLI and ETL service.
5. **Schema split — addressed:** Schema names adopted: **L0→raw**, **L1→entity**, **L2→canonical** (adr-hito6), plus **dim**, **ops**. Cold layer = raw (L0); Hot layer = entity (L1) + canonical (L2) per TRD Data Segmentation & Ingestion. ETL creates schemas and populates **only raw (L0)** and **dim**; it does **not** work on L1/L2 data (Indexer does). Remaining: define **raw**, **entity**, **canonical**, **ops** DDL in schemas/ (not yet done).
6. **health.py:** Add optional Postgres check (connect when DB vars set)? Yes/no and priority?
7. **Schema refactor vs TDR and README:** Schema hierarchy is decided (raw / entity / canonical / dim / ops). ETL pushes **raw data (L0)** into **raw** schema with CPV-derived attributes; it does not populate entity/canonical. Remaining: add DDL for **raw**, **entity**, **canonical**, **ops** in schemas/ and, if needed, an Archon task to align existing public/domain tables with the raw L0 model and README dataset structure.

Once these are decided, the remaining doc task (ETL migration responsibility) can be implemented and the rest turned into concrete Archon tasks (CLI, status, scheduler check, ETL service skeleton, schema/TR/README refactor, etc.).
