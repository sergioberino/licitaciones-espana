-- dim.cnae_dim: CNAE-2025 dimension (Clasificación Nacional de Actividades Económicas).
-- Source: ISTAC SDMX API. Data populated on-demand via `licitia-etl cnae ingest`.
-- Embedding column added by Indexer at runtime (ALTER TABLE ... ADD COLUMN embedding).

CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.cnae_dim (
    code    VARCHAR(8) PRIMARY KEY,
    label   TEXT NOT NULL
);
