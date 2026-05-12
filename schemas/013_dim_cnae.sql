-- dim.cnae_dim: CNAE-2025 dimension (Clasificación Nacional de Actividades Económicas).
-- Source: ISTAC SDMX API. Data populated on-demand via `licitia-etl cnae ingest`.
-- Embedding column added by Indexer at runtime (ALTER TABLE ... ADD COLUMN embedding).

CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.cnae_dim (
    id        SERIAL PRIMARY KEY,
    code      VARCHAR(8) NOT NULL UNIQUE,
    label     TEXT NOT NULL,
    parent_id INTEGER
);

COMMENT ON COLUMN dim.cnae_dim.parent_id IS 'Referencia lógica a dim.cnae_dim(id). 
NULL para secciones raíz (nivel 1, código de una letra). Permite reconstruir la jerarquía
CNAE desde nivel 4 hasta nivel 1.';
