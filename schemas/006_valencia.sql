-- Valencia: one table with dataset_type discriminator (SRP per category).
-- Many categories (contratacion, subvenciones, lobbies, empleo, paro, etc.); common core + raw_extra.
-- dataset_type values: contratacion, subvenciones, presupuestos, convenios, lobbies, empleo, paro, siniestralidad, patrimonio, entidades, territorio, turismo, sanidad, transporte.
-- Validated (P2) against Parquet from scripts/ccaa_valencia_parquet.py; Parquet↔schema mapping in docs/extraction-contract.md.

CREATE TABLE IF NOT EXISTS valencia_opportunities (
  id              TEXT PRIMARY KEY,
  dataset_type    TEXT,
  title           TEXT,
  description_raw TEXT,
  authority_id    TEXT REFERENCES authorities(id),
  authority_name  TEXT,
  geography_id    TEXT REFERENCES geography(id),
  region_name     TEXT,
  published_at    TIMESTAMPTZ,
  deadline_at     TIMESTAMPTZ,
  budget_amount   NUMERIC(18, 2),
  cpv_id          INTEGER REFERENCES dim.cpv_dim(num_code),
  source_url      TEXT,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_valencia_opportunities_dataset_type ON valencia_opportunities(dataset_type);
CREATE INDEX IF NOT EXISTS idx_valencia_opportunities_authority ON valencia_opportunities(authority_id);
CREATE INDEX IF NOT EXISTS idx_valencia_opportunities_published ON valencia_opportunities(published_at);
CREATE INDEX IF NOT EXISTS idx_valencia_opportunities_geography ON valencia_opportunities(geography_id);
