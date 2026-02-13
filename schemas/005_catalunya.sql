-- Catalunya: one table per dataset (SRP).
-- Common core: id, authority/geography refs, title, description_raw, dates, amounts, source_url, raw_extra.
-- Schemas differ per dataset (subvenciones vs contratación vs presupuestos, etc.); raw_extra holds variant fields.
-- Validated (P2) against Parquet from scripts/ccaa_cataluna_parquet.py; Parquet↔schema mapping in docs/extraction-contract.md.

CREATE TABLE IF NOT EXISTS catalunya_subvenciones (
  id              TEXT PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS catalunya_contratacion_publica (
  id              TEXT PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS catalunya_pressupostos (
  id              TEXT PRIMARY KEY,
  title           TEXT,
  description_raw TEXT,
  authority_id    TEXT REFERENCES authorities(id),
  authority_name  TEXT,
  geography_id    TEXT REFERENCES geography(id),
  region_name     TEXT,
  published_at    TIMESTAMPTZ,
  deadline_at     TIMESTAMPTZ,
  budget_amount   NUMERIC(18, 2),
  source_url      TEXT,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalunya_convenios (
  id              TEXT PRIMARY KEY,
  title           TEXT,
  description_raw TEXT,
  authority_id    TEXT REFERENCES authorities(id),
  authority_name  TEXT,
  geography_id    TEXT REFERENCES geography(id),
  region_name     TEXT,
  published_at    TIMESTAMPTZ,
  deadline_at     TIMESTAMPTZ,
  budget_amount   NUMERIC(18, 2),
  source_url      TEXT,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalunya_rrhh (
  id              TEXT PRIMARY KEY,
  title           TEXT,
  description_raw TEXT,
  authority_id    TEXT REFERENCES authorities(id),
  authority_name  TEXT,
  geography_id    TEXT REFERENCES geography(id),
  region_name     TEXT,
  published_at    TIMESTAMPTZ,
  deadline_at     TIMESTAMPTZ,
  budget_amount   NUMERIC(18, 2),
  source_url      TEXT,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalunya_patrimoni (
  id              TEXT PRIMARY KEY,
  title           TEXT,
  description_raw TEXT,
  authority_id    TEXT REFERENCES authorities(id),
  authority_name  TEXT,
  geography_id    TEXT REFERENCES geography(id),
  region_name     TEXT,
  published_at    TIMESTAMPTZ,
  deadline_at     TIMESTAMPTZ,
  budget_amount   NUMERIC(18, 2),
  source_url      TEXT,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_catalunya_subvenciones_authority ON catalunya_subvenciones(authority_id);
CREATE INDEX IF NOT EXISTS idx_catalunya_subvenciones_published ON catalunya_subvenciones(published_at);
CREATE INDEX IF NOT EXISTS idx_catalunya_contratacion_authority ON catalunya_contratacion_publica(authority_id);
CREATE INDEX IF NOT EXISTS idx_catalunya_contratacion_published ON catalunya_contratacion_publica(published_at);
CREATE INDEX IF NOT EXISTS idx_catalunya_pressupostos_authority ON catalunya_pressupostos(authority_id);
CREATE INDEX IF NOT EXISTS idx_catalunya_convenios_authority ON catalunya_convenios(authority_id);
CREATE INDEX IF NOT EXISTS idx_catalunya_rrhh_authority ON catalunya_rrhh(authority_id);
CREATE INDEX IF NOT EXISTS idx_catalunya_patrimoni_authority ON catalunya_patrimoni(authority_id);
