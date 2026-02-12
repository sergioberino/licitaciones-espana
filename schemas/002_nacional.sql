-- Nacional (PLACSP): one table per conjunto (SRP).
-- Same column set per README "48 columnas"; each table has a single responsibility.
-- Ingestion routes rows by parquet discriminator column (conjunto) into the correct table.
-- Validated (P2) against Parquet from nacional/licitaciones.py; Parquet↔schema mapping in docs/extraction-contract.md.

-- Shared column set for all nacional tables (PLACSP shape)
-- id, authority/geography/cpv refs, dates, amounts, adjudicación, clasificación, audit
-- All columns nullable except id (PK), created_at, updated_at (e.g. adjudicatario may be unpublished).

CREATE TABLE IF NOT EXISTS nacional_licitaciones (
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
  expediente      TEXT,
  objeto          TEXT,
  nif_organo      VARCHAR(9),
  dir3_organo     VARCHAR(9),
  ciudad_organo   TEXT,
  tipo_contrato   TEXT,
  subtipo_code    TEXT,
  procedimiento   TEXT,
  estado          TEXT,
  importe_con_iva NUMERIC(18, 2),
  importe_adjudicacion NUMERIC(18, 2),
  adjudicatario   TEXT,
  nif_adjudicatario VARCHAR(9),
  num_ofertas     INTEGER,
  es_pyme         BOOLEAN,
  cpvs            INTEGER[],  -- array of dim.cpv_dim.num_code (8-digit); each element must exist in dim.cpv_dim
  ubicacion       TEXT,
  nuts            TEXT,
  fecha_adjudicacion TIMESTAMPTZ,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nacional_agregacion_ccaa (
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
  expediente      TEXT,
  objeto          TEXT,
  nif_organo      VARCHAR(9),
  dir3_organo     VARCHAR(9),
  ciudad_organo   TEXT,
  tipo_contrato   TEXT,
  subtipo_code    TEXT,
  procedimiento   TEXT,
  estado          TEXT,
  importe_con_iva NUMERIC(18, 2),
  importe_adjudicacion NUMERIC(18, 2),
  adjudicatario   TEXT,
  nif_adjudicatario VARCHAR(9),
  num_ofertas     INTEGER,
  es_pyme         BOOLEAN,
  cpvs            INTEGER[],
  ubicacion       TEXT,
  nuts            TEXT,
  fecha_adjudicacion TIMESTAMPTZ,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nacional_contratos_menores (
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
  expediente      TEXT,
  objeto          TEXT,
  nif_organo      VARCHAR(9),
  dir3_organo     VARCHAR(9),
  ciudad_organo   TEXT,
  tipo_contrato   TEXT,
  subtipo_code    TEXT,
  procedimiento   TEXT,
  estado          TEXT,
  importe_con_iva NUMERIC(18, 2),
  importe_adjudicacion NUMERIC(18, 2),
  adjudicatario   TEXT,
  nif_adjudicatario VARCHAR(9),
  num_ofertas     INTEGER,
  es_pyme         BOOLEAN,
  cpvs            INTEGER[],
  ubicacion       TEXT,
  nuts            TEXT,
  fecha_adjudicacion TIMESTAMPTZ,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nacional_encargos_medios_propios (
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
  expediente      TEXT,
  objeto          TEXT,
  nif_organo      VARCHAR(9),
  dir3_organo     VARCHAR(9),
  ciudad_organo   TEXT,
  tipo_contrato   TEXT,
  subtipo_code    TEXT,
  procedimiento   TEXT,
  estado          TEXT,
  importe_con_iva NUMERIC(18, 2),
  importe_adjudicacion NUMERIC(18, 2),
  adjudicatario   TEXT,
  nif_adjudicatario VARCHAR(9),
  num_ofertas     INTEGER,
  es_pyme         BOOLEAN,
  cpvs            INTEGER[],
  ubicacion       TEXT,
  nuts            TEXT,
  fecha_adjudicacion TIMESTAMPTZ,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS nacional_consultas_preliminares (
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
  expediente      TEXT,
  objeto          TEXT,
  nif_organo      VARCHAR(9),
  dir3_organo     VARCHAR(9),
  ciudad_organo   TEXT,
  tipo_contrato   TEXT,
  subtipo_code    TEXT,
  procedimiento   TEXT,
  estado          TEXT,
  importe_con_iva NUMERIC(18, 2),
  importe_adjudicacion NUMERIC(18, 2),
  adjudicatario   TEXT,
  nif_adjudicatario VARCHAR(9),
  num_ofertas     INTEGER,
  es_pyme         BOOLEAN,
  cpvs            INTEGER[],
  ubicacion       TEXT,
  nuts            TEXT,
  fecha_adjudicacion TIMESTAMPTZ,
  raw_extra       JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes per domain (same pattern for each nacional table)
CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_authority ON nacional_licitaciones(authority_id);
CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_published ON nacional_licitaciones(published_at);
CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_geography ON nacional_licitaciones(geography_id);
CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_deadline ON nacional_licitaciones(deadline_at);
CREATE INDEX IF NOT EXISTS idx_nacional_licitaciones_cpv ON nacional_licitaciones(cpv_id);

CREATE INDEX IF NOT EXISTS idx_nacional_agregacion_ccaa_authority ON nacional_agregacion_ccaa(authority_id);
CREATE INDEX IF NOT EXISTS idx_nacional_agregacion_ccaa_published ON nacional_agregacion_ccaa(published_at);
CREATE INDEX IF NOT EXISTS idx_nacional_agregacion_ccaa_geography ON nacional_agregacion_ccaa(geography_id);

CREATE INDEX IF NOT EXISTS idx_nacional_contratos_menores_authority ON nacional_contratos_menores(authority_id);
CREATE INDEX IF NOT EXISTS idx_nacional_contratos_menores_published ON nacional_contratos_menores(published_at);
CREATE INDEX IF NOT EXISTS idx_nacional_contratos_menores_geography ON nacional_contratos_menores(geography_id);

CREATE INDEX IF NOT EXISTS idx_nacional_encargos_authority ON nacional_encargos_medios_propios(authority_id);
CREATE INDEX IF NOT EXISTS idx_nacional_encargos_published ON nacional_encargos_medios_propios(published_at);

CREATE INDEX IF NOT EXISTS idx_nacional_consultas_authority ON nacional_consultas_preliminares(authority_id);
CREATE INDEX IF NOT EXISTS idx_nacional_consultas_published ON nacional_consultas_preliminares(published_at);
