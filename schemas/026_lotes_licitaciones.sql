-- Tabla de lotes de licitaciones nacionales (PLACSP/CODICE).
-- Identidad: (expediente, dir3_organo, num_lote).
-- Sin FK a l0.nacional_licitaciones: el par expediente+dir3 es la clave real de la licitación,
-- estable a través de múltiples entries (eventos/publicaciones) del mismo expediente.
-- El UPSERT incremental con COALESCE permite enriquecer filas sin machacar datos ya informados
-- cuando llegan entries posteriores (adjudicación, formalización...).

CREATE TABLE IF NOT EXISTS l0.lotes_licitaciones (
  id                  BIGSERIAL    PRIMARY KEY,
  expediente          TEXT         NOT NULL,
  dir3_organo         TEXT         NOT NULL,
  num_lote            SMALLINT     NOT NULL,

  -- Definición del lote (cac:ProcurementProjectLot → cac:ProcurementProject)
  objeto              TEXT,
  importe_sin_iva     NUMERIC,
  importe_con_iva     NUMERIC,
  cpv                 TEXT,        -- Códigos CPV del lote separados por ";" (ej: "92521000;92310000")
  ubicacion           TEXT,        -- cac:RealizedLocation → cbc:CountrySubentity
  nut                 TEXT,        -- cac:RealizedLocation → cbc:CountrySubentityCode

  -- Adjudicación del lote (cac:TenderResult cuyo ProcurementProjectLotID == num_lote)
  fecha_adjudicacion  DATE,
  nif_adjudicado      TEXT,
  empresa_adjudicada  TEXT,
  importe_sin_iva_adj NUMERIC,
  importe_con_iva_adj NUMERIC,

  updated_at          TIMESTAMPTZ  DEFAULT NOW(),

  UNIQUE (expediente, dir3_organo, num_lote)
);

CREATE INDEX IF NOT EXISTS idx_lotes_licitaciones_expediente
  ON l0.lotes_licitaciones (expediente);

CREATE INDEX IF NOT EXISTS idx_lotes_licitaciones_dir3_organo
  ON l0.lotes_licitaciones (dir3_organo);

CREATE INDEX IF NOT EXISTS idx_lotes_licitaciones_nif_adjudicado
  ON l0.lotes_licitaciones (nif_adjudicado);
