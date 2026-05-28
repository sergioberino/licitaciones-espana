-- 030_l0_subvenciones_nlp.sql
-- Tabla NLP v5.0.2 deduplicada por document_key.
-- N convocatorias en l0.nacional_subvenciones pueden compartir el mismo análisis
-- si comparten documento normativo (URL o texto).

CREATE TABLE IF NOT EXISTS l0.subvenciones_nlp (
  document_key            TEXT        NOT NULL,
  document_source         VARCHAR(30) NOT NULL,
  document_heuristic_step SMALLINT    NOT NULL,
  document_ref            TEXT,

  schema_version          VARCHAR(10) NOT NULL DEFAULT 'v5.0.2',
  extracted_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  modelo_documental       VARCHAR(50),
  input_char_count        INTEGER,
  llm_model               VARCHAR(80),
  validation_status       VARCHAR(20) NOT NULL DEFAULT 'invalid',
  validation_errors       JSONB,

  modalidad_lgs           VARCHAR(50),
  mecanismo_seleccion     VARCHAR(50),
  puntuacion_minima_acceso SMALLINT,
  regimen_ue_tipo         VARCHAR(30),
  rgec_articulo           VARCHAR(20),
  tipo_beneficiario_fino  VARCHAR(30),
  texto_beneficiario_fino TEXT,
  admite_consorcio        BOOLEAN,
  art13_lgs_verificado    BOOLEAN,
  intensidad_maxima_pct   NUMERIC(5,2),
  importe_maximo_beneficiario NUMERIC(12,2),
  regla_calculo_ayuda     VARCHAR(50),
  modalidad_pago          VARCHAR(30),
  garantia_pago           VARCHAR(30),
  regimen_acumulacion     VARCHAR(40),

  -- NUTS: VARCHAR(5)[] (códigos máx 5 chars). INE: VARCHAR(6)[] (5 + dígito control opcional).
  domicilio_fiscal_nuts        VARCHAR(5)[],
  domicilio_fiscal_ine         VARCHAR(6)[],
  domicilio_fiscal_tipo        VARCHAR(40),
  centros_trabajo_nuts         VARCHAR(5)[],
  centros_trabajo_ine          VARCHAR(6)[],
  ejecucion_nuts               VARCHAR(5)[],
  ejecucion_ine                VARCHAR(6)[],
  ejecucion_municipios_anexo   BOOLEAN,
  impacto_nuts                 VARCHAR(5)[],
  geo_patron                   VARCHAR(60),
  geo_condiciones_suficientes  JSONB,

  nlp_json                JSONB NOT NULL,

  CONSTRAINT pk_subvenciones_nlp PRIMARY KEY (document_key),
  CONSTRAINT chk_subvenciones_nlp_validation_status
    CHECK (validation_status IN ('valid','partial','invalid')),
  CONSTRAINT chk_subvenciones_nlp_document_source
    CHECK (document_source IN ('url_bases_reguladoras','documentos_array','texto_convocatoria')),
  CONSTRAINT chk_subvenciones_nlp_heuristic_step
    CHECK (document_heuristic_step BETWEEN 1 AND 3)
);

CREATE INDEX IF NOT EXISTS idx_subv_nlp_extracted_at    ON l0.subvenciones_nlp (extracted_at);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_validation      ON l0.subvenciones_nlp (validation_status);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_geo_patron      ON l0.subvenciones_nlp (geo_patron);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_regimen_ue      ON l0.subvenciones_nlp (regimen_ue_tipo);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_modalidad_lgs   ON l0.subvenciones_nlp (modalidad_lgs);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_dom_fiscal_nuts ON l0.subvenciones_nlp USING GIN (domicilio_fiscal_nuts);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_ctros_trab_nuts ON l0.subvenciones_nlp USING GIN (centros_trabajo_nuts);
CREATE INDEX IF NOT EXISTS idx_subv_nlp_json            ON l0.subvenciones_nlp USING GIN (nlp_json jsonb_path_ops);

COMMENT ON TABLE l0.subvenciones_nlp IS
  'Cache de análisis NLP v5.0.2 deduplicada por document_key. Persiste el nlp_json '
  '(ficha ejecutiva) y los datos cacheados para reuso en futuras convocatorias que '
  'compartan documento normativo (dedup N:1). NO se lee en feed/filter/matching: '
  'las 25 cols categorizables se propagan a nacional_subvenciones (dual-write Batch B).';
COMMENT ON COLUMN l0.subvenciones_nlp.geo_patron IS
  'sin_restriccion | sin_datos | domicilio_fiscal | centros_trabajo | ejecucion | impacto | '
  'domicilio_fiscal_o_centros_trabajo | domicilio_fiscal_con_ejecucion | '
  'domicilio_fiscal_o_centros_trabajo_con_ejecucion | complejo';
