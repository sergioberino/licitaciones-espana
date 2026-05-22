-- 012_nacional_subvenciones.sql
-- Shape canónico v3 (Track B WP1): tabla principal + columnas NLP dual-write.
-- nacional_subvenciones = source of truth de feed/filter/matching del observatorio.
-- subvenciones_nlp = cache de análisis (dedup + ficha ejecutiva). Ver design §4.2.

CREATE TABLE IF NOT EXISTS l0.nacional_subvenciones (
  id BIGINT PRIMARY KEY,
  -- Órgano (aplanado)
  nivel1 TEXT,
  nivel2 TEXT,
  nivel3 TEXT,
  -- Información básica
  sede_electronica TEXT,
  fecha_recepcion DATE,
  -- Instrumentos y tipo
  instrumento_id SMALLINT,
  tipo_convocatoria TEXT,
  presupuesto_total NUMERIC,
  mrr BOOLEAN,
  descripcion TEXT,
  descripcion_leng TEXT,
  -- Beneficiarios y sectores
  tipos_beneficiarios SMALLINT[],     -- IDs dim.beneficiarios_subvenciones
  sectores VARCHAR(10)[],             -- CNAE/NACE (dim.cnae_dim.codigo)
  regiones VARCHAR(10)[],             -- NUT (dim.nuts_spain.geocode)
  -- Finalidad y bases reguladoras
  politica_gastos SMALLINT,           -- ID dim.politica_gastos
  descripcion_bases_reguladoras TEXT,
  url_bases_reguladoras TEXT,
  resumen_bases_reguladoras TEXT,
  -- Publicación y estado
  se_publica_diario_oficial BOOLEAN,
  abierto BOOLEAN,
  -- Fechas de solicitud
  fecha_inicio_solicitud DATE,
  fecha_fin_solicitud DATE,
  fecha_inicio_solicitud_texto TEXT,
  fecha_fin_solicitud_texto TEXT,
  -- Ayuda de estado
  ayuda_estado TEXT,
  url_ayuda_estado TEXT,
  -- Fondos y reglamento
  fondos JSONB,
  reglamento TEXT,
  -- Objetivos y sectores productos
  objetivos TEXT,
  sectores_productos JSONB,
  -- Documentos y anuncios
  documentos JSONB,
  anuncios JSONB,

  -- ===========================================================================
  -- Track B WP1 — dual-write NLP + operacional
  -- ===========================================================================

  -- Puente + operacional (2)
  nlp_document_key TEXT,                    -- FK lógico a l0.subvenciones_nlp(document_key)
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Metadata NLP (2)
  nlp_validation_status VARCHAR(20),        -- valid | partial | invalid (informativo, ficha ejecutiva)
  nlp_extracted_at      TIMESTAMPTZ,

  -- 15 dimensiones matching
  modalidad_lgs              VARCHAR(50),
  mecanismo_seleccion        VARCHAR(50),
  puntuacion_minima_acceso   SMALLINT,
  regimen_ue_tipo            VARCHAR(30),
  rgec_articulo              VARCHAR(20),
  tipo_beneficiario_fino     VARCHAR(30),
  texto_beneficiario_fino    VARCHAR(200),
  admite_consorcio           BOOLEAN,
  art13_lgs_verificado       BOOLEAN,
  intensidad_maxima_pct      NUMERIC(5,2),
  importe_maximo_beneficiario NUMERIC(12,2),
  regla_calculo_ayuda        VARCHAR(50),
  modalidad_pago             VARCHAR(30),
  garantia_pago              VARCHAR(30),
  regimen_acumulacion        VARCHAR(40),

  -- 8 geo (NUTS varchar(5)[], INE varchar(6)[])
  domicilio_fiscal_nuts      VARCHAR(5)[],
  domicilio_fiscal_ine       VARCHAR(6)[],
  domicilio_fiscal_tipo      VARCHAR(20),
  centros_trabajo_nuts       VARCHAR(5)[],
  centros_trabajo_ine        VARCHAR(6)[],
  ejecucion_nuts             VARCHAR(5)[],
  ejecucion_ine              VARCHAR(6)[],
  ejecucion_municipios_anexo BOOLEAN,
  impacto_nuts               VARCHAR(5)[],
  geo_patron                 VARCHAR(60),

  -- DNF compacto para geo_patron='complejo'. NULL para el resto de patrones.
  geo_condiciones_suficientes JSONB
);

-- ===========================================================================
-- Índices preexistentes (Track A)
-- ===========================================================================
CREATE INDEX IF NOT EXISTS idx_subvenciones_instrumento
  ON l0.nacional_subvenciones (instrumento_id);
CREATE INDEX IF NOT EXISTS idx_subvenciones_beneficiarios
  ON l0.nacional_subvenciones USING GIN (tipos_beneficiarios);
CREATE INDEX IF NOT EXISTS idx_subvenciones_sectores
  ON l0.nacional_subvenciones USING GIN (sectores);
CREATE INDEX IF NOT EXISTS idx_subvenciones_regiones
  ON l0.nacional_subvenciones USING GIN (regiones);

-- ===========================================================================
-- Track B WP1 — índices feed/filter/matching
-- ===========================================================================
CREATE INDEX IF NOT EXISTS idx_conv_nlp_document_key      ON l0.nacional_subvenciones (nlp_document_key);
CREATE INDEX IF NOT EXISTS idx_conv_ingested_at           ON l0.nacional_subvenciones (ingested_at);
CREATE INDEX IF NOT EXISTS idx_conv_nlp_validation_status ON l0.nacional_subvenciones (nlp_validation_status);
CREATE INDEX IF NOT EXISTS idx_conv_nlp_extracted_at      ON l0.nacional_subvenciones (nlp_extracted_at);
CREATE INDEX IF NOT EXISTS idx_conv_geo_patron            ON l0.nacional_subvenciones (geo_patron);
CREATE INDEX IF NOT EXISTS idx_conv_modalidad_lgs         ON l0.nacional_subvenciones (modalidad_lgs);
CREATE INDEX IF NOT EXISTS idx_conv_mecanismo_seleccion   ON l0.nacional_subvenciones (mecanismo_seleccion);
CREATE INDEX IF NOT EXISTS idx_conv_tipo_benef_fino       ON l0.nacional_subvenciones (tipo_beneficiario_fino);
CREATE INDEX IF NOT EXISTS idx_conv_regimen_ue_tipo       ON l0.nacional_subvenciones (regimen_ue_tipo);
CREATE INDEX IF NOT EXISTS idx_conv_dom_fiscal_nuts       ON l0.nacional_subvenciones USING GIN (domicilio_fiscal_nuts);
CREATE INDEX IF NOT EXISTS idx_conv_centros_trabajo_nuts  ON l0.nacional_subvenciones USING GIN (centros_trabajo_nuts);
CREATE INDEX IF NOT EXISTS idx_conv_ejecucion_nuts        ON l0.nacional_subvenciones USING GIN (ejecucion_nuts);
CREATE INDEX IF NOT EXISTS idx_conv_geo_cond_suf          ON l0.nacional_subvenciones USING GIN (geo_condiciones_suficientes jsonb_path_ops);

COMMENT ON COLUMN l0.nacional_subvenciones.nlp_document_key IS
  'FK lógico a l0.subvenciones_nlp(document_key). NULL = no analizada.';
COMMENT ON COLUMN l0.nacional_subvenciones.ingested_at IS
  'Timestamp de primera ingesta. NO actualizar en ON CONFLICT DO UPDATE.';
COMMENT ON COLUMN l0.nacional_subvenciones.nlp_validation_status IS
  'Metadato informativo (valid|partial|invalid). Sirve a la ficha ejecutiva para señalar '
  'al consultor cuando un análisis requiere revisión. NO es gate operativo del matcher.';
COMMENT ON COLUMN l0.nacional_subvenciones.geo_patron IS
  'sin_restriccion | sin_datos | domicilio_fiscal | centros_trabajo | ejecucion | impacto | '
  'domicilio_fiscal_o_centros_trabajo | domicilio_fiscal_con_ejecucion | '
  'domicilio_fiscal_o_centros_trabajo_con_ejecucion | complejo';
COMMENT ON COLUMN l0.nacional_subvenciones.geo_condiciones_suficientes IS
  'DNF del requisito geográfico cuando geo_patron=complejo. NULL otherwise. '
  'Vocabulario v5.0.3 (descriptivo: domicilio_fiscal/centros_trabajo/ejecucion/impacto).';
