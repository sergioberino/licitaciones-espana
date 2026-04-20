CREATE TABLE IF NOT EXISTS l0.nacional_subvenciones (
  id BIGINT PRIMARY KEY,
  -- Órgano (aplanado)
  nivel1 TEXT,
  nivel2 TEXT,
  nivel3 TEXT,
  -- Información básica
  sede_electronica TEXT,
  codigo_bdns TEXT,
  fecha_recepcion DATE,
  -- Instrumentos y tipo
  instrumentos JSONB,
  tipo_convocatoria TEXT,
  presupuesto_total NUMERIC,
  mrr BOOLEAN,
  descripcion TEXT,
  descripcion_leng TEXT,
  -- Beneficiarios y sectores
  tipos_beneficiarios JSONB,
  sectores JSONB,
  regiones JSONB,
  -- Finalidad y bases reguladoras
  descripcion_finalidad TEXT,
  descripcion_bases_reguladoras TEXT,
  url_bases_reguladoras TEXT,
  -- Publicación y estado
  se_publica_diario_oficial BOOLEAN,
  abierto BOOLEAN,
  -- Fechas de solicitud
  fecha_inicio_solicitud DATE,
  fecha_fin_solicitud DATE,
  text_inicio TEXT,
  text_fin TEXT,
  -- Ayuda de estado
  ayuda_estado TEXT,
  url_ayuda_estado TEXT,
  -- Fondos y reglamento
  fondos JSONB,
  reglamento JSONB,
  -- Objetivos y sectores productos
  objetivos JSONB,
  sectores_productos JSONB,
  -- Documentos y anuncios
  documentos JSONB,
  anuncios JSONB
);