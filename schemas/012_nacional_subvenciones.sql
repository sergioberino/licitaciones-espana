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
  tipos_beneficiarios SMALLINT[], -- Array de IDs de dim.beneficiarios_subvenciones
  sectores VARCHAR(10)[], -- Array de códigos CNAE/NACE (referencia: dim.cnae_dim.codigo)
  regiones VARCHAR(10)[], -- Array de códigos NUT (dim.nuts_spain.geocode),
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
  reglamento TEXT, -- Solo descripción del reglamento
  -- Objetivos y sectores productos
  objetivos TEXT, -- Solo descripción del primer objetivo
  sectores_productos JSONB,
  -- Documentos y anuncios
  documentos JSONB,
  anuncios JSONB
);

-- Índice para optimizar filtrado por instrumento (campo usado frecuentemente)
CREATE INDEX IF NOT EXISTS idx_subvenciones_instrumento 
ON l0.nacional_subvenciones(instrumento_id);

-- Índices adicionales para arrays (optimización de búsquedas con operadores @>, &&, etc.)
CREATE INDEX IF NOT EXISTS idx_subvenciones_beneficiarios 
ON l0.nacional_subvenciones USING GIN(tipos_beneficiarios);

CREATE INDEX IF NOT EXISTS idx_subvenciones_sectores 
ON l0.nacional_subvenciones USING GIN(sectores);

CREATE INDEX IF NOT EXISTS idx_subvenciones_regiones 
ON l0.nacional_subvenciones USING GIN(regiones);