CREATE TABLE IF NOT EXISTS l0.nacional_subvenciones (
  id INTEGER PRIMARY KEY,
  numero_convocatoria VARCHAR(255) NOT NULL,
  mrr BOOLEAN,
  descripcion TEXT,
  descripcion_leng TEXT,
  fecha_recepcion DATE,
  nivel1 VARCHAR(255),
  nivel2 VARCHAR(255),
  nivel3 VARCHAR(255),
  codigo_invente VARCHAR(255)
);