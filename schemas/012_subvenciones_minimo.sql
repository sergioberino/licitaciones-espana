CREATE TABLE IF NOT EXISTS l0.nacional_subvenciones_minimo (
  id INTEGER PRIMARY KEY,
  numeroConvocatoria VARCHAR(255) NOT NULL,
  mrr BOOLEAN,
  descripcion TEXT,
  descripcionLeng TEXT,
  fechaRecepcion DATE,
  nivel1 VARCHAR(255),
  nivel2 VARCHAR(255),
  nivel3 VARCHAR(255),
  codigoINVENTE VARCHAR(255)
);