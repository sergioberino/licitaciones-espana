CREATE TABLE IF NOT EXISTS dim.dim_dir3 (
  num_code           VARCHAR(16) PRIMARY KEY,   -- Código DIR3 real
  label              TEXT,
  parent_code        VARCHAR(16),

  nivel_admon        SMALLINT,
  tipo_ent_publica   VARCHAR(8),
  nivel_jerarquico   SMALLINT,
  estado             CHAR(1),
  vig_alta_oficial   DATE,
  nif_cif            VARCHAR(16)
  -- parent_code es referencia lógica al código de la unidad superior; sin FK para evitar orden de inserción.
);
