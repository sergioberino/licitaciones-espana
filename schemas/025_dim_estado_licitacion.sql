-- Tabla dimensional de estados de licitación (lookup estático).
-- id: SMALLSERIAL PK → referenciado como SMALLINT FK en l0.nacional_licitaciones.estado_code.
-- code: código original PLACSP (PUB, PRE, EV, ADJ, RES, ANUL).
-- label: etiqueta legible; sustituye la columna estado (TEXT) que se elimina de l0.

CREATE TABLE IF NOT EXISTS dim.estado_licitacion (
  id    SMALLSERIAL  PRIMARY KEY,
  code  VARCHAR(4)   UNIQUE NOT NULL,
  label TEXT         NOT NULL
);

INSERT INTO dim.estado_licitacion (code, label) VALUES
  ('PUB',  'Publicada'),
  ('PRE',  'Anuncio Previo'),
  ('EV',   'En evaluación'),
  ('ADJ',  'Adjudicada'),
  ('RES',  'Resuelta'),
  ('ANUL', 'Anulada')
ON CONFLICT (code) DO NOTHING;
