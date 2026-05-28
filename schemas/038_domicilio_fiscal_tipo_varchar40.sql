-- 038_domicilio_fiscal_tipo_varchar40.sql (canónico; incremental parent: 034)

ALTER TABLE l0.subvenciones_nlp
  ALTER COLUMN domicilio_fiscal_tipo TYPE VARCHAR(40);

ALTER TABLE l0.nacional_subvenciones
  ALTER COLUMN domicilio_fiscal_tipo TYPE VARCHAR(40);
