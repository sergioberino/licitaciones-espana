-- Tabla de municipios españoles con su correspondencia NUTS3 (nivel provincial).
-- Fuente oficial: INE — Relación de municipios y sus códigos por provincias.
-- https://www.ine.es/daco/daco42/codmun/codmunmapa.htm
--
-- Propósito: resolver el requisito geográfico G2 ("centro de trabajo en la región")
-- sin que el servicio consumidor deba implementar un algoritmo INE↔NUTS.
-- El onboarding registra el CodigoINE del municipio; la FK provee el NUTS3
-- directamente, que es el eje de matching contra regiones[] de convocatorias.
--
-- Diseño:
--   · codigo_ine  — código INE de 5 dígitos (PK natural, estable).
--   · municipio   — nombre oficial del municipio según INE.
--   · nuts3       — FK a dim.nuts_spain(geocode), nivel provincial (ES111..ES703).
--                   El NUTS2 y NUTS1 se derivan por truncación cuando se necesitan.
--
-- Datos: ~8.131 municipios. Poblar mediante script de ingesta desde INE.
-- Los datos NO se incluyen en este DDL snapshot; se cargan como migración de datos
-- separada para mantener el fichero de contrato DDL limpio y testeable.

CREATE TABLE IF NOT EXISTS dim.codigos_ine (
    codigo_ine  VARCHAR(5)  PRIMARY KEY,
    municipio   TEXT        NOT NULL,
    nuts3       VARCHAR(10) NOT NULL REFERENCES dim.nuts_spain(geocode)
);

-- Índice sobre nuts3 para queries de matching geográfico:
-- «dame todos los municipios que pertenecen al NUTS3 X»
CREATE INDEX IF NOT EXISTS idx_codigos_ine_nuts3
ON dim.codigos_ine (nuts3);
