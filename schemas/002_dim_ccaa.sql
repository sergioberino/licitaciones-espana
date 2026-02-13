-- Comunidades Autónomas (dim). Tipos INTEGER sin precisión (PostgreSQL no admite INTEGER(2)).
CREATE TABLE IF NOT EXISTS dim.dim_ccaa (
    num_code INTEGER PRIMARY KEY,
    label VARCHAR(255) NOT NULL
);

INSERT INTO dim.dim_ccaa (num_code, label) VALUES (1, 'Andalucía');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (2, 'Aragón');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (3, 'Principado de Asturias');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (4, 'Islas Baleares');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (5, 'Islas Canarias');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (6, 'Cantabria');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (7, 'Castilla y León');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (8, 'Castilla-La Mancha');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (9, 'Cataluña');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (10, 'Comunidad Valenciana');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (11, 'Extremadura');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (12, 'Galicia');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (13, 'Madrid');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (14, 'Región de Murcia');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (15, 'Comunidad Foral de Navarra');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (16, 'País Vasco');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (17, 'La Rioja');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (18, 'Ceuta ');
INSERT INTO dim.dim_ccaa (num_code, label) VALUES (19, 'Melilla');
