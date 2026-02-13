-- Provincias (dim). FK a dim.dim_ccaa. Tipos INTEGER sin precisión (PostgreSQL no admite INTEGER(3)).
CREATE TABLE IF NOT EXISTS dim.dim_provincia (
    num_code INTEGER PRIMARY KEY,
    parent_code INTEGER NOT NULL,
    label VARCHAR(255) NOT NULL,
    CONSTRAINT fk_provincia_ccaa FOREIGN KEY (parent_code) REFERENCES dim.dim_ccaa(num_code)
);

INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (4, 1, 'Almeria');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (11, 1, 'Cadiz');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (14, 1, 'Cordoba');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (18, 1, 'Granada');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (21, 1, 'Huelva');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (23, 1, 'Jaen');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (29, 1, 'Malaga');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (41, 1, 'Sevilla');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (22, 2, 'Huesca');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (44, 2, 'Teruel');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (50, 2, 'Zaragoza');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (33, 3, 'Asturias');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (7, 4, 'Baleares');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (35, 5, 'Las Palmas');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (38, 5, 'Santa Cruz De Tenerife');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (39, 6, 'Cantrabria');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (5, 7, 'Avila');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (9, 7, 'Burgos');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (24, 7, 'Leon');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (34, 7, 'Palencia');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (37, 7, 'Salamanca');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (40, 7, 'Segovia');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (42, 7, 'Soria');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (47, 7, 'Valladolid');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (49, 7, 'Zamora');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (2, 8, 'Albacete');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (13, 8, 'Ciudad Real');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (16, 8, 'Cuenca');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (19, 8, 'Guadalajara');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (45, 8, 'Toledo');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (8, 9, 'Barcelona');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (17, 9, 'Girona');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (25, 9, 'Lleida');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (43, 9, 'Tarragona');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (3, 10, 'Alicante');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (12, 10, 'Castellón');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (46, 10, 'Valencia');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (6, 11, 'Badajoz');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (10, 11, 'Caceres');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (15, 12, 'La Coruña');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (27, 12, 'Lugo');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (32, 12, 'Ourense');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (36, 12, 'Pontevedra');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (28, 13, 'Madrid');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (30, 14, 'Murcia');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (31, 15, 'Navarra');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (1, 16, 'Alava');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (20, 16, 'Guipuzcoa');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (48, 16, 'Vizcaya');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (26, 17, 'La Rioja');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (51, 18, 'Ceuta');
INSERT INTO dim.dim_provincia (num_code, parent_code, label) VALUES (52, 19, 'Melilla');
