-- Tabla de instrumentos de subvenciones
-- Datos estáticos que no cambiarán

CREATE TABLE IF NOT EXISTS dim.instrumentos_subvenciones (
    id INTEGER PRIMARY KEY,
    descripcion VARCHAR(255) NOT NULL
);

-- Insertar instrumentos
INSERT INTO dim.instrumentos_subvenciones (id, descripcion) VALUES
(1, 'SUBVENCIÓN Y ENTREGA DINERARIA SIN CONTRAPRESTACIÓN'),
(2, 'PRÉSTAMO'),
(4, 'GARANTÍA'),
(5, 'VENTAJA FISCAL'),
(6, 'APORTACIÓN DE FINANCIACIÓN RIESGO'),
(7, 'OTROS INSTRUMENTOS DE AYUDA')
ON CONFLICT DO NOTHING;
