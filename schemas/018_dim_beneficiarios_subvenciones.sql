-- Tabla de beneficiarios de subvenciones
-- Datos estáticos que no cambiarán

CREATE TABLE IF NOT EXISTS dim.beneficiarios_subvenciones (
    id INTEGER PRIMARY KEY,
    descripcion VARCHAR(255) NOT NULL
);

-- Insertar beneficiarios
INSERT INTO dim.beneficiarios_subvenciones (id, descripcion) VALUES
(1, 'PERSONAS FÍSICAS QUE NO DESARROLLAN ACTIVIDAD ECONÓMICA'),
(2, 'PERSONAS JURÍDICAS QUE NO DESARROLLAN ACTIVIDAD ECONÓMICA'),
(3, 'PYME Y PERSONAS FÍSICAS QUE DESARROLLAN ACTIVIDAD ECONÓMICA'),
(4, 'GRAN EMPRESA'),
(5, 'SIN INFORMACION ESPECIFICA')
ON CONFLICT DO NOTHING;
