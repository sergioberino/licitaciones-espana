#!/usr/bin/env python3
"""
TO-DO — No es una implementación estable.

Script previsto para la fase de traducción de .parquet a .sql: validar
esquemas SQL frente a los datos Parquet (columnas y tipos) antes o durante
la generación de DDL/ETL. Hasta que esa fase esté definida, este módulo
queda como recordatorio y no debe usarse en pipelines ni en producción.

Uso previsto (cuando se implemente):
  Rutina en CLI
"""

import sys

if __name__ == "__main__":
    print(
        "validate_schemas_parquet.py: TO-DO. No es una implementación estable; "
        "previsto para la fase parquet → sql. No usar en producción.",
        file=sys.stderr,
    )
    sys.exit(1)
