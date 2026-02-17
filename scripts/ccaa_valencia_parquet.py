"""
Conversión de CSVs de Valencia a Parquet.
Un único datos.parquet por categoría (concatenando todos los CSVs de esa categoría).

Uso: python ccaa_valencia_parquet.py [--categories contratacion,paro]
Entrada: tmp/valencia_datos/  (o LICITACIONES_TMP_DIR/valencia_datos)
Salida: tmp/valencia_parquet/<categoria>/datos.parquet
"""

import argparse
import os
from pathlib import Path

import pandas as pd

# === CONFIGURACIÓN (P2: under tmp/; LICITACIONES_TMP_DIR or repo tmp) ===
_repo_root = Path(__file__).resolve().parent.parent
_tmp_base = Path(os.environ.get("LICITACIONES_TMP_DIR", _repo_root / "tmp"))
INPUT_DIR = _tmp_base / "valencia_datos"
OUTPUT_DIR = _tmp_base / "valencia_parquet"

# Encodings a probar
ENCODINGS = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

# Separadores a probar
SEPARATORS = [';', ',', '\t']


def detect_encoding_and_sep(filepath: Path) -> tuple:
    """Detecta encoding y separador de un CSV."""
    for encoding in ENCODINGS:
        for sep in SEPARATORS:
            try:
                df = pd.read_csv(filepath, encoding=encoding, sep=sep, nrows=5)
                if len(df.columns) > 1:
                    return encoding, sep
            except:
                continue
    return 'utf-8', ';'


def convert_to_parquet(csv_path: Path, parquet_path: Path) -> bool:
    """Convierte un CSV a Parquet."""
    try:
        encoding, sep = detect_encoding_and_sep(csv_path)
        
        # Leer CSV
        df = pd.read_csv(
            csv_path,
            encoding=encoding,
            sep=sep,
            low_memory=False,
            on_bad_lines='skip'
        )
        
        # Convertir columnas object a string para evitar errores
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
        
        # Guardar como Parquet
        df.to_parquet(parquet_path, index=False, compression='snappy')
        
        # Estadísticas
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        reduction = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
        
        print(f"  ✅ {parquet_path.name}")
        print(f"     {len(df):,} registros | {csv_size:.1f} MB → {parquet_size:.1f} MB ({reduction:.0f}% reducción)")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error en {csv_path.name}: {e}")
        return False


def convert_category_to_single_parquet(category_dir: Path, output_category: Path, parquet_path: Path) -> tuple[int, bool]:
    """Concatena todos los CSVs de una categoría y escribe un único datos.parquet. Devuelve (registros, ok)."""
    csv_files = sorted(category_dir.glob("*.csv"))
    if not csv_files:
        return 0, False
    dfs = []
    for csv_file in csv_files:
        try:
            enc, sep = detect_encoding_and_sep(csv_file)
            df = pd.read_csv(csv_file, encoding=enc, sep=sep, low_memory=False, on_bad_lines="skip")
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠️ Omitido {csv_file.name}: {e}")
    if not dfs:
        return 0, False
    combined = pd.concat(dfs, ignore_index=True)
    # Normalizar columnas para Parquet: tipos mixtos (object con float/str) provocan ArrowTypeError
    for col in combined.columns:
        if combined[col].dtype == "object" or combined[col].dtype.name == "string":
            combined[col] = combined[col].astype(str).replace("nan", "").replace("<NA>", "")
    output_category.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(parquet_path, index=False, compression="snappy")
    return len(combined), True


def main():
    parser = argparse.ArgumentParser(description="Valencia: CSV a Parquet (un datos.parquet por categoría)")
    parser.add_argument("--categories", type=str, default=None,
                        help="Categorías a procesar (coma-separadas); si no se indica, todas.")
    args = parser.parse_args()
    categories_filter = [c.strip() for c in args.categories.split(",")] if args.categories else None

    print("=" * 60)
    print("CONVERSIÓN CSV → PARQUET - COMUNITAT VALENCIANA")
    print("=" * 60)

    if not INPUT_DIR.exists():
        print(f"❌ No existe la carpeta {INPUT_DIR}")
        print("   Ejecuta primero: python ccaa_valencia.py")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_registros = 0
    datos_parquet = "datos.parquet"

    for category_dir in sorted(INPUT_DIR.iterdir()):
        if not category_dir.is_dir():
            continue
        if categories_filter and category_dir.name not in categories_filter:
            continue

        output_category = OUTPUT_DIR / category_dir.name
        parquet_path = output_category / datos_parquet
        print(f"\n📁 {category_dir.name.upper()}")
        n, ok = convert_category_to_single_parquet(category_dir, output_category, parquet_path)
        if ok:
            total_registros += n
            print(f"  ✅ {datos_parquet}: {n:,} registros")

    print("\n" + "=" * 60)
    print("✅ CONVERSIÓN COMPLETADA")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())