"""
Conversión de CSVs de Valencia a Parquet
Ejecutar después de ccaa_valencia.py

Ejecutar: python ccaa_valencia_parquet.py
Entrada: tmp/valencia_datos/  (o LICITACIONES_TMP_DIR/valencia_datos)
Salida: tmp/valencia_parquet/
"""

import pandas as pd
from pathlib import Path
import os

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


def main():
    print("=" * 60)
    print("CONVERSIÓN CSV → PARQUET - COMUNITAT VALENCIANA")
    print("=" * 60)
    
    if not INPUT_DIR.exists():
        print(f"❌ No existe la carpeta {INPUT_DIR}")
        print("   Ejecuta primero: python ccaa_valencia.py")
        return
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    total_csv = 0
    total_parquet = 0
    total_registros = 0
    
    # Procesar cada categoría
    for category_dir in sorted(INPUT_DIR.iterdir()):
        if not category_dir.is_dir():
            continue
        
        csv_files = list(category_dir.glob("*.csv"))
        if not csv_files:
            continue
        
        print(f"\n📁 {category_dir.name.upper()}")
        print("-" * 40)
        
        # Crear subcarpeta de salida
        output_category = OUTPUT_DIR / category_dir.name
        output_category.mkdir(exist_ok=True)
        
        for csv_file in sorted(csv_files):
            total_csv += 1
            
            # Nombre del parquet
            parquet_name = csv_file.stem.replace(" ", "_") + ".parquet"
            parquet_path = output_category / parquet_name
            
            if parquet_path.exists():
                print(f"  ⏭️ Ya existe: {parquet_name}")
                total_parquet += 1
                continue
            
            if convert_to_parquet(csv_file, parquet_path):
                total_parquet += 1
    
    # Resumen final
    print("\n" + "=" * 60)
    print("✅ CONVERSIÓN COMPLETADA")
    print("=" * 60)
    
    total_size_csv = 0
    total_size_parquet = 0
    
    print("\n📊 RESUMEN POR CATEGORÍA:")
    for category_dir in sorted(OUTPUT_DIR.iterdir()):
        if category_dir.is_dir():
            files = list(category_dir.glob("*.parquet"))
            size = sum(f.stat().st_size for f in files) / (1024 * 1024)
            total_size_parquet += size
            
            # Contar registros
            registros = 0
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    registros += len(df)
                except:
                    pass
            
            total_registros += registros
            print(f"  📁 {category_dir.name}: {len(files)} archivos, {registros:,} registros, {size:.1f} MB")
    
    # CSV original
    for category_dir in sorted(INPUT_DIR.iterdir()):
        if category_dir.is_dir():
            files = list(category_dir.glob("*.csv"))
            total_size_csv += sum(f.stat().st_size for f in files) / (1024 * 1024)
    
    print(f"\n📈 TOTALES:")
    print(f"   Archivos: {total_parquet}")
    print(f"   Registros: {total_registros:,}")
    print(f"   Tamaño CSV: {total_size_csv:.1f} MB")
    print(f"   Tamaño Parquet: {total_size_parquet:.1f} MB")
    print(f"   Reducción: {(1 - total_size_parquet/total_size_csv)*100:.0f}%")


if __name__ == "__main__":
    main()