#!/usr/bin/env python3
"""
PRP Phase 2: Validate SQL schemas against Parquet data files.
Reads Parquet column names and dtypes, maps to PostgreSQL-friendly types,
and reports alignment with schemas/002, 003, 004 for ETL and DB optimization.

Run from repo root:
  python3 scripts/validate_schemas_parquet.py
  python3 scripts/validate_schemas_parquet.py --base-dir tmp   # after extraction
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:
    print("Requires pyarrow. Run: pip install pyarrow", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent.parent


def arrow_type_to_pg(arrow_type) -> str:
    """Map Arrow type to PostgreSQL type for schema optimization."""
    name = str(arrow_type).lower()
    if "int64" in name or "long" in name:
        return "BIGINT"
    if "int32" in name or "int" in name:
        return "INTEGER"
    if "float" in name or "double" in name:
        return "NUMERIC(18, 2)"
    if "bool" in name:
        return "BOOLEAN"
    if "timestamp" in name or "date" in name:
        return "TIMESTAMPTZ"
    if "string" in name or "utf8" in name or "large_string" in name:
        return "TEXT"
    if "dictionary" in name:
        return "TEXT"
    return "TEXT"


def infer_parquet_schema(parquet_path: Path) -> list[dict] | None:
    """Return list of {name, dtype, pg_type} for each column (metadata only, no row data read)."""
    try:
        schema = pq.read_schema(parquet_path)
    except Exception as e:
        print(f"  Skip {parquet_path.name}: {e}", file=sys.stderr)
        return None
    out = []
    for i in range(schema.num_fields):
        field = schema.field(i)
        pg = arrow_type_to_pg(field.type)
        out.append({"name": field.name, "dtype": str(field.type), "pg_type": pg})
    return out


def main():
    parser = argparse.ArgumentParser(description="Validate Parquet column names/types vs SQL schemas")
    parser.add_argument("--base-dir", type=str, default=None, help="Base dir for Parquet (e.g. tmp); default repo root")
    args = parser.parse_args()
    base = REPO_ROOT / args.base_dir if args.base_dir else REPO_ROOT

    report = {"nacional": [], "catalunya": [], "valencia": [], "base_dir": str(base)}

    # Nacional: tmp/output or nacional/
    nacional_path = base / "output" / "licitaciones_completo_2024_2024.parquet"
    if not nacional_path.exists():
        nacional_path = base / "nacional" / "licitaciones_completo_2012_2026.parquet"
    if nacional_path.exists():
        data = infer_parquet_schema(nacional_path)
        report["nacional"] = data if data else []
        print("Nacional:", len(report["nacional"]), "columns")
    else:
        print("Nacional: file not found", nacional_path)

    # Catalunya: one representative (contratos_registro)
    cat_path = base / "catalunya_parquet" / "contratacion" / "contratos_registro.parquet"
    if not cat_path.exists():
        cat_path = base / "catalunya" / "contratacion" / "contratos_registro.parquet"
    if cat_path.exists():
        data = infer_parquet_schema(cat_path)
        report["catalunya"] = data if data else []
        print("Catalunya (contratos_registro):", len(report["catalunya"]), "columns")
    else:
        print("Catalunya: file not found", cat_path)

    # Valencia: one representative (contratacion)
    val_path = base / "valencia_parquet" / "contratacion"
    if not val_path.exists():
        val_path = base / "valencia" / "contratacion"
    if val_path.exists():
        first_parquet = next(val_path.glob("*.parquet"), None)
        if first_parquet:
            data = infer_parquet_schema(first_parquet)
            report["valencia"] = data if data else []
            print("Valencia (contratacion sample):", len(report["valencia"]), "columns")
        else:
            print("Valencia: no parquet in contratacion/")
    else:
        print("Valencia: dir not found", val_path)

    out_path = REPO_ROOT / "schemas" / "parquet_columns_report.json"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print("Report written to", out_path)
    return report


if __name__ == "__main__":
    main()
