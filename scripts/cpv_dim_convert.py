#!/usr/bin/env python3
"""
Read schemas/cpv_code.sql (MariaDB export with Code + control digit) and produce
schemas/001b_dim_cpv.sql: dim.cpv_dim DDL + INSERT with num_code (8-digit, control
digit stripped), code (original VARCHAR), label (TEXT). Aligns with .llm/status-and-etl-next-steps.md.
"""
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INPUT_SQL = REPO_ROOT / "schemas" / "cpv_code.sql"
OUTPUT_SQL = REPO_ROOT / "schemas" / "001b_dim_cpv.sql"

# Match one VALUES row: (id, 'code', 'label') with possible '' inside strings
ROW_RE = re.compile(
    r"\(\s*(\d+)\s*,\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*\)"
)


def unescape(s: str) -> str:
    return s.replace("''", "'")


def strip_control_digit(code: str) -> int | None:
    """From '03000000-1' return 3000000 (8-digit integer). Return None if not valid CPV."""
    if "-" in code:
        prefix = code.split("-")[0].strip()
        if len(prefix) == 8 and prefix.isdigit():
            return int(prefix)
    return None


def main() -> int:
    content = INPUT_SQL.read_text(encoding="utf-8", errors="replace")
    rows: list[tuple[int, str, str]] = []

    for m in ROW_RE.finditer(content):
        _id, code_raw, label_raw = m.groups()
        code = unescape(code_raw)
        label = unescape(label_raw)
        num_code = strip_control_digit(code)
        if num_code is None:
            continue  # skip header/junk rows (e.g. '', 'Código', 'Epígrafe')
        rows.append((num_code, code, label))

    # Dedupe by num_code (first wins, in case of duplicates)
    seen: set[int] = set()
    unique: list[tuple[int, str, str]] = []
    for num_code, code, label in rows:
        if num_code in seen:
            continue
        seen.add(num_code)
        unique.append((num_code, code, label))

    def escape_sql(s: str) -> str:
        return s.replace("'", "''")

    ddl = """-- dim.cpv_dim: CPV dimension (source Code with control digit + num_code 8-digit for Parquet/L0).
-- Generated from schemas/cpv_code.sql by scripts/cpv_dim_convert.py (control digit stripped).
-- Apply after 001_reference.sql; domain tables (002-005) reference dim.cpv_dim(num_code).

CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.cpv_dim (
  num_code  INTEGER PRIMARY KEY,
  code      VARCHAR(16) NOT NULL,
  label     TEXT,
  UNIQUE (code)
);

"""

    lines = [ddl]
    lines.append("INSERT INTO dim.cpv_dim (num_code, code, label) VALUES\n")
    values = [
        f"  ({num_code}, '{escape_sql(code)}', '{escape_sql(label)}')"
        for num_code, code, label in unique
    ]
    lines.append(",\n".join(values))
    lines.append("\n;\n")

    OUTPUT_SQL.write_text("".join(lines), encoding="utf-8")
    print(f"Wrote {OUTPUT_SQL} ({len(unique)} rows)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
