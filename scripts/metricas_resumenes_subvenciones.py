"""
Genera un informe Markdown con métricas de coste de los resúmenes de subvenciones
producidos por LLM, leyendo la tabla ops.llm_resumen_subvenciones_logs.

Salida: MétricasResumenesSubvenciónes-<YYYY-MM-DD>.md en el directorio de trabajo.
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2

# Importar config del proyecto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import get_database_url

# ---------------------------------------------------------------------------
# Precios por millón de tokens (USD)
# ---------------------------------------------------------------------------
MODEL_PRICES: dict[str, dict[str, float]] = {
    "gpt-5.2": {"input": 1.75, "output": 14.00},
    "gpt-5.2-pro": {"input": 21.00, "output": 168.00},
    "gpt-5.1": {"input": 1.25, "output": 10.00},
    "gpt-5": {"input": 1.25, "output": 10.00},
    "gpt-5-mini": {"input": 0.25, "output": 2.00},
    "gpt-5-nano": {"input": 0.05, "output": 0.40},
    "gpt-5-pro": {"input": 15.00, "output": 120.00},
    "gpt-4.1": {"input": 2.00, "output": 8.00},
    "gpt-4.1-mini": {"input": 0.40, "output": 1.60},
    "gpt-4.1-nano": {"input": 0.10, "output": 0.40},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "o4-mini": {"input": 1.10, "output": 4.40},
}

QUERY = """
SELECT model, input_tokens, completion_tokens, processing_time
FROM ops.llm_resumen_subvenciones_logs;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _token_cost(tokens: float, price_per_million: float) -> float:
    return tokens / 1_000_000 * price_per_million


def _fmt_tokens(n: float) -> str:
    return f"{n:,.0f}"


def _fmt_usd(v: float) -> str:
    return f"${v:,.6f}"


def _fmt_ratio(v: float) -> str:
    return f"{v:.4f}"


def _fmt_time(v: float) -> str:
    return f"{v:.3f} s"


def _prices_for(model: str) -> dict[str, float] | None:
    return MODEL_PRICES.get(model)


# ---------------------------------------------------------------------------
# Markdown builders
# ---------------------------------------------------------------------------


def _header_row(*cols: str) -> str:
    return "| " + " | ".join(cols) + " |"


def _separator(n: int) -> str:
    return "| " + " | ".join(["---"] * n) + " |"


def _token_table(
    label_input: str,
    label_output: str,
    input_tokens: float,
    output_tokens: float,
    prices: dict[str, float] | None,
) -> str:
    if prices:
        cost_in = _token_cost(input_tokens, prices["input"])
        cost_out = _token_cost(output_tokens, prices["output"])
        cost_total = cost_in + cost_out
        cost_in_str = _fmt_usd(cost_in)
        cost_out_str = _fmt_usd(cost_out)
        cost_total_str = _fmt_usd(cost_total)
    else:
        cost_in_str = cost_out_str = cost_total_str = "N/A"

    lines = [
        _header_row("Concepto", "Tokens", "Coste (USD)"),
        _separator(3),
        f"| {label_input} | {_fmt_tokens(input_tokens)} | {cost_in_str} |",
        f"| {label_output} | {_fmt_tokens(output_tokens)} | {cost_out_str} |",
        f"| **Total** | {_fmt_tokens(input_tokens + output_tokens)} | **{cost_total_str}** |",
    ]
    return "\n".join(lines)


def _model_section(model: str, df_model: pd.DataFrame) -> str:
    prices = _prices_for(model)
    n = len(df_model)

    # --- totals ---
    tot_input = df_model["input_tokens"].sum()
    tot_output = df_model["completion_tokens"].sum()
    tot_ratio = tot_input / tot_output if tot_output else float("inf")
    tot_time = df_model["processing_time"].sum()

    # --- means ---
    mean_input = df_model["input_tokens"].mean()
    mean_output = df_model["completion_tokens"].mean()
    mean_ratio = df_model["ratio"].mean()
    mean_time = df_model["processing_time"].mean()

    # --- medians ---
    med_input = df_model["input_tokens"].median()
    med_output = df_model["completion_tokens"].median()
    med_ratio = df_model["ratio"].median()
    med_time = df_model["processing_time"].median()

    if prices is None:
        price_note = (
            f"\n> ⚠️ Modelo **{model}** no está en la tabla de precios conocidos. "
            "Los costes se muestran como N/A.\n"
        )
    else:
        price_note = ""

    lines = [
        f"## Modelo: `{model}`",
        "",
        f"**Subvenciones procesadas:** {n:,}",
        price_note,
        "",
        "### Consumo total",
        "",
        _token_table(
            "Tokens de entrada", "Tokens de salida (completions)", tot_input, tot_output, prices
        ),
        "",
        f"**Ratio entrada/salida:** {_fmt_ratio(tot_ratio)}",
        "",
        f"**Tiempo de procesamiento total:** {_fmt_time(float(tot_time))}",
        "",
        "### Valores medios (por subvención)",
        "",
        _token_table(
            "Tokens de entrada", "Tokens de salida (completions)", mean_input, mean_output, prices
        ),
        "",
        f"**Ratio entrada/salida (media):** {_fmt_ratio(mean_ratio)}",
        "",
        f"**Tiempo de procesamiento (media):** {_fmt_time(float(mean_time))}",
        "",
        "### Valores medianos (por subvención)",
        "",
        _token_table(
            "Tokens de entrada", "Tokens de salida (completions)", med_input, med_output, prices
        ),
        "",
        f"**Ratio entrada/salida (mediana):** {_fmt_ratio(med_ratio)}",
        "",
        f"**Tiempo de procesamiento (mediana):** {_fmt_time(float(med_time))}",
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


def _global_summary(df: pd.DataFrame) -> str:
    n = len(df)
    tot_input = df["input_tokens"].sum()
    tot_output = df["completion_tokens"].sum()
    tot_time = df["processing_time"].sum()

    # Coste global: calculado sólo para modelos con precio conocido
    total_cost = 0.0
    cost_coverage = 0
    for model, group in df.groupby("model"):
        prices = _prices_for(model)
        if prices:
            total_cost += _token_cost(group["input_tokens"].sum(), prices["input"])
            total_cost += _token_cost(group["completion_tokens"].sum(), prices["output"])
            cost_coverage += len(group)

    coverage_pct = cost_coverage / n * 100 if n else 0

    lines = [
        "## Resumen global",
        "",
        f"| Métrica | Valor |",
        f"| --- | --- |",
        f"| Subvenciones totales | {n:,} |",
        f"| Modelos distintos | {df['model'].nunique()} |",
        f"| Tokens de entrada totales | {_fmt_tokens(tot_input)} |",
        f"| Tokens de salida totales | {_fmt_tokens(tot_output)} |",
        f"| Tokens totales | {_fmt_tokens(tot_input + tot_output)} |",
        f"| Coste total estimado (USD) | **{_fmt_usd(total_cost)}** |",
        f"| Cobertura de precios | {cost_coverage:,} / {n:,} ({coverage_pct:.1f}%) |",
        f"| Tiempo de procesamiento total | {_fmt_time(float(tot_time))} |",
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    db_url = get_database_url()
    if not db_url:
        print("ERROR: Variables de entorno de base de datos no configuradas.", file=sys.stderr)
        sys.exit(1)

    print("Conectando a la base de datos...", flush=True)
    conn = psycopg2.connect(db_url)
    try:
        df = pd.read_sql_query(QUERY, conn)
    finally:
        conn.close()

    if df.empty:
        print("No hay datos en ops.llm_resumen_subvenciones_logs.")
        sys.exit(0)

    # Tipos correctos
    df["input_tokens"] = df["input_tokens"].astype(float)
    df["completion_tokens"] = df["completion_tokens"].astype(float)
    df["processing_time"] = df["processing_time"].astype(float)
    df["ratio"] = df.apply(
        lambda r: (
            r["input_tokens"] / r["completion_tokens"]
            if r["completion_tokens"] > 0
            else float("inf")
        ),
        axis=1,
    )

    today = date.today().isoformat()
    output_file = Path(f"MétricasResumenesSubvenciónes-{today}.md")

    print(f"Generando informe: {output_file}", flush=True)

    sections = [
        f"# Métricas de Resúmenes de Subvenciones\n",
        f"**Generado:** {today}  \n",
        f"**Fuente:** `ops.llm_resumen_subvenciones_logs`\n",
        "\n---\n\n",
        _global_summary(df),
    ]

    for model in sorted(df["model"].unique()):
        sections.append(_model_section(model, df[df["model"] == model].copy()))

    content = "\n".join(sections)

    output_file.write_text(content, encoding="utf-8")
    print(f"Informe guardado en: {output_file.resolve()}", flush=True)


if __name__ == "__main__":
    main()
