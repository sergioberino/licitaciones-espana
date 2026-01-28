# 🇪🇸 Licitaciones y Contratación Pública de España

Dataset completo de contratación pública española con datos de múltiples fuentes:

| Fuente | Registros | Período | Cobertura |
|--------|-----------|---------|-----------|
| **PLACSP** (Nacional) | 8.7M | 2012-2026 | Todas las CCAA |
| **Catalunya** | 17.6M | 2014-2025 | Generalitat + Ayto. Barcelona |

## 📂 Estructura del repositorio

```
licitaciones-espana/
├── nacional/
│   ├── licitaciones_espana.parquet              # 641 MB - Última versión
│   ├── licitaciones_completo_2012_2026.parquet  # 780 MB - Historial completo
│   └── licitaciones.py                          # Script extracción PLACSP
│
├── catalunya/
│   ├── contratacion/
│   │   ├── contratos_registro.parquet           # ⭐ 461 MB - MASTER (3.4M reg)
│   │   ├── publicaciones_pscp.parquet           # 414 MB - Ciclo completo (1.6M)
│   │   ├── adjudicaciones_generalitat.parquet
│   │   ├── contratacion_programada.parquet
│   │   ├── contratos_covid.parquet
│   │   ├── fase_ejecucion.parquet
│   │   ├── resoluciones_tribunal.parquet
│   │   ├── contratistas_bcn.parquet             # Barcelona
│   │   ├── contratos_menores_bcn.parquet
│   │   ├── modificaciones_bcn.parquet
│   │   ├── perfil_contratante_bcn.parquet
│   │   └── resumen_trimestral_bcn.parquet
│   ├── subvenciones/
│   │   ├── raisc_concesiones.parquet            # ⭐ 119 MB - MASTER (9.6M reg)
│   │   ├── raisc_convocatorias.parquet
│   │   └── convocatorias_subvenciones.parquet
│   ├── presupuestos/
│   │   ├── ejecucion_gastos.parquet             # 2014-2025
│   │   ├── ejecucion_ingresos.parquet
│   │   └── presupuestos_aprobados.parquet
│   ├── entidades/
│   │   ├── ens_locals.parquet                   # Todos los entes locales
│   │   ├── sector_publico_generalitat.parquet
│   │   ├── ajuntaments.parquet
│   │   ├── ajuntaments_lista.parquet
│   │   ├── codigos_departamentos.parquet
│   │   └── composicio_plens.parquet
│   ├── convenios/
│   │   └── convenios.parquet
│   ├── rrhh/
│   │   ├── altos_cargos.parquet
│   │   ├── retribuciones_funcionarios.parquet
│   │   ├── retribuciones_laboral.parquet
│   │   ├── taules_retributives.parquet
│   │   ├── convocatorias_personal.parquet
│   │   └── enunciats_examens.parquet
│   ├── territorio/
│   │   ├── municipis_catalunya.parquet
│   │   └── municipis_espanya.parquet
│   └── README.md
│
├── scripts/
│   ├── ccaa_cataluna.py                         # Script descarga CSV
│   └── ccaa_cataluna_parquet.py                 # Script conversión Parquet
│
├── .gitattributes
├── .gitignore
├── requirements.txt
└── README.md
```

## 📊 Resumen de datos

### Nacional (PLACSP)

| Conjunto | Registros | Período |
|----------|-----------|---------|
| Licitaciones | 3.6M | 2012 - actualidad |
| Agregación CCAA | 1.7M | 2016 - actualidad |
| Contratos menores | 3.3M | 2018 - actualidad |
| Encargos medios propios | 14.7K | 2021 - actualidad |
| Consultas preliminares | 3.7K | 2022 - actualidad |
| **Total** | **8.7M** | **2012 - 2026** |

### Catalunya

| Categoría | Registros | Datasets | Tamaño |
|-----------|-----------|----------|--------|
| Contratación | 5.1M | 12 | 900 MB |
| Subvenciones (RAISC) | 9.7M | 3 | 120 MB |
| Presupuestos | 2.5M | 3 | 50 MB |
| Entidades | 150K | 6 | 15 MB |
| Convenios | 45K | 1 | 8 MB |
| RRHH | 50K | 6 | 5 MB |
| Territorio | 10K | 2 | 2 MB |
| **Total** | **17.6M** | **33** | **1.1 GB** |

## 📥 Descarga y uso

```python
import pandas as pd

# === NACIONAL (PLACSP) ===
df_nacional = pd.read_parquet('nacional/licitaciones_espana.parquet')

# === CATALUNYA ===
# Contratos formalizados (dataset principal)
df_cat = pd.read_parquet('catalunya/contratacion/contratos_registro.parquet')

# Subvenciones (9.6M registros)
df_subv = pd.read_parquet('catalunya/subvenciones/raisc_concesiones.parquet')

# Presupuestos Generalitat (2014-2025)
df_pres = pd.read_parquet('catalunya/presupuestos/ejecucion_gastos.parquet')

# Entes locales de Catalunya
df_ents = pd.read_parquet('catalunya/entidades/ens_locals.parquet')

# Cargar solo columnas específicas (más rápido)
df = pd.read_parquet('catalunya/contratacion/contratos_registro.parquet',
                     columns=['Codi_expedient', 'Objecte', 'Import_adjudicacio_sense_IVA'])
```

## 🔧 Regenerar datos

### Nacional (PLACSP)
```bash
cd nacional
python licitaciones.py
```

### Catalunya
```bash
cd scripts

# 1. Descargar CSVs (~12 GB)
python ccaa_cataluna.py

# 2. Convertir a Parquet (~1.1 GB)
python ccaa_cataluna_parquet.py
```

## 📋 Campos principales

### Nacional (PLACSP) - 48 columnas

| Categoría | Campos |
|-----------|--------|
| **Identificación** | id, expediente, objeto, url |
| **Órgano** | organo_contratante, nif_organo, dir3_organo |
| **Tipo** | tipo_contrato, procedimiento, estado |
| **Importes** | importe_sin_iva, importe_con_iva, importe_adjudicacion |
| **Adjudicación** | adjudicatario, nif_adjudicatario, num_ofertas, es_pyme |
| **Clasificación** | cpv_principal, nuts, ubicacion |
| **Fechas** | fecha_publicacion, fecha_limite, fecha_adjudicacion |

### Catalunya - Contratos (~40 columnas)

| Categoría | Campos |
|-----------|--------|
| **Identificación** | Codi_expedient, Objecte, Numero_contracte |
| **Órgano** | Organ_contractacio, Codi_organ, NIF_organ |
| **Tipo** | Tipus_contracte, Procediment, Estat |
| **Importes** | Import_licitacio_sense_IVA, Import_adjudicacio_sense_IVA |
| **Adjudicación** | Adjudicatari, NIF_adjudicatari, Numero_ofertes |
| **Clasificación** | CPV, Codi_NUTS |
| **Fechas** | Data_publicacio, Data_adjudicacio, Data_formalitzacio |

## 📝 Fuentes de datos

### Nacional
- [Plataforma de Contratación del Sector Público (PLACSP)](https://contrataciondelsectorpublico.gob.es/)
- [Datos Abiertos - Ministerio de Hacienda](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx)

### Catalunya
- [Portal Transparència Catalunya](https://analisi.transparenciacatalunya.cat/)
- [Open Data Barcelona](https://opendata-ajuntament.barcelona.cat/)
- [Registre Públic de Contractes](https://contractacio.gencat.cat/)

## 📄 Licencia

Datos públicos - [Licencia de Reutilización](https://datos.gob.es/es/aviso-legal)

## 🔗 Enlaces

- [BQuant Finance](https://bquantfinance.com)
- [Newsletter BQuant Fund Lab](https://bquantfinance.substack.com)

---

⭐ Si te resulta útil, dale una estrella al repo
