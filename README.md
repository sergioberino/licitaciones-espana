# 🇪🇸 Licitaciones Públicas de España

Dataset completo de todas las licitaciones públicas de España extraídas de la [Plataforma de Contratación del Sector Público (PLACSP)](https://contrataciondelsectorpublico.gob.es/).

## 📊 Datos incluidos

| Conjunto | Registros | Período |
|----------|-----------|---------|
| Licitaciones | 3.6M | 2012 - actualidad |
| Agregación CCAA | 1.7M | 2016 - actualidad |
| Contratos menores | 3.3M | 2018 - actualidad |
| Encargos medios propios | 14.7K | 2021 - actualidad |
| Consultas preliminares (CPM) | 3.7K | 2022 - actualidad |
| **Total** | **8.69M** | **2012 - 2026** |

## 📥 Descarga

```python
import pandas as pd

# Opción 1: Clonar el repo (incluye datos vía Git LFS)
# git clone https://github.com/BquantFinance/licitaciones-espana.git

df = pd.read_parquet('licitaciones_espana.parquet')
```

### Archivos disponibles

| Archivo | Descripción | Tamaño |
|---------|-------------|--------|
| `licitaciones_espana.parquet` | Última versión de cada licitación | 641 MB |
| `licitaciones_completo_2012_2026.parquet` | Historial completo (todas las versiones) | 780 MB |

## 📋 Campos disponibles (48 columnas)

| Categoría | Campos |
|-----------|--------|
| **Identificación** | id, expediente, objeto, url |
| **Órgano contratante** | organo_contratante, nif_organo, dir3_organo, ciudad_organo, dependencia |
| **Tipo** | tipo_contrato, subtipo_code, procedimiento, estado |
| **Importes** | importe_sin_iva, importe_con_iva, importe_adjudicacion, importe_adj_con_iva |
| **Adjudicación** | adjudicatario, nif_adjudicatario, num_ofertas, es_pyme |
| **Clasificación** | cpv_principal, cpvs, ubicacion, nuts |
| **Fechas** | fecha_publicacion, fecha_limite, fecha_adjudicacion, fecha_updated |
| **Otros** | duracion, duracion_unidad, financiacion_ue, urgencia |

## 🚀 Ejemplos de uso

```python
import pandas as pd

df = pd.read_parquet('licitaciones_espana.parquet')

# Top 10 adjudicatarios por importe
top = df.groupby('adjudicatario')['importe_sin_iva'].sum().nlargest(10)

# Licitaciones por año
df.groupby('ano')['id'].count().plot(kind='bar')

# Importe medio por tipo de contrato
df.groupby('tipo_contrato')['importe_sin_iva'].mean()

# Filtrar por CPV (ej: servicios informáticos)
df_it = df[df['cpv_principal'].str.startswith('72', na=False)]

# Cargar solo columnas específicas (más rápido)
df = pd.read_parquet('licitaciones_espana.parquet', 
                     columns=['expediente', 'importe_sin_iva', 'adjudicatario'])
```

## 🔧 Extracción de datos

Los datos se extraen de los [datos abiertos de PLACSP](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx) mediante:

1. **Descarga** de 78 archivos ZIP (~15 GB comprimidos)
2. **Parsing** de archivos ATOM/XML en formato CODICE (estándar europeo)
3. **Detección automática** del tipo de registro:
   - `ContractFolderStatus` → Licitaciones, menores, encargos, agregación
   - `PreliminaryMarketConsultationStatus` → Consultas preliminares (CPM)
4. **Conversión a Parquet** con tipos optimizados (90% reducción vs CSV)

El script `licitaciones.py` procesa todos los conjuntos de datos disponibles.

## 🔄 Actualización

Los datos de PLACSP se actualizan mensualmente. Para regenerar:

1. Descarga los nuevos ZIPs desde [datos abiertos PLACSP](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx)
2. Ejecuta `licitaciones.py`

## 📋 Requisitos

```bash
pip install pandas pyarrow
```

## 📝 Notas

- Para análisis, usar `licitaciones_espana.parquet` (última versión de cada licitación)
- Para historial completo, usar `licitaciones_completo_2012_2026.parquet`
- Columna `tipo_registro`: `LICITACION` o `CPM` (Consulta Preliminar de Mercado)
- Algunas fechas anómalas (1970, 2029) provienen de errores en los datos de origen

## 📄 Licencia

Datos públicos del Gobierno de España - [Licencia de Reutilización](https://datos.gob.es/es/aviso-legal)

## 🔗 Enlaces

- [Plataforma de Contratación del Sector Público](https://contrataciondelsectorpublico.gob.es/)
- [Datos Abiertos - Ministerio de Hacienda](https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/licitaciones_plataforma_contratacion.aspx)
- [BQuant Finance](https://bquantfinance.com)

---

⭐ Si te resulta útil, dale una estrella al repo
