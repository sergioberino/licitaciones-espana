# Changelog

Todos los cambios notables del CLI y del microservicio ETL se documentan aquí.

## [2.1.2] — 2026-05-21 (LTS)

### Resumen

- **PATCH sobre `v2.1.1`** — alineación SemVer con el tag chain existente (`v2.1.0` → `v2.1.1` → `v2.1.2`). Mantiene la cadencia *rolling* (un issue cerrado → un incremento PATCH) descrita en `docs/policies/rolling-updates-repo-sync.md` §5.1. Cierra [#63](https://github.com/sergioberino/licitaciones-espana/issues/63).

### Modificado

- **`etl/ingest_l0.py` — `load_parquet_to_l0` — UPSERT incremental para licitaciones nacionales:** el UPSERT para tablas nacionales (`nacional_licitaciones`, `nacional_contratos_menores`, `nacional_encargos_medios_propios`, `nacional_consultas_preliminares`, `nacional_agregacion_ccaa`) cambia de `ON CONFLICT (natural_id) DO NOTHING` a `ON CONFLICT ("expediente", "dir3_organo") DO UPDATE SET` con `COALESCE` en cada columna salvo el par de conflicto. De este modo, campos como `estado_code`, `importe_adjudicacion`, `adjudicatario`, `fecha_adjudicacion` o `procedimiento` se actualizan cuando llegan entries posteriores del mismo expediente, sin machacar datos ya informados. Coherente con el comportamiento ya existente en `l0.lotes_licitaciones`.
- **`etl/ingest_l0.py` — `ensure_l0_table` — índice único `(expediente, dir3_organo)`:** añade `CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_exp_dir3 ON ... ("expediente", "dir3_organo")` para tablas nacionales con CPV. Este índice es el que PostgreSQL necesita para resolver el `ON CONFLICT` sobre el par de negocio estable (el mismo utilizado en `l0.lotes_licitaciones`).
- **`etl/ingest_l0.py` — `load_parquet_to_l0` — conteo exacto de nuevos vs. actualizados:** sustituye `cur.rowcount` por `COUNT(*)` antes/después del bucle de batches para obtener la cifra real de filas _nuevas_ (diferencia neta) frente a _actualizadas_. Idéntico al patrón ya implementado en `load_lotes_parquets_to_l0` en `[2.0.7]`. El conteo tarda unos segundos extra pero la ingesta se ejecuta en horario nocturno (2 am) y no impacta al usuario.
- **`etl/ingest_l0.py` — `ensure_l0_table` y `load_parquet_to_l0` — eliminación de `natural_id` en tablas nacionales:** la columna `natural_id` se elimina por completo de las tablas nacionales (`nacional_licitaciones`, etc.) ya que carece de utilidad al haber pasado la clave de negocio a `(expediente, dir3_organo)`. `ensure_l0_table` ya no la incluye en el `CREATE TABLE` y ejecuta `ALTER TABLE ... DROP COLUMN IF EXISTS "natural_id"` para tablas ya existentes en BD (idempotente). `load_parquet_to_l0` omite la extracción del ID numérico de la URL y no lo incluye en el `INSERT`.

---

## [2.0.7] — 2026-05-20

### Añadido

- **`l0.lotes_licitaciones` — tabla de lotes de licitaciones PLACSP** (`schemas/026_lotes_licitaciones.sql`): nueva tabla en el schema `l0` para almacenar los lotes definidos en los feeds ATOM de la Plataforma de Contratación del Sector Público. Identidad: `UNIQUE (expediente, dir3_organo, num_lote)` — par estable a través de múltiples entries/eventos del mismo expediente (no se referencia `natural_id` ni `l0_id` de `nacional_licitaciones`). Campos de definición del lote (`objeto`, `importe_sin_iva`, `importe_con_iva`, `cpv`, `ubicacion`, `nut`) y de adjudicación (`fecha_adjudicacion`, `nif_adjudicado`, `empresa_adjudicada`, `importe_sin_iva_adj`, `importe_con_iva_adj`). Índices en `expediente`, `dir3_organo` y `nif_adjudicado`. Migración registrada en `INIT_MIGRATIONS`.
- **`nacional/licitaciones.py` — extracción de lotes:** nueva función `parsear_lotes_entry(entry, expediente, dir3_organo)` que combina dos fuentes independientes presentes en el mismo entry:
  - **Fuente A** (`cac:ProcurementProjectLot`): definición del lote (objeto, importes, CPVs unidos con `";"`, ubicación/NUT).
  - **Fuente B** (`cac:TenderResult` con `ProcurementProjectLotID`): adjudicación del lote (fecha, NIF y nombre de adjudicatario, importes adjudicados). Ausencia de `WinningParty` (lote desierto) deja los campos a `NULL`.
  - Ambas fuentes se combinan por `num_lote`; si en un entry solo aparece una, los campos de la otra quedan a `NULL` para ser rellenados cuando llegue el entry correspondiente.
- **`etl/ingest_l0.py` — `load_lotes_parquets_to_l0`:** nueva función que busca los parquets `_part_{script_conjunto}_*_lotes.parquet` en el directorio de salida y los carga en `l0.lotes_licitaciones` mediante un UPSERT incremental con `COALESCE`: cada campo se actualiza solo si el valor entrante no es `NULL`, preservando datos ya informados ante entries posteriores (adjudicación, formalización). Constantes `LOTES_PARQUET_COLUMNS` y `LOTES_TABLE` añadidas.

### Modificado

- **`nacional/licitaciones.py` — `parsear_entry`:** antes de leer `cac:TenderResult`, detecta si el entry contiene `cac:ProcurementProjectLot`; si es así, fija `importe_adjudicacion = None` en la licitación principal (los importes de adjudicación se gestionan en `lotes_licitaciones`).
- **`nacional/licitaciones.py` — `procesar_archivo_atom` y `procesar_zip`:** ahora retornan la tupla `(licitaciones, lotes)`. El bucle principal de `main()` vuelca los lotes a parquets parciales separados (`_part_{conjunto_id}_{idx:04d}_lotes.parquet`) que el ingest recoge en el Paso 2.
- **`etl/cli.py` — `cmd_ingest`:** tras cargar los parquets principales del conjunto `nacional` (excepto `subvenciones`), ejecuta `load_lotes_parquets_to_l0` como Paso 2. Errores en la carga de lotes son no bloqueantes (se loguean pero no abortan el ingest). `load_lotes_parquets_to_l0` añadido a la importación desde `etl.ingest_l0`.
- **`l0.nacional_licitaciones.natural_id`: de `TEXT` a `BIGINT`:** la columna almacenaba la URL completa del feed PLACSP (p. ej. `https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/16577694`); ahora almacena únicamente el identificador numérico final (`16577694`). El constraint `UNIQUE NOT NULL` y el mecanismo de idempotencia `ON CONFLICT (natural_id) DO NOTHING` se mantienen sin cambios. Mismo cambio aplicado a `nacional_agregacion_ccaa`, `nacional_contratos_menores`, `nacional_encargos_medios_propios` y `nacional_consultas_preliminares`.
- **`etl/ingest_l0.py`:** `ensure_l0_table` y `load_parquet_to_l0` aceptan el parámetro `natural_id_type` (`"TEXT"` por defecto). Para el conjunto `nacional`, se pasa `"BIGINT"`: durante la ingesta se extrae el sufijo numérico de la URL con `re.search(r'/(\d+)$', ...)` y se inserta como entero.
- **`etl/cli.py`:** `cmd_ingest` lee `natural_id_type` del `CONJUNTOS_REGISTRY` y lo propaga a `load_parquet_to_l0` (tanto en la ruta secuencial como en la ThreadPool).
- **`CONJUNTOS_REGISTRY["nacional"]`:** añadido `"natural_id_type": "BIGINT"`.

---

## [2.0.6] — 2026-05-15

### Corregido

- **L0 nacional — `PROCEDIMIENTOS`:** alineación completa con `TenderingProcessCode-2.08.gc` (PLACSP/CODICE): códigos **6–8** y **100** corregidos; añadidos **9–13** (p. ej. **9** = «Abierto simplificado»). Sin cambios en `schemas/` — no afecta al gate `ddl-diff-on-bump`.

---

## [2.0.5] — 2026-05-12

### Añadido

- **`dim.estado_licitacion` (lookup de estados PLACSP)** ([#56](https://github.com/sergioberino/licitaciones-espana/issues/56), PR [#57](https://github.com/sergioberino/licitaciones-espana/pull/57)): tabla dimensional con `id SMALLSERIAL`, `code` y `label`; seed de los estados `PUB`, `PRE`, `EV`, `ADJ`, `RES` y `ANUL`.

### Modificado

- **`l0.nacional_licitaciones.estado_code`**: de `TEXT` a `SMALLINT` FK a `dim.estado_licitacion.id`; se elimina la columna `estado` redundante.
- **`etl/ingest_l0.py`**: resuelve `estado_code` contra el lookup dimensional antes de insertar en tablas L0 nacionales.
- **`schemas/004_nuts_spain.sql`**: corrección de sintaxis en el `INSERT` de islas (coma faltante).

---

## [2.0.4] — 2026-05-12

### Añadido

- **`dim.municipios` con códigos postales** ([#54](https://github.com/sergioberino/licitaciones-espana/issues/54), PR [#55](https://github.com/sergioberino/licitaciones-espana/pull/55)): sustituye `dim.codigos_ine` por `dim.municipios` con `codigos_postales VARCHAR(5)[]`, FK a `dim.nuts_spain` y seed de 8.132 municipios INE vigentes a 1-enero-2026.
- **Jerarquía CNAE en `dim.cnae_dim`** ([#52](https://github.com/sergioberino/licitaciones-espana/issues/52), PR [#53](https://github.com/sergioberino/licitaciones-espana/pull/53)): `id SERIAL`, `parent_id` y `code` único; el ingest resuelve la jerarquía oficial CNAE-2025 en memoria.
- **Índice `estado_code` en `l0.nacional_licitaciones`** ([#50](https://github.com/sergioberino/licitaciones-espana/issues/50), PR [#51](https://github.com/sergioberino/licitaciones-espana/pull/51)): acelera filtros por estado en consultas L0 nacionales.

---

## [2.0.3] — 2026-05-05

### Interno

- Verificación del flujo `ddl-diff-on-bump` end-to-end en dirección forward (CI trigger test). Sin cambios funcionales.

---

## [2.0.2] — 2026-05-05

### Añadido

- **`dim.codigos_ine`** ([#44](https://github.com/sergioberino/licitaciones-espana/issues/44), PR [#43](https://github.com/sergioberino/licitaciones-espana/pull/43)): tabla dimensional de referencia que mapea los 8.132 municipios españoles (código INE 5 dígitos) al NUTS3 provincial correspondiente, con FK activa a `dim.nuts_spain(geocode)`. Resuelve el requisito geográfico G2 («centro de trabajo en la región») sin necesidad de implementar un algoritmo INE↔NUTS en el consumidor. Fuente: INE diccionario26.xlsx, vigente a 1-enero-2026.

---

## [2.0.1] — 2026-05-04

### Corregido

- **Geocodes NUTS en `dim.organos_gestores_ccaa`** ([#38](https://github.com/sergioberino/licitaciones-espana/issues/38), PR [#39](https://github.com/sergioberino/licitaciones-espana/pull/39)): los códigos NUTS estaban asignados por orden de aparición en el JSON en lugar de por nombre de CCAA. Corregido en `schemas/014_dim_ccaa_organos.sql`. Incorporado en v2.0.0; esta release formaliza el cierre de la issue.
- **BDNS — texto de fechas de solicitud** ([#41](https://github.com/sergioberino/licitaciones-espana/issues/41), PR [#42](https://github.com/sergioberino/licitaciones-espana/pull/42)): columnas `fecha_inicio_solicitud_texto` / `fecha_fin_solicitud_texto` en `l0.nacional_subvenciones`. Incorporado en v2.0.0; esta release formaliza el cierre de la issue.

---

## [2.0.0] — 2026-04-30

### Breaking changes

- **`schemas/` reframed como contratos DDL snapshot:** eliminados ficheros legacy (`005_catalunya`, `006_valencia`, `007_views`) y ALTERs incrementales (`009_scheduler_runs_pid`, `011_nacional_new_columns`, `022_nacional_procedimiento_code_integer`). Los ALTERs se reubican como migraciones incrementales en el repositorio consumidor (`services/postgres/migrations/etl/`). `008_scheduler.sql` consolidado con `process_id`.
- **`schema_check` desacoplado del runtime HTTP:** el startup es log-only (sin guardar estado para decisiones de salud). `/health` reporta `schema_check_delegated: true`. `/migrations` y `/init-db` añaden aviso de deprecación — la auditoría vinculante reside en `ops.migrator_history` del consumidor.
- **Consumidores deben adoptar el job `migrator`** para tracking y ejecución de migraciones. Ver [ci-upgrade-architecture.md](https://github.com/sergioberino/licitaciones-espana/blob/main/schemas/README.md) en el README de schemas.

### Añadido

- **`GET /ddl`** — endpoint read-only que expone los contratos DDL de `schemas/` (filename, checksum SHA-256, size) para verificación por consumidores. `GET /ddl/{filename}` devuelve el SQL completo como `text/plain`.
- **Documentación:** `schemas/README.md` reescrito como guía de contratos DDL snapshot con sección "Migrator (v2.0.0+)".

### Incluye v1.5.5

- **`014_dim_ccaa_organos.sql` corregido:** geocodes NUTS reasignados correctamente por nombre de CCAA.
- **BDNS fecha_texto:** columnas `fecha_inicio_solicitud_texto` / `fecha_fin_solicitud_texto` en `l0.nacional_subvenciones` ([#39](https://github.com/sergioberino/licitaciones-espana/pull/39)).

---

## [1.5.5] — 2026-04-29

### Corregido

- **`014_dim_ccaa_organos.sql` — geocodes de CCAA incorrectos:** el fichero SQL tenía los códigos NUTS asignados por orden de aparición en el JSON en lugar de por nombre, mezclando las comunidades autónomas (p. ej. Asturias aparecía con `ES63` —Ceuta— y Canarias con `ES12` —Asturias—). Se volvió a generar el .sql con los código nuts correctos.

### Añadido

- **BDNS — texto de fechas de solicitud** ([#39](https://github.com/sergioberino/licitaciones-espana/pull/39)): nuevas columnas `fecha_inicio_solicitud_texto` y `fecha_fin_solicitud_texto` (`TEXT`) en `l0.nacional_subvenciones`, mapeando `textInicio` y `textFin` de la API. Cubren los casos frecuentes en que `fechaInicioSolicitud` / `fechaFinSolicitud` son `null` pero el plazo está descrito en texto libre. Actualizado el scraper histórico, el diario y `SUBVENCIONES_PARQUET_COLUMNS`.

---

## [1.5.4] — 2026-04-28

### Cambio mayor (subvenciones)

- **BDNS — `l0.nacional_subvenciones` normalizado** ([#35](https://github.com/sergioberino/licitaciones-espana/pull/35)): JSONB anidados sustituidos por estructuras tipadas (`instrumento_id` FK, arrays `tipos_beneficiarios`, `sectores` CNAE, `regiones` NUT; `reglamento` / `objetivos` en TEXT; índices GIN en arrays). Ingesta en dos fases (`/busqueda` + `/convocatorias` por ID), rate limiting adaptativo global, paralelismo en scraping y carga de parquets, consulta incremental optimizada en el flujo diario. **Cambio incompatible:** hace falta truncar/recrear la tabla y **reingestar** (pasos detallados en la PR).

### Añadido

- **Observabilidad LLM para resúmenes de subvenciones** ([#37](https://github.com/sergioberino/licitaciones-espana/pull/37)): migración **`023_llm_resumen_logs.sql`** — tabla **`ops.llm_resumen_subvenciones_logs`** (tokens de entrada y salida, modelo, tiempo de procesamiento; PK alineada con `l0.nacional_subvenciones`). Script **`scripts/metricas_resumenes_subvenciones.py`** que genera un informe Markdown de costes y métricas por modelo.

### Mejorado

- **`politica_gastos`:** lookup de `descripcionFinalidad` frente a **`dim.politica_gastos`** de forma case-insensitive (clave normalizada a mayúsculas, coherente con el catálogo dimensional).

---

## [1.5.3] — 2026-04-27

### Corregido

- **L0 nacional — procedimiento de contratación (PLACSP / CODICE):** el diccionario de códigos `cbc:ProcedureCode` estaba desalineado respecto al códice vigente (p. ej. **3** = «Negociado sin publicidad», **4** = «Negociado con publicidad»). Se añaden los códigos **7** (Concurso de proyectos) y **8** (Contrato menor). Se mantienen **100** y **999** hasta una auditoría posterior.
- **`procedimiento_code` en base de datos:** tipo **INTEGER** en el esquema L0 de tablas `nacional_*` (excepto `nacional_subvenciones`). Migración **`022_nacional_procedimiento_code_integer.sql`**: convierte columnas existentes de texto a entero de forma segura y **actualiza la columna `procedimiento`** según el mapeo corregido (corrige filas ya insertadas sin depender solo de re-ingesta).

### Mejorado

- **`init-db`:** antes de aplicar cada migración SQL se fija el parámetro de sesión `etl.db_schema` desde **`DB_SCHEMA`**, de modo que los `DO` bloques que iteran tablas `nacional_%` actúan sobre el esquema de trabajo configurado (p. ej. `raw` o `l0`).

---

## [1.5.2] — 2026-04-23

### Corregido

- **Ingesta CNAE — secciones excluidas:** el filtro `isdigit()` en `_filter_numeric_codes` descartaba todos los códigos de sección (letras `A`–`U`), cargando únicamente divisiones, grupos y clases numéricas. Ahora se ingestian todos los niveles jerárquicos de la clasificación (secciones, divisiones, grupos y clases). Función renombrada a `_extract_codes` para reflejar el comportamiento real.

---

## [1.5.1] — 2026-04-17

### Resumen

- **Observabilidad del scheduler:** registro estructurado de líneas (`scheduler.log`), tabla `scheduler.incidents`, API de consulta de log e incidentes, latido en fichero (`scheduler.heartbeat`).
- **Recuperación operativa:** al arranque del API se detectan reinicios bruscos (ejecuciones «running» sin proceso, PID huérfano), se registran incidentes en español y se intenta el reinicio automático del daemon del scheduler en segundo plano.
- **Diagnóstico en incidentes:** contexto de contenedores vía API Docker sobre socket Unix (sin binario `docker` en la imagen), con soporte de respuestas HTTP chunked; montaje opcional del socket en `docker-compose` del servicio (solo lectura; evaluar riesgo de seguridad).

### Añadido

- **Migración `021_scheduler_incidents.sql`** (y registro en `INIT_MIGRATIONS`): persistencia de incidentes del supervisor.
- **`etl/log_supervisor.py`:** supervisor de log con recorte, escritura de incidentes en BD y latido.
- **API:** endpoints para leer trazas del scheduler y listar / resolver incidentes (según implementación publicada).
- **`etl/docker_incident_context.py`:** captura de listado de contenedores para adjuntar al detalle de incidentes de arranque.

### Mejorado

- **`POST /scheduler/run` (tarea única):** la petición no bloquea hasta el fin de la ingesta; se lanza el proceso en segundo plano (evita cortes por tiempo de espera en proxies o navegador).
- **Subprocesos de ingesta y tareas del bucle del scheduler:** `PYTHONUNBUFFERED=1` y `flush` en progreso por stderr para que la salida aparezca en tiempo casi real en ficheros de log.
- **Bucle del scheduler:** la línea `Completed` del log puede incluir **insertadas** / **omitidas** leídas de `scheduler.runs` tras el subproceso; incidente `heartbeat_gap` solo si el tick fue anormalmente largo **sin** tareas debidas (evita falsos positivos durante ingestas largas).

### Corregido

- **Ingesta nacional / licitaciones:** se evita reutilizar un ZIP en caché de descarga incompleta o obsoleto (se fuerza re-descarga cuando corresponde).

---

## [1.5.0] — 2026-04-17

### Añadido

- **Tablas dimensionales de órganos gestores:** 4 nuevas tablas en el esquema `dim` para entidades administrativas de nivel 3 — comunidades autónomas (`dim.organos_gestores_ccaa`), ministerios (`dim.ministerios_organos`), provincias (`dim.provincias_organos`) y otros órganos (`dim.otros_organos`). Datos estáticos pre-cargados vía migraciones `014`–`017`.
- **Tablas dimensionales de subvenciones:** 3 nuevas tablas — beneficiarios (`dim.beneficiarios_subvenciones`), instrumentos (`dim.instrumentos_subvenciones`) y política de gastos (`dim.politica_gastos`). Migraciones `018`–`020`.

### Corregido

- **`INIT_MIGRATIONS` — nombre de fichero 014 desalineado:** `cli.py` referenciaba `014_dim_organos_gestores_ccaa.sql` pero el fichero real es `014_dim_ccaa_organos.sql`, lo que impedía que la migración se aplicase durante `init-db`.

---

## [1.4.1] — 2026-04-08

### Mejorado

- **`GET /health`**: devuelve HTTP 503 con `"status": "degraded"` cuando la base de datos no es accesible (antes siempre 200 + `"status": "ok"`). Permite que los health probes de Docker detecten correctamente un servicio degradado.

### Corregido

- **`POST /scheduler/unregister` — eliminación de tareas independiente de `enabled`**: el endpoint usaba `get_task_id()` que filtra por `AND enabled = true`, impidiendo borrar tareas visibles en la UI con `enabled = false` (0 deleted). Ahora ejecuta `DELETE FROM scheduler.tasks WHERE conjunto = %s AND subconjunto = %s` directamente.

### Añadido

- **Registro de ejecuciones del scheduler para `cmd_borme` (ingest)**: `cmd_borme` era el único comando CLI que no registraba runs en `scheduler.runs`. Sin run registrada, `last_finished_at` nunca se actualizaba y `next_run_at` se calculaba siempre sobre `created_at`, dejando la tarea permanentemente "Vencida". Ahora sigue el mismo patrón de tracking que `cmd_ingest` y `cmd_subvenciones`.

---

## [1.4.0] — 2026-04-08

### Añadido

- **CNAE (Clasificación Nacional de Actividades Económicas):** nueva tabla de dimensión `dim.cnae_dim` con migración DDL (`013_dim_cnae.sql`) incluida en `INIT_MIGRATIONS`.
- **Ingesta CNAE desde fuente oficial:** módulo `etl/cnae_ingest.py` que obtiene los códigos CNAE-2025 de la API SDMX del ISTAC (datos.canarias.es), filtra códigos numéricos y realiza upsert en `dim.cnae_dim`.
- **CLI `licitia-etl cnae ingest`:** nuevo verbo para ejecutar la ingesta CNAE bajo demanda (patrón similar a BORME).
- **API `POST /cnae/ingest`:** endpoint REST para disparar la ingesta CNAE desde el panel de administración.
- **`dim_status` en `GET /health`:** el endpoint de salud ahora incluye el estado de las tablas de dimensión (`cpv`, `cnae`): `has_rows` y `has_embeddings`, permitiendo al panel detectar si faltan datos o indexaciones.

---

## [1.3.4] — 2026-04-07

### Corregido

- **`GET /scheduler/status` y `licitia-etl scheduler status`**: `get_next_run_at()` ahora acepta **`created_at`** (alineado con `api.py` y `get_tasks_due`). Antes el código desplegado llamaba con `created_at=...` pero la firma en `scheduler.py` no lo incluía → `TypeError: unexpected keyword argument 'created_at'`.
- **`get_tasks_due`** y **CLI** pasan `created_at` de la tarea para coherencia con el cálculo de `next_run_at` para tareas nuevas sin ejecución previa.

---

## [1.3.3] — 2026-04-07

### Mejorado

- **Logs de subvenciones (BDNS) para Docker / panel “Salida”**: `scrape_historico` y `scrape_diario` emiten progreso por línea (sin `tqdm`), prefijo `[subvenciones]`, marcadores `SCRAPING COMPLETADO` / error, y resúmenes por página (nuevos, duplicados, filtrados) en el flujo diario.

### Alineación de versiones

- **`__version__`**, `pyproject.toml` y metadatos OpenAPI (`FastAPI.version`) unificados en **1.3.3** (antes `__init__.py` / API seguían en 1.2.3 pese a tags posteriores).

### Eliminado

- **`tests/test_api.py`**: smoke tests HTTP contra la app FastAPI (sustituibles por pruebas manuales o contract tests en otro entorno si se desean).

---

## [1.3.2] — 2026-04-06

### Sincronización con repositorio padre

- **Merge de `BquantFinance/licitaciones-espana` `main`** en el fork: incorpora los commits pendientes (mapeo `BudgetAmount`, refactor **Galicia** con pipeline reanudable, `tests/test_galicia.py`, fixture `tests/fixtures/entry_budget.xml`, dependencias `python-dateutil` y `beautifulsoup4`).
- **Conflictos resueltos manualmente**: unión de `.gitignore` (artefactos Galicia upstream + rutas Andalucía/Asturias del fork), `requirements.txt` (deps fork + upstream), `galicia/scraper_galicia.py` (cierre correcto de `main()` según upstream), `tests/test_parsear_entry.py` (fixture de presupuesto upstream + tests extendidos del fork: URLs, criterios, requisitos), `README.md` (documentación del microservicio fork; enlace implícito al upstream para datasets por CCAA).

---

## [1.3.1] — 2026-04-06

### Añadido

- **CLI `licitia-etl subvenciones diario`** ([#24](https://github.com/sergioberino/licitaciones-espana/pull/24), cierra [#25](https://github.com/sergioberino/licitaciones-espana/issues/25)): comando explícito para la **actualización diaria** de subvenciones (misma lógica que puede usar el scheduler), invocando `scrape_diario` / `LatestParams` desde `nacional.subvenciones`. Documentación del módulo CLI actualizada (`subvenciones` en la lista de comandos).

### Mejorado

- **Actualización diaria BDNS**: la rama integrada en #24 afinó el flujo de “últimas” convocatorias (paginación, límite de página, filtros, fechas/camelCase→snake_case, queries e índices según subvenciones, etc.) además del punto de entrada CLI anterior.

### Nota (histórica)

La integración completa con `upstream/main` quedó resuelta en **[1.3.2]**.

---

## [1.3.0] — 2026-04-06

### Añadido

- **Ingesta de subvenciones nacionales (BDNS)** ([#21](https://github.com/sergioberino/licitaciones-espana/pull/21)): nueva fuente vía API `infosubvenciones.es` (`nacional/subvenciones.py`), subconjunto `nacional` / `subvenciones`, migración `schemas/012_nacional_subvenciones.sql`, integración en `ingest_l0`, CLI y scheduler (frecuencia diaria para actualización incremental).

### Corregido

- **JSONB y arrays numpy en ingesta L0** ([#23](https://github.com/sergioberino/licitaciones-espana/pull/23)): normalización recursiva de valores (p. ej. arrays numpy) antes de `psycopg2.extras.Json`, evitando fallos al serializar columnas JSONB en ingestas nacionales.

---

## [1.2.3] — 2026-03-27

### Añadido

- **`dim.nuts_spain`**: nueva tabla de dimensión con clasificación NUTS de España (77 regiones, niveles 1/2/3 según Eurostat). Columnas: `geocode` (PK), `etiqueta`. Idempotente (`ON CONFLICT DO UPDATE`).
- **Migración `004_nuts_spain.sql`** añadida a `INIT_MIGRATIONS` — se aplica automáticamente con `licitia-etl init-db` (CLI) y `POST /init-db` (API).

---

## [1.2.2] — 2026-03-27

### Correcciones

- **Fix crítico ingesta JSONB**: `pd.isna(v)` lanzaba `ValueError: The truth value of an array with more than one element is ambiguous` al procesar filas de `nacional/contratos_menores` con valores reales en las columnas JSONB (`docs_adicionales`, `criterios_adjudicacion`, `requisitos_solvencia`). Los tres batches fallaban completamente.
  - Guardado seguro: `v is None or (not isinstance(v, (list, dict)) and pd.isna(v))`.
  - Serialización JSONB correcta: `psycopg2.extras.Json(v)` para valores `list`/`dict`.
- Test de regresión `test_null_guard_safe_for_jsonb_list_and_dict` añadido.

### Pruebas

- 99 pruebas; 0 fallos.

---

## [1.2.1] — 2026-03-25

### Añadido

- **Intervalos de frecuencia personalizados en el scheduler**: las tareas pueden registrarse con cualquiera de los seis intervalos: `Diario`, `Semanal`, `Mensual`, `Trimestral`, `Semestral`, `Anual`.
- **`VALID_SCHEDULE_EXPRS`**: constante pública con los seis valores válidos de frecuencia.
- **`validate_schedule_expr(expr, default)`**: función auxiliar que valida una expresión de frecuencia y lanza `ValueError` si no es válida.
- **`get_next_run_at` — ramas `Diario`, `Semanal`, `Semestral`**: lógica de cálculo de próxima ejecución para las nuevas frecuencias.
  - `Diario`: misma día a las 02:00 si el slot no ha pasado, si no el día siguiente.
  - `Semanal`: lunes de la misma semana a las 02:00 si el slot no ha pasado, si no el lunes siguiente.
  - `Semestral`: 1 de enero o 1 de julio a las 02:00 (el siguiente en llegar).
- **`GET /scheduler/defaults`**: nuevo endpoint que devuelve los intervalos válidos y el intervalo predeterminado por tarea.
- **`POST /scheduler/register` — validación 422**: si `schedule_expr` es inválido (global o por tarea), devuelve HTTP 422.
- **`register_tasks` — `schedule_overrides`**: parámetro opcional `dict[tuple[str,str], str]` para sobrescribir la frecuencia de tareas concretas.
- **CLI `scheduler register --frecuencia`**: opción nueva que aplica una frecuencia global a todas las tareas registradas en esa llamada.
- **`get_all_task_pairs()`**: función pública que devuelve el conjunto de pares `(conjunto, subconjunto)` del registro.

### Pruebas

- 98 pruebas unitarias + de API (de 87 en v1.2.0); 0 fallos.
- Cobertura de ramas de `get_next_run_at` para cada una de las seis frecuencias.
- Tests de frontera en `:00` para `Diario`, `Semanal` y `Semestral`.

---

## [1.2.0] — 2026-03-25

### Añadido

- **Campo `valor_estimado_contrato`**: nueva columna NUMERIC(18,2) que almacena `EstimatedOverallContractAmount` del presupuesto UBL. Antes se mapeaba incorrectamente a `importe_sin_iva`.
- **Referencias documentales**: columnas `doc_legal_nombre`, `doc_legal_url` (PCAP), `doc_tecnico_nombre`, `doc_tecnico_url` (PPT), y `docs_adicionales` (JSONB) para documentos adicionales con nombre, URL y hash.
- **Criterios de adjudicación**: columna `criterios_adjudicacion` (JSONB) con tipo (OBJ/SUBJ), descripción, nota, subtipo y peso de cada criterio.
- **Requisitos de solvencia**: columna `requisitos_solvencia` (JSONB) con solvencia técnica, económica y requisitos específicos, cada grupo con nombre en español.
- **Sanitización de URLs**: función `_sanitizar_url()` que decodifica entidades HTML (`&amp;` → `&`) en todas las URLs extraídas de los atoms PLACSP.
- **Migración 011**: `schemas/011_nacional_new_columns.sql` — ALTER TABLE para añadir las 8 columnas nuevas a tablas `nacional_*` existentes. Registrada en `INIT_MIGRATIONS`.
- **Guía de arquitectura**: `docs/arquitectura-global.md` documenta el sistema de migraciones y cómo añadir cambios DDL futuros.

### Corregido

- **Mapeo de importes en `BudgetAmount`**: `EstimatedOverallContractAmount` se mapeaba a `importe_sin_iva` (incorrecto). Ahora: `EstimatedOverallContractAmount` → `valor_estimado_contrato`, `TaxExclusiveAmount` → `importe_sin_iva`, `TotalAmount` → `importe_con_iva` (sin cambio).
- **Shadowing de `subtipo_code`**: el loop de criterios de adjudicación sobreescribía la variable `subtipo_code` a nivel de proyecto. Renombrado a `crit_subtipo_code`.

### Upstream PR

- Issue y PR en español a `BquantFinance/licitaciones-espana` con la corrección del mapeo de importes (solo WI-1).

### Nota

- Las filas ya ingestadas tendrán `NULL` en las columnas nuevas. Solo nuevas ingestas populan los campos.
- `parsear_entry_cpm()` devuelve `None` para todos los campos nuevos (CPMs no tienen estos datos).

---

## [1.1.2] — 2026-03-25

### Upstream sync (BquantFinance/licitaciones-espana)

- **Andalucía scraper refactor** (`scripts/ccaa_andalucia.py`): improved coverage and stability (upstream PR #5); 11 new tests in `tests/test_ccaa_andalucia.py`
- **Asturias downloader script** (`scripts/ccaa_asturias.py`): new standalone script to fetch contracts from Asturias open-data portal to Parquet; see `scripts/README.md` for usage. Ingest wiring (CONJUNTOS_REGISTRY) deferred to 1.2.0.
- **Calidad analytics module** (`calidad/calidad_licitaciones.py`): 20 quality indicators on contracts dataset; standalone tool, not part of ingest pipeline
- `requirements.txt`: `requests>=2.31.0` added; `numpy<2` pin preserved

### Fork changes

- **Data hygiene**: removed `catalunya/README.md` (doc-only); `.gitignore` updated to cover `asturias_data/`, `ccaa_Andalucia/perfiles_cache.json`, `catalunya/`
- **No new HTTP API routes** added in this release; API surface unchanged from 1.1.1
- **No new CLI subcommands** added; ingest catalog visible via `GET /ingest/conjuntos` (8 conjuntos)

### QA performed (2026-03-25)

**CLI / pytest**

- 55 tests passed, 1 skipped (DB-dependent), 0 failed — full suite including 11 new upstream Andalucía tests
- `python -m etl.cli --help` and `python -m etl.cli ingest --help` — exit 0, no import errors
- `calidad.calidad_licitaciones` imports cleanly

**API (ETL microservice at `http://localhost:8002`)**

| Endpoint                 | HTTP | Notes                                              |
| ------------------------ | ---- | -------------------------------------------------- |
| `GET /health`            | 200  | `db: true`, 4 migrations pending                   |
| `GET /status`            | 200  | `database: connected`                              |
| `GET /db-info`           | 200  | 37 tables, 765 MB                                  |
| `GET /ingest/conjuntos`  | 200  | 8 conjuntos returned                               |
| `GET /migrations`        | 200  | 6 applied, 4 pending                               |
| `GET /scheduler/status`  | 200  | 5 nacional tasks registered, `loop_running: false` |
| `GET /scheduler/running` | 200  | No active runs                                     |

No 500 errors. All endpoints returned expected codes.

### Notes

- API version string updated from 1.1.0 → 1.1.2
- 4 schema migrations on disk pending application (`005_catalunya.sql`, `006_valencia.sql`, `007_views.sql`, `010_borme.sql`) — apply via `POST /init-db` or `licitia-etl init-db`
- `etl/__init__.py` `__version__` remains `1.1.0`; full multi-file semver alignment planned for 1.2.0

---

## [1.1.1] - 2026-03-03

### Añadido

- **Campo `loop_running` en `GET /scheduler/status`.** Indica si el daemon del scheduler está activo (comprobación por PID file y señal al proceso).
- **`GET /scheduler/running`.** Devuelve la lista de ejecuciones con estado `running` (run_id, task_id, conjunto, subconjunto, process_id, started_at).
- **`POST /scheduler/register` con `tasks[]`.** Acepta un cuerpo `{ "tasks": [{ "conjunto": "...", "subconjunto": "..." }, ...] }` para registrar solo los pares indicados (además del registro por `conjuntos[]`).
- **`POST /scheduler/runs/stop`.** Recibe `{ "run_ids": [ ... ] }`; envía SIGTERM a los procesos de las ejecuciones indicadas y las marca como `failed` con mensaje «Detenido por usuario».
- **`POST /scheduler/unregister`.** Recibe `{ "tasks": [{ "conjunto", "subconjunto" }, ...] }`; elimina las tareas programadas correspondientes (y sus runs por CASCADE).

### Corregido

- **`get_next_run_at`.** La comparación para decidir el siguiente slot programado usa `last_finished_at` (en zona Europe/Madrid) en lugar de `now`, de modo que una tarea programada para el mismo día se ejecute correctamente después de una ejecución previa.

---

## [1.0.0] - 2026-02-17 - Release producción (scheduler completo)

### Añadido

- **Scheduler con config en BD:** Schema `scheduler` con tablas `scheduler.tasks` y `scheduler.runs`. Poblado con `licitia-etl scheduler register`; ejecución con `scheduler run` (intervalo `--tick-seconds`, por defecto 60). Frecuencias: Mensual (día 1 a 02:00 Europe/Madrid), Trimestral (ene/abr/jul/oct), Anual (1 enero).
- **scheduler run / stop:** `scheduler run` inicia el daemon; PID file en directorio temporal; `scheduler stop` envía SIGTERM y elimina el PID file. Con `-d` el daemon se ejecuta en segundo plano y la salida va a `scheduler.log`.
- **scheduler status:** Tabla con última ejecución, estado (**scheduled** cuando la última run fue ok, **running** / **failed**), PID, filas y columna **PRÓXIMA EJECUCIÓN** (fecha/hora de la siguiente ejecución programada).
- **Registro de runs desde ingest:** Cada ingest registra un run al inicio (evita solapamiento) y actualiza al finalizar (status, filas, error). `scheduler status` refleja el estado actual.
- **Comandos `health` y `db-info`:** Comprobación de BD y schema scheduler; tamaño y listado de tablas para supervisión.

### Corregido

- **get_tasks_due:** Uso de `reference_now` en `get_next_run_at` para que las tareas nunca ejecutadas se consideren debidas en el mismo tick (evita desfase de milisegundos que dejaba `due_count` en 0).
- **insert_run_start:** Firma correcta `insert_run_start(conn, task_id)` en `cmd_ingest`.

### Nota

- El ETL no crea ni mantiene `dim.cpv_router`; es responsabilidad del indexador si se usa.

---

## [0.2.0] - Ingesta L0 definitiva (todos los conjuntos + PK surrogada + tests)

### Añadido

- **Comando `ingest` (todos los conjuntos):** `licitia-etl ingest <conjunto> <subconjunto> [--anos X-Y] [--solo-descargar|--solo-procesar]`. Conjuntos: **nacional** (5 subconjuntos), **catalunya** (5), **valencia** (14), **andalucia** (1). Requiere `DB_*` y `DB_SCHEMA`; nacional requiere además `--anos` X-Y.
- **PK surrogada en tablas L0:** Todas las tablas L0 usan `l0_id` BIGSERIAL PRIMARY KEY y `natural_id` TEXT UNIQUE NOT NULL (URL o identificador de fuente), para evitar PK de texto largas y facilitar búsquedas por otros índices.
- **Tablas L0 en DB_SCHEMA:** Tablas `<conjunto>_<subconjunto>` con columnas del parquet (o inferidas) más extensiones CPV (`principal_prefix4`, `principal_prefix6`, `secondary_prefix6`) e `ingested_at`. Carga idempotente con `ON CONFLICT (natural_id) DO NOTHING`.
- **Registro de conjuntos (`CONJUNTOS_REGISTRY`):** Configuración por conjunto (subconjuntos, resolución de parquet, scripts, columnas). Catalunya/Valencia/Andalucía infieren esquema desde el parquet.
- **Integración con scripts:** Nacional invoca `nacional.licitaciones`; catalunya/valencia/andalucía ejecutan los scripts en `scripts/` (ccaa_cataluna, ccaa_valencia_parquet, ccaa_andalucia, etc.) cuando el parquet no existe.
- **Guía de usuario:** Sección «Comando ingest» en `docs/quick-guide-deploy.md` con tabla de conjuntos/subconjuntos, variables de entorno y ejemplos mínimos.
- **Rutina de tests (pytest):** `tests/test_ingest_l0.py` con tests unitarios (registro, rutas, derive_cpv_prefixes, inferencia de esquema) y test de idempotencia marcado con `@pytest.mark.integration` (requiere DB). Ejecución: `pytest tests/ -v` o `pytest tests/ -v -m "not integration"` para solo unitarios.
- **Dependencia pytest** en requirements.txt.

### Cambiado

- **ingest_l0:** `ensure_l0_table` y `load_parquet_to_l0` aceptan `column_defs` y `natural_id_col` opcionales para soportar fuentes genéricas (catalunya, valencia, andalucía). Nacional sigue usando el esquema fijo de 40 columnas.
- **CLI:** `cmd_ingest` valida conjunto y subconjunto contra el registro y enruta scripts y parquet según el conjunto.

### Nota

- E2E recomendado: `init-db` y luego `ingest nacional consultas_preliminares --anos 2026-2026`; re-ejecutar y comprobar 0 insertadas y N omitidas. Para catalunya/valencia/andalucía, ejecutar antes los scripts en `scripts/` para generar los parquets.

---

## [0.1.1] - Datos estáticos dim y schemas en Postgres init

### Añadido

- **Schemas en init de Postgres:** En el script de inicialización del microservicio Postgres (`init.d/01-pgvector.sql`) se crean los esquemas `dim`, `l0`, `l1` y `l2` tras la extensión pgvector, de modo que al levantar el contenedor las capas existan antes de que el ETL ejecute init-db.
- **Datos estáticos dim en init-db:** init-db aplica y rellena la capa dim: `dim.dim_ccaa`, `dim.dim_provincia` y `dim.dim_dir3`. Los esquemas 002_dim_ccaa y 002b_dim_provincia crean las tablas en el schema `dim` con tipos válidos en PostgreSQL (INTEGER sin precisión); la FK de provincias referencia `dim.dim_ccaa(num_code)`.
- **Ingesta DIR3 desde Listado Unidades AGE:** Tras aplicar `003_dim_dir3.sql`, init-db descarga el XLSX del Listado de información básica de unidades orgánicas de la AGE (administracionelectronica.gob.es / datos.gob.es), lo parsea con pandas (openpyxl), mapea columnas a `dim.dim_dir3` e inserta (TRUNCATE + INSERT). Logging en español con prefijo `[dir3_ingest]`; User-Agent, timeout y reintentos en la descarga. La URL es configurable con `DIR3_XLSX_URL`.
- **Dependencia openpyxl** en requirements.txt para lectura de XLSX.

### Cambiado

- **Orden de migraciones:** `SCHEMA_FILES` en init-db queda reducido a la capa dim: `001_dim_cpv.sql`, `002_dim_ccaa.sql`, `002b_dim_provincia.sql`, `003_dim_dir3.sql`, más scheduler (008, 009). cpv_router no es responsabilidad del ETL.
- **FK recursiva dim_dir3:** La restricción `fk_dim_dir3_parent` es `DEFERRABLE INITIALLY DEFERRED` para permitir insertar filas en cualquier orden; se comprueba al commit.

### Nota

- Las fuentes oficiales de los catálogos estáticos (CPV, DIR3, códigos de provincias y CCAA) se documentarán en una iteración posterior.

---

## [0.1.0] - Primera iteración publicable

### Añadido

- **Guía rápida de despliegue:** Documento `docs/quick-guide-deploy.md` (en español): requisitos, arranque rápido, tabla de comandos del CLI, variables de entorno y enlace a notas operativas. Sustituye a `how-to-use-deployment.md`.
- **Comando `status`:** Comprueba en un solo comando la conexión a la base de datos. Mensajes en español. Sustituye a comandos previos de comprobación, que pasan a ser subrutinas internas.
- **CLI en español:** Descripciones, ayuda de argumentos y mensajes de éxito/error en español.
- **Ayuda con atribución:** En `licitia-etl --help` se muestra el origen del fork (BquantFinance/licitaciones-espana) y la autoría del CLI (Sergio Berino, Grupo TOP Digital).
- **Descubribilidad:** En la ayuda se listan las integraciones (PostgreSQL), las variables de entorno (.env) y el orden recomendado: `status` → `init-db` → `ingest`.
- **Mensajes de retorno estándar:** Mensajes claros para salud (conexión DB, estado global) y para init-db / ingest; stderr para errores, exit 0/1 documentados.
- **Versión 0.1.0** fijada en el paquete; `--version` muestra la versión e indica CHANGELOG.md para el historial.

### Cambiado

- La lógica de comprobación de BD se usa desde `status` mediante `_comprobar_base_datos()`.
- Comentarios y docstrings en `cli.py` y `config.py` traducidos al español (invariante de microservicio atómico mantenido).

### Extensión futura (no implementado en esta iteración)

- Prefijo de log opcional (p. ej. variable `LICITIA_ETL_LOG_PREFIX`) para logs agregados; queda para una iteración posterior.
