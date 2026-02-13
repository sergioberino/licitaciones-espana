# Changelog

Todos los cambios notables del CLI y del microservicio ETL se documentan aquí.

## [0.1.1] - Datos estáticos dim y schemas en Postgres init

### Añadido

- **Schemas en init de Postgres:** En el script de inicialización del microservicio Postgres (`init.d/01-pgvector.sql`) se crean los esquemas `dim`, `l0`, `l1` y `l2` tras la extensión pgvector, de modo que al levantar el contenedor las capas existan antes de que el ETL ejecute init-db.
- **Datos estáticos dim en init-db:** init-db aplica y rellena la capa dim: `dim.dim_ccaa`, `dim.dim_provincia` y `dim.dim_dir3`. Los esquemas 002_dim_ccaa y 002b_dim_provincia crean las tablas en el schema `dim` con tipos válidos en PostgreSQL (INTEGER sin precisión); la FK de provincias referencia `dim.dim_ccaa(num_code)`.
- **Ingesta DIR3 desde Listado Unidades AGE:** Tras aplicar `003_dim_dir3.sql`, init-db descarga el XLSX del Listado de información básica de unidades orgánicas de la AGE (administracionelectronica.gob.es / datos.gob.es), lo parsea con pandas (openpyxl), mapea columnas a `dim.dim_dir3` e inserta (TRUNCATE + INSERT). Logging en español con prefijo `[dir3_ingest]`; User-Agent, timeout y reintentos en la descarga. La URL es configurable con `DIR3_XLSX_URL`.
- **Dependencia openpyxl** en requirements.txt para lectura de XLSX.

### Cambiado

- **Orden de migraciones:** `SCHEMA_FILES` en init-db queda reducido a la capa dim: `001_dim_cpv.sql`, `001b_dim_cpv_router.sql`, `002_dim_ccaa.sql`, `002b_dim_provincia.sql`, `003_dim_dir3.sql`. Los esquemas de sectores (004_nacional, 005_catalunya, 006_valencia, 007_views) quedan para cuando se alineen con la documentación TDR del proyecto.
- **FK recursiva dim_dir3:** La restricción `fk_dim_dir3_parent` es `DEFERRABLE INITIALLY DEFERRED` para permitir insertar filas en cualquier orden; se comprueba al commit.

### Nota

- Las fuentes oficiales de los catálogos estáticos (CPV, DIR3, códigos de provincias y CCAA) se documentarán en una iteración posterior.

---

## [0.1.0] - Primera iteración publicable

### Añadido

- **Guía rápida de despliegue:** Documento `docs/quick-guide-deploy.md` (en español): requisitos, arranque rápido, tabla de comandos del CLI, variables de entorno y enlace a notas operativas. Sustituye a `how-to-use-deployment.md`.
- **Comando `status`:** Comprueba en un solo comando la conexión a la base de datos y al servicio de embedding. Mensajes en español (p. ej. "ETL está operativo. Base de datos y servicio de embedding accesibles."). Sustituye a los comandos públicos `check-connection` y `check-embedding`, que pasan a ser subrutinas internas.
- **CLI en español:** Descripciones, ayuda de argumentos y mensajes de éxito/error en español.
- **Ayuda con atribución:** En `licitia-etl --help` se muestra el origen del fork (BquantFinance/licitaciones-espana) y la autoría del CLI (Sergio Berino, Grupo TOP Digital).
- **Descubribilidad:** En la ayuda se listan las integraciones (PostgreSQL, servicio de embedding), las variables de entorno (.env) y el orden recomendado: `status` → `init-db` → `generate_embedding --target cpv`.
- **Mensajes de retorno estándar:** Mensajes claros para salud (conexión DB, embedding, estado global) y para init-db / generate_embedding; stderr para errores, exit 0/1 documentados.
- **Versión 0.1.0** fijada en el paquete; `--version` muestra la versión e indica CHANGELOG.md para el historial.

### Cambiado

- `check-connection` y `check-embedding` dejan de ser comandos públicos; su lógica se usa desde `status` mediante `_comprobar_base_datos()` y `_comprobar_embedding()`.
- Comentarios y docstrings en `cli.py`, `config.py` y `embed_cpv.py` traducidos al español (invariante de microservicio atómico mantenido).

### Extensión futura (no implementado en esta iteración)

- Prefijo de log opcional (p. ej. variable `LICITIA_ETL_LOG_PREFIX`) para logs agregados; queda para una iteración posterior.
