# ETL — Microservicio CLI (licitia-etl)

Microservicio **ETL** que se integra con una **base de datos PostgreSQL** y un **servicio de embedding** para preparar y mantener esquemas, dimensiones y el índice CPV de búsqueda semántica. Aprovecha las extracciones y el contexto de datos del repositorio original [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana).

## Qué hace este servicio

- **Comprobar estado** (`status`): conexión a la base de datos y al servicio de embedding.
- **Inicializar base de datos** (`init-db`): aplica esquemas SQL (dim, tablas nacional/catalunya/valencia, vistas) y carga datos estáticos (p. ej. dimensión CPV).
- **Rellenar índice CPV** (`generate_embedding`): lee la dimensión CPV, llama al servicio de embedding por lotes e inserta vectores en `dim.cpv_router` para búsqueda por similitud.

Configuración solo por **variables de entorno** (`.env`). Funciona en modo **standalone** (tu Postgres y tu embedding) o dentro de un stack orquestado (Docker Compose).

## Comandos

| Comando | Descripción |
|--------|-------------|
| `licitia-etl status` | Comprueba conexión a base de datos y servicio de embedding. |
| `licitia-etl init-db` | Aplica esquemas y carga datos estáticos (dim, CPV). |
| `licitia-etl generate_embedding --target cpv` | Rellena el índice de embedding para el router CPV. |

Orden recomendado: **status** → **init-db** → **generate_embedding --target cpv**.

Ver `licitia-etl --help` para opciones, variables de entorno y atribución (fork y autor del CLI).

## Configuración

Variables principales en el `.env` de este servicio:

- **Base de datos:** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **Servicio de embedding:** `EMBEDDING_SERVICE_URL`
- **Opcionales (generate_embedding):** `EMBED_BATCH_SIZE`, `INGEST_BATCH_SIZE`, `EMBED_MAX_WORKERS`

Ejemplo mínimo: copiar `.env.example` a `.env` y definir al menos `DB_*` y `EMBEDDING_SERVICE_URL`.

## Cómo ejecutarlo

- **En Docker (orquestado):** desde la raíz del repo de ingestión, el compose arranca postgres, embedding y este servicio ETL. Ejemplo: `docker compose run --rm etl licitia-etl status`.
- **Standalone:** Postgres y el servicio de embedding deben estar accesibles; configurar `DB_HOST` y `EMBEDDING_SERVICE_URL` en consecuencia.

Detalle: [docs/quick-guide-deploy.md](docs/quick-guide-deploy.md).

## Roadmap

- **v0.1.0 (actual):** Esquemas y datos estáticos (CPV, dim) incluidos en el repo; `init-db` los aplica. Índice CPV poblado con `generate_embedding --target cpv`.
- **Próximamente:** Recuperación automática de datos estáticos (CPV, DIR3, códigos de provincias y CCAA) desde fuentes oficiales. Mejoras alineadas con [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana) se pueden proponer en el repositorio original.

## Más información

- **Changelog:** [CHANGELOG.md](CHANGELOG.md)
  
## Créditos

- **Repo de extracción de datos públicos:** [@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
- **Mantenedor del CLI (licitia-etl):** [Sergio Berino](https://es.linkedin.com/in/sergio-emilio-berino-a19a53328/en) | [Grupo TOP Digital](https://grupotopdigital.es/)
