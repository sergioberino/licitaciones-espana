# ETL — Microservicio CLI (licitia-etl)

Microservicio **ETL** que se integra con una **base de datos PostgreSQL** para preparar y mantener esquemas, dimensiones y tablas L0 de ingesta. Aprovecha las extracciones y el contexto de datos del repositorio original [BquantFinance/licitaciones-espana](https://github.com/BquantFinance/licitaciones-espana).

## Qué hace este servicio

- **Comprobar estado** (`status`): conexión a la base de datos.
- **Inicializar base de datos** (`init-db`): aplica esquemas SQL (dim, scheduler, tablas L0) y carga datos estáticos (p. ej. dimensión CPV, DIR3).
- **Ingestar datos** (`ingest`): descarga o usa parquets existentes y carga en tablas L0 del schema de trabajo.
- **Scheduler** (`scheduler register` / `run` / `stop` / `status`): registra tareas por conjunto y las ejecuta según frecuencia (Mensual/Trimestral/Anual).
- **Supervisión** (`health`, `db-info`): comprobación de BD y schema scheduler; tamaño y listado de tablas.

Configuración por **variables de entorno** (`.env`). Funciona en modo **standalone** o dentro de un stack orquestado (Docker Compose).

## Comandos

| Comando | Descripción |
|--------|-------------|
| `licitia-etl status` | Comprueba conexión a la base de datos. |
| `licitia-etl init-db` | Aplica esquemas y carga datos estáticos (CPV, DIR3, Provincias, CCAA). |
| `licitia-etl ingest` *conjunto* *subconjunto* [*--anos*] | Descarga/genera parquet e ingesta en tablas L0. |
| `licitia-etl scheduler register` [*conjuntos*] | Registra tareas programadas (sin argumentos: todos los conjuntos). |
| `licitia-etl scheduler run` | Inicia el scheduler (ejecuta tareas según frecuencia). |
| `licitia-etl scheduler stop` | Detiene el proceso del scheduler. |
| `licitia-etl scheduler status` | Lista tareas con última ejecución, estado (scheduled/running/failed) y **PRÓXIMA EJECUCIÓN**. |
| `licitia-etl health` | Comprueba BD y schema scheduler; código 0/1 para supervisión. |
| `licitia-etl db-info` | Muestra tamaño por schema y listado de tablas. |

Orden recomendado: **status** → **init-db** → **ingest**; para ejecución programada: **scheduler register** → **scheduler run**.

Ver `licitia-etl --help` para opciones y atribución (fork y autor del CLI).

## Configuración

Variables principales en el `.env` de este servicio: base de datos (host, puerto, nombre, usuario, contraseña) y schema de trabajo. Copiar `.env.example` a `.env` y rellenar los valores necesarios.

Detalle: [docs/quick-guide-deploy.md](docs/quick-guide-deploy.md).

**Release 1.0.0** — Listo para producción. Scheduler completo (register, run, stop, status con PRÓXIMA EJECUCIÓN), ingest L0, health, db-info. Sin referencias a embeddings en este dominio.

## Roadmap

- Esquemas y datos estáticos incluidos; `init-db` los aplica. Scheduler con tareas en BD, ejecución según frecuencia (Mensual/Trimestral/Anual), `scheduler.log` y status con PRÓXIMA EJECUCIÓN.
- La generación de embeddings no pertenece al dominio del ETL; es responsabilidad del microservicio de indexación si se requiere.

## Más información

- **Changelog:** [CHANGELOG.md](CHANGELOG.md)

## Créditos

- **Repo de extracción de datos públicos:** [@Gsnchez](https://twitter.com/Gsnchez) | [BQuant Finance](https://bquantfinance.com)
- **Mantenedor del CLI (licitia-etl):** [Sergio Berino](https://es.linkedin.com/in/sergio-emilio-berino-a19a53328/en) | [Grupo TOP Digital](https://grupotopdigital.es/)
