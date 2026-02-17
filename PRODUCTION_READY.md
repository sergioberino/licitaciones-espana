# ETL 1.0.0 — Listo para producción

**Fecha:** 2026-02-17  
**Versión:** 1.0.0  
**Estado:** Listo para pushear.

## Checklist

- [x] **CHANGELOG.md** — Actualizado con release 1.0.0 (scheduler completo, PRÓXIMA EJECUCIÓN, scheduled, fix reference_now, scheduler.log, sin embeddings). En español.
- [x] **README.md** — Comandos, configuración, roadmap y nota de release 1.0.0. En español.
- [x] **docs/quick-guide-deploy.md** — Guía de despliegue con scheduler run -d, scheduler.log y scheduler status (PRÓXIMA EJECUCIÓN). En español.
- [x] **Versión** — `etl/__init__.py` y `pyproject.toml` en 1.0.0.
- [x] **Embeddings** — Todas las referencias eliminadas del dominio del ETL (fuera de ámbito).
- [x] **Scheduler** — Register, run, stop, status con columna PRÓXIMA EJECUCIÓN y estado scheduled/running/failed; salida del daemon en scheduler.log; corrección get_tasks_due (reference_now).

## Resumen

El microservicio ETL está listo para uso en producción: inicialización de BD (init-db), ingesta L0 (ingest), scheduler programado (register → run → status), health y db-info. Documentación en español y sin dependencias de embedding en este repositorio.
