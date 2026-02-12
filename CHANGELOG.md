# Changelog

Todos los cambios notables del CLI y del microservicio ETL se documentan aquí.

## [0.1.0] - Primera iteración publicable

### Añadido

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
