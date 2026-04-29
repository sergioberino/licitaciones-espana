# Política de actualizaciones en cadencia (*rolling*) entre repositorios enlazados

**Estado:** definitivo.  
**Audiencia:** mantenedores, release managers, agentes y revisores de cambios.

**Nota editorial:** el §1 nombra **licitaciones-espana** solo como ejemplo de línea de producto y modos de uso. Del **§2 en adelante** el texto usa **roles** (*repositorio de referencia* / *repositorio de integración*) y no nombra organizaciones, URLs ni otros repositorios privados.

---

## 1. Aplicabilidad

Esta política aplica en **dos modos** (pueden coexistir en el mismo ecosistema de producto):

1. **Repositorio aislado** — el árbol de **licitaciones-espana** se usa **solo**: releases, tags SemVer y changelog viven en ese repo; quien despliegue consume directamente ese tag o SHA. Las reglas de versionado (§4) y el significado de *rolling* en el producto (§3, primer ítem) se cumplen **enteramente** en ese repositorio, sin puntero externo.

2. **Integración vía submódulo (u otro vínculo versionado)** — el mismo código de **licitaciones-espana** se incorpora como **submódulo** (o equivalente) en un **repositorio de integración**. Ahí el consumo real es el **puntero** (commit del submódulo) que el integración fija; las actualizaciones “en cadencia” son el **bump** de ese puntero hacia tags **publicados** del referencia (§5), alineado con la cadencia y criterios de merge acordados.

**Cómo se relacionan las actualizaciones entre ambos modos:** el referencia (p. ej. licitaciones-espana) **publica** la verdad de versión (`vMAJOR.MINOR.PATCH`, notas, changelog). El modo aislado avanza siguiendo solo esa línea. El modo integración **no reescribe** esa verdad: **reproduce** una revisión concreta del referencia y la adelanta de forma explícita y trazable (commit de bump en el integración). Así, un mismo tag del referencia es la **unidad** que puede desplegarse sola o embebida; la política de *rolling* asegura que el integración no quede rezagado indefinidamente ni adelante de lo publicado sin decisión.

---

## 2. Roles

| Rol | Descripción |
|-----|-------------|
| **Repositorio de referencia** | Donde vive el código de la **línea de producto** consumida como dependencia (ETL, librería, etc.). Publica **versiones** (tags, releases) con artefactos de esquema o contratos que esa versión exige. |
| **Repositorio de integración** | Donde se **fija** la revisión aceptada del referencia (SHA/tag), se construyen imágenes, compose y flujos de despliegue. Es el lugar habitual del **bump** de dependencia. |

En el modo **solo referencia**, un mismo repositorio cumple el rol de referencia y no hay segundo repo que puntee. En el modo **submódulo**, **licitaciones-espana** actúa como referencia y el otro árbol como integración. Ambos roles se adhieren a **la misma política**; solo cambia qué repositorio desempeña cada rol.

---

## 3. Qué significa “rolling updates” en este contexto

No se usa como sinónimo exclusivo de “rolling deployment” en orquestadores. Aquí designa:

1. **Avance por versiones pequeñas y frecuentes** del repositorio de referencia (releases semver cuando el cambio lo permita).
2. **Alineación continua o por ventanas** del repositorio de integración: actualizar el puntero hacia el tag o SHA acordado **sin parones largos** entre entornos que deban mantenerse homogéneos.
3. **Rollback explícito**: si una versión falla en validación, el puntero vuelve a la última combinación **conocida buena**; no se “reescribe historia” de tags publicados como consumibles.

---

## 4. Versionado en el repositorio de referencia

- **Formato:** [SemVer 2.0.0](https://semver.org/) (`MAJOR.MINOR.PATCH`) en tags anotados o equivalente acordado por el equipo.
- **Tags de release:** inmutables una vez publicados como consumo externo; correcciones van en un **nuevo** PATCH (o hotfix documentado).
- **CHANGELOG:** cada release que altere comportamiento observable (API, esquema, flags, rutas de datos) debe tener entrada trazable (archivo `CHANGELOG`, notas de release o ambos — lo que el referencia ya use se mantiene).
- **MAJOR:** cambios que exigen pasos manuales coordinados, migración de datos incompatible o eliminación de contratos públicos.
- **MINOR:** funcionalidad compatible hacia atrás dentro del contrato actual.
- **PATCH:** correcciones y ajustes compatibles.

---

## 5. Cadencia por issue resuelto, evidencia mínima y bump en integración

### 5.1 Repositorio de referencia: de issue a release SemVer

- **Flujo mínimo trazable:** **Issue** → **PR** que **menciona** ese issue en el cuerpo (p. ej. `Closes #N` / `Fixes #N`) → **revisión** → **integración a la rama de release**. El CI del referencia puede exigir esa mención (p. ej. workflow *PR Requires Linked Issue*).
- **Cadencia *rolling* (referencia):** coincide con **cada issue resuelto** e integrado: no hay calendario fijo intermedio; el ritmo es el del trabajo cerrado con PR.
- **Regla por defecto (hoy):** **un issue resuelto → un incremento PATCH** (`MAJOR.MINOR.PATCH+1`), salvo que el alcance del cambio imponga otro tipo de release (ver siguiente viñeta).
- **Hotfix, MINOR o MAJOR:** según **complejidad** y **superficie afectada** del issue (API, esquema, contratos, compatibilidad), el mantenedor clasifica el merge como **hotfix** (PATCH urgente documentado), **MINOR** (funcionalidad compatible) o **MAJOR** (**breaking**). La decisión queda reflejada en el número SemVer del tag y en notas de release / `CHANGELOG`.
- **MAJOR y otros releases con breaking changes:** quedan a cargo del **mantenedor del repositorio** de referencia; en última instancia pueden alinearse con la **completación de un milestone** en GitHub (cierre coordinado de un conjunto de issues), sin sustituir el criterio humano de compatibilidad.
- **Automatización futura:** este esquema (issue cerrado + PR vinculado + tipo de bump) es **determinista** y apto para codificarlo después en GitHub Actions (p. ej. validar enlace issue–PR antes de etiquetar, sugerir o exigir bump según etiquetas del issue o del milestone).

### 5.2 Repositorio de integración: bump del puntero

- **Regla base:** el puntero al referencia solo avanza a **tags publicados** (o SHAs explícitamente acordados como equivalentes a release), nunca a ramas efímeras salvo entornos de experimentación claramente marcados.
- **Evidencia mínima aceptable para merge del bump:** que exista en el referencia una **release SemVer publicada** que cumpla el contrato de **§5.1** (cambio originado en issue, PR que lo referencia, revisión e integración). El integración no inventa versión: **reproduce** el tag del referencia.
- **Cadencia del bump:** la misma que la del referencia en la práctica — **cada nueva release publicada** que el integración deba consumir (típicamente tras cada issue resuelto que dispare PATCH, o tras MINOR/MAJOR cuando toque).
- **Congelación:** ventanas donde no se sube MINOR/MAJOR (p. ej. antes de demo o producción) se anuncian en el canal del equipo; PATCH de seguridad puede excepcionar la regla con acuerdo explícito.

---

## 6. Coherencia entre ambos repositorios

- **Una sola política:** los mismos criterios SemVer, changelog y etiquetas inmutables aplican al referencia; el integración **bumpea el puntero** según las releases generadas bajo **§5.1** (cadencia ligada a issues resueltos, no a un calendario abstracto).
- **Documentación de contexto:** el contrato expuesto a agentes y humanos (p. ej. “cómo fijar versión del referencia”, “qué hace un bump”) debe repetir estos puntos sin duplicar secretos operativos (credenciales, hosts internos).
- **Seguridad y confidencialidad:** nombres de repos privados u org no son necesarios para cumplir la política en los apartados genéricos; los enlaces internos pueden vivir fuera de este archivo.

---

## 7. Historial de ediciones

| Fecha | Cambio |
|-------|--------|
| 2026-04-28 | Creación: política neutral *rolling* entre referencia e integración. |
| 2026-04-28 | Versión **definitiva**: §1 *Aplicabilidad* (modo aislado vs submódulo); eliminados apartados previos de alcance CI/DDL y próximos pasos; renumeración. |
| 2026-04-28 | §5: cadencia por **issue resuelto**; flujo Issue→PR→revisión→SemVer (PATCH por defecto; hotfix/MINOR/MAJOR según alcance; MAJOR/milestones mantenedor); bump en integración alineado a tags; nota sobre automatización CI futura. |
| 2026-04-28 | Copia canónica en este repositorio (sincronizar con el repo de integración al cambiar la política). |
