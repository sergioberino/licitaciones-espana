-- 022_nacional_procedimiento_code_integer.sql
-- PLACSP/CODICE: procedimiento_code como INTEGER y etiquetas alineadas (1-8 + 100 + 999).
-- Omite nacional_subvenciones (esquema distinto). Usa etl.db_schema si está definido; si no, 'l0'.

DO $$
DECLARE
    sch TEXT := current_setting('etl.db_schema', true);
    tbl TEXT;
    col_type TEXT;
BEGIN
    IF sch IS NULL OR sch = '' THEN
        sch := 'l0';
    END IF;

    FOR tbl IN
        SELECT t.tablename
        FROM pg_tables t
        WHERE t.schemaname = sch
          AND t.tablename LIKE 'nacional_%'
          AND t.tablename <> 'nacional_subvenciones'
    LOOP
        SELECT c.data_type INTO col_type
        FROM information_schema.columns c
        WHERE c.table_schema = sch
          AND c.table_name = tbl
          AND c.column_name = 'procedimiento_code';

        IF col_type IS NULL THEN
            CONTINUE;
        END IF;

        IF col_type IN ('text', 'character varying') THEN
            EXECUTE format(
                'ALTER TABLE %I.%I ALTER COLUMN procedimiento_code TYPE INTEGER USING ('
                'CASE '
                '  WHEN procedimiento_code IS NULL THEN NULL '
                '  WHEN trim(both from procedimiento_code::text) = '''' THEN NULL '
                '  WHEN trim(both from procedimiento_code::text) ~ ''^[0-9]+$'' '
                '    THEN trim(both from procedimiento_code::text)::integer '
                '  ELSE NULL '
                'END)',
                sch,
                tbl
            );
        END IF;

        EXECUTE format(
            'UPDATE %I.%I SET procedimiento = CASE procedimiento_code '
            '  WHEN 1 THEN ''Abierto'' '
            '  WHEN 2 THEN ''Restringido'' '
            '  WHEN 3 THEN ''Negociado sin publicidad'' '
            '  WHEN 4 THEN ''Negociado con publicidad'' '
            '  WHEN 5 THEN ''Diálogo competitivo'' '
            '  WHEN 6 THEN ''Asociación para la innovación'' '
            '  WHEN 7 THEN ''Concurso de proyectos'' '
            '  WHEN 8 THEN ''Contrato menor'' '
            '  WHEN 100 THEN ''Basado en acuerdo marco'' '
            '  WHEN 999 THEN ''Otros'' '
            '  ELSE CAST(procedimiento_code AS TEXT) '
            'END '
            'WHERE procedimiento_code IS NOT NULL',
            sch,
            tbl
        );
    END LOOP;
END
$$;
