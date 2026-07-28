\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
ALTER SYSTEM SET pg_task.json = '[{"data":"postgres"},{"data":"postgres","schema":"task_make_test_schema","table":"task_make_test"}]';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('task_make_test_schema.task_make_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('task_make_test_schema.task_make_test') IS NOT NULL AS table_created;
SELECT count(*) = 28 AS column_count_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attnum > 0 AND NOT attisdropped;
SELECT array_agg(enumlabel::text ORDER BY enumsortorder) = ARRAY['PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP'] AS enum_ok FROM pg_catalog.pg_enum WHERE enumtypid = 'task_make_test_schema.state'::regtype;
SELECT bool_and(attnotnull) AS not_null_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attname IN ('id', 'plan', 'active', 'live', 'repeat', 'timeout', 'count', 'max', 'state', 'delete', 'drift', 'header', 'save', 'string', 'delimiter', 'escape', 'quote', 'group', 'input', 'null');
SELECT bool_and(NOT attnotnull) AS nullable_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attname IN ('parent', 'start', 'stop', 'pid', 'data', 'error', 'output', 'remote');
SELECT count(*) = 6 AS index_count_ok FROM pg_catalog.pg_index WHERE indrelid = 'task_make_test_schema.task_make_test'::regclass;
SELECT count(*) = 1 AS trigger_ok FROM pg_catalog.pg_trigger WHERE tgrelid = 'task_make_test_schema.task_make_test'::regclass AND NOT tgisinternal;
INSERT INTO task_make_test_schema.task_make_test (input) VALUES ('SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task_make_test_schema.task_make_test WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task_make_test_schema.task_make_test;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $$ BEGIN PERFORM pg_sleep(5); END $$;
DROP SCHEMA task_make_test_schema CASCADE;
