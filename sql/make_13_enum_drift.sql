SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT current_user AS test_user
\gset
CREATE SCHEMA task_enum_drift_test_schema;
CREATE TYPE task_enum_drift_test_schema.state AS ENUM ('PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL');
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_enum_drift_test_schema","table":"task_enum_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF to_regclass('task_enum_drift_test_schema.task_enum_drift_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT array_agg(enumlabel::text ORDER BY enumsortorder) = ARRAY['PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP'] AS enum_healed FROM pg_catalog.pg_enum WHERE enumtypid = 'task_enum_drift_test_schema.state'::regtype;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_enum_drift_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS task_enum_drift_test_schema CASCADE;
RESET client_min_messages;
