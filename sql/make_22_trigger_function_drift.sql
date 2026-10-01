SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT current_user AS test_user
\gset
-- a table left by an earlier version: a trigger firing on other events (wake_up before it woke pg_work up on DELETE and UPDATE OF plan too), a trigger on another column, and a function with the right body but neither security nor search_path right
CREATE SCHEMA task_trigger_drift_test_schema;
CREATE TABLE task_trigger_drift_test_schema.task_trigger_drift_test ("id" serial8 PRIMARY KEY, "plan" timestamptz);
CREATE FUNCTION task_trigger_drift_test_schema.task_trigger_drift_test_wake_up() RETURNS trigger AS $function$BEGIN RETURN NULL; END;$function$ LANGUAGE plpgsql;
CREATE TRIGGER task_trigger_drift_test_wake_up AFTER INSERT ON task_trigger_drift_test_schema.task_trigger_drift_test FOR EACH STATEMENT EXECUTE PROCEDURE task_trigger_drift_test_schema.task_trigger_drift_test_wake_up();
DO $body$ BEGIN EXECUTE format('CREATE FUNCTION task_trigger_drift_test_schema.task_trigger_drift_test_group() RETURNS trigger SECURITY DEFINER AS %L LANGUAGE plpgsql', (SELECT prosrc FROM pg_catalog.pg_proc WHERE oid = (SELECT tgfoid FROM pg_catalog.pg_trigger WHERE tgrelid = 'task'::regclass AND tgname = 'task_group'))); END;$body$ LANGUAGE plpgsql;
CREATE TRIGGER task_trigger_drift_test_group BEFORE UPDATE OF "plan" ON task_trigger_drift_test_schema.task_trigger_drift_test FOR EACH ROW EXECUTE PROCEDURE task_trigger_drift_test_schema.task_trigger_drift_test_group();
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_trigger_drift_test_schema","table":"task_trigger_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'task_trigger_drift_test_schema.task_trigger_drift_test'::regclass AND attname = 'user' AND NOT attisdropped) AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work task_trigger_drift_test_schema task_trigger_drift_test %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for table task_trigger_drift_test_schema.task_trigger_drift_test to be brought up to date by the pg_work worker'; END IF;
END;$body$ LANGUAGE plpgsql;
-- both triggers fire as the ones of the main task table do, and both functions are as pg_work makes them
SELECT substr(d.tgname, length('task_trigger_drift_test_') + 1) AS trigger, d.tgtype = m.tgtype AS type_healed, (SELECT string_agg(attname::text, ',' ORDER BY attname) FROM pg_catalog.pg_attribute WHERE attrelid = d.tgrelid AND attnum = ANY (d.tgattr::int2[])) AS columns, (SELECT string_agg(attname::text, ',' ORDER BY attname) FROM pg_catalog.pg_attribute WHERE attrelid = d.tgrelid AND attnum = ANY (d.tgattr::int2[])) IS NOT DISTINCT FROM (SELECT string_agg(attname::text, ',' ORDER BY attname) FROM pg_catalog.pg_attribute WHERE attrelid = m.tgrelid AND attnum = ANY (m.tgattr::int2[])) AS columns_healed FROM pg_catalog.pg_trigger d JOIN pg_catalog.pg_trigger m ON m.tgrelid = 'task'::regclass AND m.tgname = 'task_' || substr(d.tgname, length('task_trigger_drift_test_') + 1) WHERE d.tgrelid = 'task_trigger_drift_test_schema.task_trigger_drift_test'::regclass AND d.tgname IN ('task_trigger_drift_test_wake_up', 'task_trigger_drift_test_group') ORDER BY 1;
SELECT proname, prosecdef, proconfig FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE nspname = 'task_trigger_drift_test_schema' AND proname IN ('task_trigger_drift_test_wake_up', 'task_trigger_drift_test_group') ORDER BY 1;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_trigger_drift_test_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker(s) matching ''pg_work task_trigger_drift_test_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS task_trigger_drift_test_schema CASCADE;
RESET client_min_messages;
