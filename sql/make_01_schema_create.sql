SELECT current_setting('pg_task.json') AS json_baseline
\gset
DELETE FROM pg_task_test_state WHERE key = 'json_baseline';
INSERT INTO pg_task_test_state VALUES ('json_baseline', :'json_baseline');
SELECT current_user AS test_user
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_make_test_schema","table":"task_make_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'task_make_test_schema' AND c.relname = 'task_make_test') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work task_make_test_schema task_make_test %' AND datname = current_database() AND state = 'idle' AND (current_setting('server_version_num')::int < 100000 OR to_jsonb(a) ->> 'wait_event_type' = 'Extension')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for table task_make_test_schema.task_make_test to be created by the pg_work worker'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('task_make_test_schema.task_make_test') IS NOT NULL AS table_created;
