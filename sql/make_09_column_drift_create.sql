SELECT current_setting('pg_task.json') AS json_baseline
\gset
DELETE FROM pg_task_test_state WHERE key = 'json_baseline';
INSERT INTO pg_task_test_state VALUES ('json_baseline', :'json_baseline');
SELECT current_user AS test_user
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_column_drift_test_schema","table":"task_column_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'task_column_drift_test_schema' AND c.relname = 'task_column_drift_test') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for table task_column_drift_test_schema.task_column_drift_test to be created by the pg_work worker'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_column_drift_test_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for pg_work worker(s) matching ''pg_work task_column_drift_test_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
