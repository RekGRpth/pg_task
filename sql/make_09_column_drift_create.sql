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
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF to_regclass('task_column_drift_test_schema.task_column_drift_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_column_drift_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
