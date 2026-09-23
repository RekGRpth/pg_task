SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"task_make_data_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'task_make_data_test') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for database ''task_make_data_test'' to be auto-created'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'task_make_data_test') AS database_created;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $$ BEGIN PERFORM pg_sleep(5); END $$;
DROP DATABASE task_make_data_test;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_make_data_test') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for backend(s) connected as role ''task_make_data_test'' to disconnect'; END IF;
END;$body$ LANGUAGE plpgsql;
DROP ROLE task_make_data_test;
