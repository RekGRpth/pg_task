SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT current_user AS test_user
\gset
-- an idle pg_work waits until the next planned task, which more than about 24.8 days ahead is more than the wait takes: it must keep running rather than abort (or wait forever)
ALTER SYSTEM SET pg_work.idle = 1;
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_idle_timeout_test_schema","table":"task_idle_timeout_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'task_idle_timeout_test_schema' AND c.relname = 'task_idle_timeout_test') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work task_idle_timeout_test_schema task_idle_timeout_test %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for table task_idle_timeout_test_schema.task_idle_timeout_test to be created by the pg_work worker'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_idle_timeout_test_schema task_idle_timeout_test %' AND datname = current_database()
\gset
INSERT INTO task_idle_timeout_test_schema.task_idle_timeout_test (plan, input) VALUES (now() + '30 days', 'SELECT 1');
SELECT pg_sleep(3);
SELECT pg_stat_clear_snapshot();
SELECT count(*) = 1 AS same_work_still_running FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_idle_timeout_test_schema task_idle_timeout_test %' AND datname = current_database() AND pid = :work_pid;
ALTER SYSTEM RESET pg_work.idle;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_idle_timeout_test_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker(s) matching ''pg_work task_idle_timeout_test_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS task_idle_timeout_test_schema CASCADE;
RESET client_min_messages;
