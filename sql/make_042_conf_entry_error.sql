-- an entry of pg_task.json that can't be started, here as its role can't be made (public is a reserved name), takes neither pg_conf down, to be restarted into the same error over and over, nor the entries beside it
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT pid AS conf_pid FROM pg_catalog.pg_stat_activity WHERE application_name = 'pg_conf'
\gset
SELECT set_config('pg_task_test.conf_pid', :'conf_pid', false) IS NOT NULL AS conf_pid_saved;
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"public","schema":"conf_error_bad"},{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"conf_error_good"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'conf_error_good' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work conf_error_good task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of conf_error_good.task to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
-- a reload more, to try the bad entry again
SELECT pg_reload_conf();
SELECT pg_sleep(2);
SELECT pg_stat_clear_snapshot();
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = 'pg_conf' AND pid = current_setting('pg_task_test.conf_pid')::int) AS same_pg_conf;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work conf_error_good task %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of conf_error_good.task to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA conf_error_good CASCADE;
RESET client_min_messages;
