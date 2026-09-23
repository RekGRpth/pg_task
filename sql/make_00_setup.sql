SELECT current_setting('pg_task.json') AS base_json
\gset
SELECT :'base_json' NOT LIKE '%"data":"' || :'DBNAME' || '"%' AS base_json_added
\gset
SELECT CASE WHEN :'base_json_added' = 't' THEN left(:'base_json', -1) || ',{"data":"' || :'DBNAME' || '"}]' ELSE :'base_json' END AS json_val
\gset
CREATE TABLE pg_task_test_state (key text PRIMARY KEY, value text);
INSERT INTO pg_task_test_state VALUES ('base_json', :'base_json'), ('base_json_added', :'base_json_added');
ALTER SYSTEM SET pg_work.restart = 1;
ALTER SYSTEM SET pg_task.sleep = 100;
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database() AND state = 'idle') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_task extension to create public.task and its pg_work worker to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
