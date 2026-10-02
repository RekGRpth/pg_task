-- a hash column of the user's own is left alone, with its data, when pg_work starts: only the hash column of earlier versions of pg_task is dropped
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT current_user AS test_user
\gset
ALTER SYSTEM SET pg_work.restart = 2;
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"own_hash_test_schema","table":"own_hash_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work own_hash_test_schema own_hash_test %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work of own_hash_test_schema.own_hash_test to be idle'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER TABLE own_hash_test_schema.own_hash_test ADD COLUMN hash text DEFAULT 'mine';
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work own_hash_test_schema own_hash_test %' AND datname = current_database()
\gset
SELECT set_config('pg_task_test.work_pid', :'work_pid', false) IS NOT NULL AS work_pid_saved;
SELECT pg_terminate_backend(:work_pid) AS work_terminated;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work own_hash_test_schema own_hash_test %' AND datname = current_database() AND pid <> current_setting('pg_task_test.work_pid')::int AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work of own_hash_test_schema.own_hash_test to be idle'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = 'own_hash_test_schema.own_hash_test'::regclass AND attname = 'hash' AND NOT attisdropped) AS hash_column_kept;
INSERT INTO own_hash_test_schema.own_hash_test (input) VALUES ('SELECT 1 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM own_hash_test_schema.own_hash_test WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, hash FROM own_hash_test_schema.own_hash_test;
ALTER SYSTEM RESET pg_work.restart;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work own_hash_test_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker(s) matching ''pg_work own_hash_test_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS own_hash_test_schema CASCADE;
RESET client_min_messages;
