-- an int setting of a database or a role the setting takes, but a cast doesn't (1e2 from 12 on, 0x64 before 16), is parsed as the setting parses it, rather than fail the query of pg_conf for every entry of pg_task.json, no pg_work started for any of them
SELECT current_setting('pg_task.json') AS json_baseline, current_user AS test_user, CASE WHEN current_setting('server_version_num')::int >= 120000 THEN '1e2' ELSE '0x64' END AS sleep_value
\gset
ALTER ROLE :"test_user" IN DATABASE :"DBNAME" SET pg_task.sleep = :'sleep_value';
SELECT left(:'json_baseline', -1) || ',' || json_build_object('data', :'DBNAME', 'user', :'test_user', 'schema', 'settings_parse_schema')::text || ']' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'settings_parse_schema' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work settings_parse_schema task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of settings_parse_schema.task'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_sleep(1);
INSERT INTO settings_parse_schema.task (input) VALUES ('SELECT 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM settings_parse_schema.task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of settings_parse_schema.task to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output FROM settings_parse_schema.task;
ALTER ROLE :"test_user" IN DATABASE :"DBNAME" RESET pg_task.sleep;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work settings_parse_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of settings_parse_schema to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA settings_parse_schema CASCADE;
RESET client_min_messages;
