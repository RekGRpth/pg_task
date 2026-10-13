-- the self-provisioning takes in no schema someone else owns either, as its owner may drop the task table of pg_task in it for one of its own, whose triggers then run as pg_task.user: pg_work refuses it, unless owned by pg_task.user or a superuser, and takes it in once it is
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS untrusted_schema CASCADE;
DROP ROLE IF EXISTS task_schema_owner;
RESET client_min_messages;
CREATE ROLE task_schema_owner;
CREATE SCHEMA untrusted_schema AUTHORIZATION task_schema_owner;
SELECT current_setting('pg_task.json') AS json_baseline, current_user AS test_user
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"untrusted_schema"}]' AS json_val
\gset
ALTER SYSTEM SET pg_work.restart = 1;
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
SELECT pg_sleep(3);
SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'untrusted_schema') AS schema_left_alone;
ALTER SCHEMA untrusted_schema OWNER TO :"test_user";
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'untrusted_schema' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work untrusted_schema task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of untrusted_schema.task to become idle, its schema owned by pg_task.user now'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET pg_work.restart;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work untrusted_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of untrusted_schema to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA untrusted_schema CASCADE;
RESET client_min_messages;
DROP ROLE task_schema_owner;
