-- a task done frees a slot of its group for the next one, which waits for it already due, and which an idle pg_work, waiting only for tasks planned ahead, would leave waiting with no pass to take it: remote tasks done with a COMMIT of their own, which ends them after the pass their result woke pg_work for, and local ones alike (pg_work.idle of the entry's role set to 1, for it to go idle after a single empty pass)
SET client_min_messages = warning;
CREATE ROLE task_idle_slot SUPERUSER LOGIN;
RESET client_min_messages;
ALTER ROLE task_idle_slot SET pg_work.idle = 1;
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"task_idle_slot","schema":"idle_slot_schema"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'idle_slot_schema' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work idle_slot_schema task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of idle_slot_schema.task to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO idle_slot_schema.task ("group", max, remote, input) VALUES
    ('remote', 0, :remote, 'BEGIN; SELECT pg_sleep(2)'),
    ('remote', 0, :remote, 'SELECT 1 AS a'),
    ('local', 0, NULL, 'SELECT pg_sleep(2)'),
    ('local', 0, NULL, 'SELECT 1 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..150 LOOP
        IF NOT EXISTS (SELECT 1 FROM idle_slot_schema.task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 150 x pg_sleep(0.1) waiting for the tasks of idle_slot_schema.task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, state FROM idle_slot_schema.task ORDER BY id;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_idle_slot' OR application_name LIKE 'pg_task idle_slot_schema task %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for backend(s) of the entry of idle_slot_schema.task to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA idle_slot_schema CASCADE;
RESET client_min_messages;
DROP ROLE task_idle_slot;
