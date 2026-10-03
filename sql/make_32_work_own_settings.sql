-- pg_work's session got the settings of its role on connecting, which a reload doesn't override once reset there: its fallback, as pg_conf's, takes the server's configuration files instead, even when pg_task.user isn't a superuser (spi tells: a COMMIT task fails in spi mode, but not in local one)
SET client_min_messages = warning;
CREATE ROLE task_work_own_svc LOGIN;
RESET client_min_messages;
GRANT CREATE ON DATABASE :"DBNAME" TO task_work_own_svc;
ALTER ROLE task_work_own_svc SET pg_task.spi = on;
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"task_work_own_svc","schema":"task_work_own_schema"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'task_work_own_schema' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work task_work_own_schema task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of task_work_own_schema.task to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task_work_own_schema.task ("user", input) VALUES ('task_work_own_svc', 'COMMIT');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task_work_own_schema.task WHERE state NOT IN ('DONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task in task_work_own_schema.task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state AS state_in_spi FROM task_work_own_schema.task;
DELETE FROM task_work_own_schema.task;
ALTER ROLE task_work_own_svc RESET pg_task.spi;
SELECT pg_reload_conf();
SELECT pg_sleep(2);
INSERT INTO task_work_own_schema.task ("user", input) VALUES ('task_work_own_svc', 'COMMIT');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task_work_own_schema.task WHERE state NOT IN ('DONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task in task_work_own_schema.task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
-- before 9.5 there is no pg_file_settings to fall back to
SELECT state = 'DONE' OR current_setting('server_version_num')::int < 90500 AS local_once_reset FROM task_work_own_schema.task;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_work_own_svc' OR application_name LIKE 'pg_work task_work_own_schema task %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task_work_own_svc sessions to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA task_work_own_schema CASCADE;
RESET client_min_messages;
REVOKE CREATE ON DATABASE :"DBNAME" FROM task_work_own_svc;
DROP ROLE task_work_own_svc;
