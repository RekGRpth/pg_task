-- a database made from another as its template has a task table of the same oid, and the same ids: the locks of their tasks and groups tell them apart by the database too, rather than take the task of the one for that of the other, left in TAKE
SET client_min_messages = warning;
DROP DATABASE IF EXISTS task_template_a;
DROP DATABASE IF EXISTS task_template_b;
RESET client_min_messages;
CREATE DATABASE task_template_a;
SELECT current_setting('pg_task.json') AS json_baseline, current_user AS test_user, current_database() AS test_database
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"task_template_a","user":"' || :'test_user' || '"}]' AS json_a, left(:'json_baseline', -1) || ',{"data":"task_template_a","user":"' || :'test_user' || '"},{"data":"task_template_b","user":"' || :'test_user' || '"}]' AS json_ab
\gset
ALTER SYSTEM SET pg_task.json = :'json_a';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work public task %' AND datname = 'task_template_a' AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of task_template_a to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname = 'task_template_a') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task_template_a to be left'; END IF;
END;$body$ LANGUAGE plpgsql;
CREATE DATABASE task_template_b TEMPLATE task_template_a;
ALTER SYSTEM SET pg_task.json = :'json_ab';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF (SELECT count(*) FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work public task %' AND datname IN ('task_template_a', 'task_template_b') AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) = 2 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work workers of task_template_a and task_template_b to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
\c task_template_a
INSERT INTO task (input) VALUES ('SELECT pg_sleep(2)') RETURNING id;
\c task_template_b
INSERT INTO task (input) VALUES ('SELECT pg_sleep(2)') RETURNING id;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of task_template_b to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id, state FROM task;
\c task_template_a
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of task_template_a to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id, state FROM task;
\c :test_database
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname IN ('task_template_a', 'task_template_b')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task_template_a and task_template_b to be left'; END IF;
END;$body$ LANGUAGE plpgsql;
DROP DATABASE task_template_a;
DROP DATABASE task_template_b;
