-- the bookkeeping of a task doesn't go by the transaction characteristics its input or its author's role set for the session, which would fail it, and have the task run again on every reset: a read only session, a serializable one, and a role whose transactions are read only by default all get their task DONE, run once
DELETE FROM task WHERE "group" LIKE 'session_settings_%';
SET client_min_messages = warning;
DROP TABLE IF EXISTS session_settings_probe;
DROP ROLE IF EXISTS task_read_only_author;
RESET client_min_messages;
CREATE TABLE session_settings_probe (i int);
CREATE ROLE task_read_only_author LOGIN;
ALTER ROLE task_read_only_author SET default_transaction_read_only = on;
\set remote NULL
INSERT INTO task ("group", remote, input, "user") VALUES
    ('session_settings_read_only', :remote, 'INSERT INTO session_settings_probe VALUES (1); SET default_transaction_read_only = on', current_user),
    ('session_settings_serializable', :remote, 'SET default_transaction_isolation = serializable; SELECT 1 AS a', current_user),
    ('session_settings_role', :remote, 'SELECT 1 AS a', 'task_read_only_author');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'session_settings_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task groups ''session_settings_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state FROM task WHERE "group" LIKE 'session_settings_%' ORDER BY "group";
SELECT count(*) AS input_runs FROM session_settings_probe;
DELETE FROM task WHERE "group" LIKE 'session_settings_%';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_read_only_author') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for backend(s) connected as role ''task_read_only_author'' to disconnect'; END IF;
END;$body$ LANGUAGE plpgsql;
DROP TABLE session_settings_probe;
DROP ROLE task_read_only_author;
