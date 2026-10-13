-- a remote task runs in pg_work, which keeps its own application_name meanwhile, to be found by, rather than take that of the task, which its connection has already, on the remote server
DELETE FROM task WHERE "group" = 'appname';
INSERT INTO task ("group", input, remote) VALUES ('appname', 'SELECT pg_sleep(3)', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'appname' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task in group ''appname'' to start'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_sleep(0.5);
SELECT pg_stat_clear_snapshot();
SELECT (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()) AS works, (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name = 'pg_task public task appname' AND datname = current_database()) AS tasks;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'appname' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''appname'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state FROM task WHERE "group" = 'appname';
DELETE FROM task WHERE "group" = 'appname';
