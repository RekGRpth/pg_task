-- a remote task counts against max from TAKE on, not once its connection is up: slow the connections of pg_task down with a login event trigger (PostgreSQL 17+), so that work_sleep() runs while they are still coming up
SELECT current_setting('server_version_num')::int >= 170000 AS slow_login;
DO $body$ BEGIN
    IF current_setting('server_version_num')::int >= 170000 THEN
        EXECUTE $sql$CREATE FUNCTION pg_task_test_slow_login() RETURNS event_trigger LANGUAGE plpgsql AS $function$ BEGIN IF current_setting('application_name') LIKE 'pg_task %' THEN PERFORM pg_sleep(0.3); END IF; END; $function$$sql$;
        EXECUTE $sql$CREATE EVENT TRIGGER pg_task_test_slow_login ON login EXECUTE FUNCTION pg_task_test_slow_login()$sql$;
    END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = 'max_slow_connect';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, remote) SELECT 'max_slow_connect', 'SELECT pg_sleep(0.3) AS a', 1, 'dbname=' || :'DBNAME' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'max_slow_connect' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''max_slow_connect'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", output, error, state, count(id) FROM task WHERE "group" = 'max_slow_connect' AND plan > :ct::timestamp GROUP BY "group", output, error, state;
-- max = 1: never more than two of them running at once
SELECT max(c) <= 2 AS within_max FROM (SELECT (SELECT count(*) FROM task AS b WHERE b."group" = a."group" AND b.start <= a.start AND a.start < b.stop) AS c FROM task AS a WHERE a."group" = 'max_slow_connect' AND a.plan > :ct::timestamp) AS s;
DELETE FROM task WHERE "group" = 'max_slow_connect';
DO $body$ BEGIN
    IF current_setting('server_version_num')::int >= 170000 THEN
        EXECUTE 'DROP EVENT TRIGGER pg_task_test_slow_login';
        EXECUTE 'DROP FUNCTION pg_task_test_slow_login()';
    END IF;
END;$body$ LANGUAGE plpgsql;
