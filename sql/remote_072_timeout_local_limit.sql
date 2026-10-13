-- the timeout of a remote task is that of its own, set on the remote server, not capped by the statement_timeout of this server, where pg_work runs it from, as a local one is: that of the remote server is in effect for a timeout of 0 only
DELETE FROM task WHERE "group" = 'timeout_local_limit';
SET statement_timeout = 0;
ALTER SYSTEM SET statement_timeout = '1s';
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
INSERT INTO task ("group", timeout, input, remote) VALUES ('timeout_local_limit', '10 sec', 'SELECT pg_sleep(2)', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'timeout_local_limit' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''timeout_local_limit'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET statement_timeout;
SELECT pg_reload_conf();
RESET statement_timeout;
SELECT state, error FROM task WHERE "group" = 'timeout_local_limit';
DELETE FROM task WHERE "group" = 'timeout_local_limit';
