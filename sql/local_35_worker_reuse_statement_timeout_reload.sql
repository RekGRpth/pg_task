DELETE FROM task WHERE "group" = 'worker_reuse_timeout_reload';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 16) AS s) INSERT INTO task ("group", input, live, timeout) SELECT 'worker_reuse_timeout_reload', 'SELECT pg_sleep(0.3) AS a', '5 sec', '10 sec' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'worker_reuse_timeout_reload' AND state = 'DONE') >= 1 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 15 x pg_sleep(1) waiting for at least 1 DONE task(s) in group ''worker_reuse_timeout_reload'''; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET statement_timeout = '150ms';
SELECT pg_reload_conf();
SET statement_timeout = 0;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'worker_reuse_timeout_reload' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 15 x pg_sleep(1) waiting for task group ''worker_reuse_timeout_reload'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET statement_timeout;
SELECT pg_reload_conf();
RESET statement_timeout;
SELECT EXISTS (
    SELECT 1 FROM task a JOIN task b ON a.pid = b.pid
    WHERE a."group" = 'worker_reuse_timeout_reload' AND b."group" = 'worker_reuse_timeout_reload' AND a.plan > :ct::timestamp AND b.plan > :ct::timestamp
    AND a.state = 'DONE' AND b.state = 'FAIL' AND b.error LIKE '%statement timeout%'
) AS reused_worker_saw_reload;
