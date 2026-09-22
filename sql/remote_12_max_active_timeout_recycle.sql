DELETE FROM task WHERE "group" = 'max_active_timeout_recycle';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active, remote) SELECT 'max_active_timeout_recycle', 'SELECT pg_sleep(10) AS a', 1, 5, '5 sec', 'dbname=' || :'DBNAME' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'max_active_timeout_recycle' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for task group ''max_active_timeout_recycle'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = 'max_active_timeout_recycle' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
