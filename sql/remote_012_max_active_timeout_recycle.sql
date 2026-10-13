DELETE FROM task WHERE "group" = 'max_active_timeout_recycle';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 10) AS s) INSERT INTO task ("group", input, max, count, active, remote) SELECT 'max_active_timeout_recycle', 'SELECT pg_sleep(4) AS a', 1, 5, '2 sec', 'dbname=' || :'DBNAME' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'max_active_timeout_recycle' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''max_active_timeout_recycle'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = 'max_active_timeout_recycle' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state ORDER BY 6;
