DELETE FROM task WHERE "group" = 'max_single_slot_20rows';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count, remote) SELECT 'max_single_slot_20rows', 'SELECT pg_sleep(1) AS a', 1, 5, 'dbname=' || :'DBNAME' FROM s;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'max_single_slot_20rows' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''max_single_slot_20rows'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = 'max_single_slot_20rows' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
