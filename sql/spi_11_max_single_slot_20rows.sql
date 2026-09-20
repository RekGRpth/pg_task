DELETE FROM task WHERE "group" = 'max_single_slot_20rows';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
WITH s AS (SELECT generate_series(1, 20) AS s) INSERT INTO task ("group", input, max, count) SELECT 'max_single_slot_20rows', 'SELECT pg_sleep(1) AS a', 1, 5 FROM s;
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'max_single_slot_20rows' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state, count(id) FROM task WHERE "group" = 'max_single_slot_20rows' AND plan > :ct::timestamp GROUP BY "group", input, output, error, state, pid;
