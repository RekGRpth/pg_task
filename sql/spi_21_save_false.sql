DELETE FROM task WHERE "group" = 'save_false';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, count, save) VALUES ('save_false', 'CREATE TEMP TABLE save_probe_20 (a int); INSERT INTO save_probe_20 VALUES (1)', 5, false);
INSERT INTO task ("group", input, count, save) VALUES ('save_false', 'SELECT count(*) FROM save_probe_20', 5, false);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'save_false' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for task group ''save_false'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", bool_and(CASE WHEN input LIKE 'CREATE%' THEN state = 'DONE' ELSE state = 'FAIL' END) AS discard_worked, count(DISTINCT pid) = 1 AS same_worker
FROM task WHERE "group" = 'save_false' AND plan > :ct::timestamp GROUP BY "group";
