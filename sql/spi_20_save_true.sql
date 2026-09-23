DELETE FROM task WHERE "group" = 'save_true';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, count, save) VALUES ('save_true', 'CREATE TEMP TABLE save_probe_19 (a int); INSERT INTO save_probe_19 VALUES (1)', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('save_true', 'SELECT count(*) FROM save_probe_19', 5, true);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'save_true' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''save_true'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) = 1 AS same_worker, max(output) FILTER (WHERE input LIKE 'SELECT count%') = '1' AS state_preserved
FROM task WHERE "group" = 'save_true' AND plan > :ct::timestamp GROUP BY "group";
