DELETE FROM task WHERE "group" = 'save_true';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, count, save) VALUES ('save_true', 'CREATE TEMP TABLE save_probe_19 AS SELECT 1 AS a', 5, true);
INSERT INTO task ("group", input, count, save) VALUES ('save_true', 'SELECT count(*) FROM save_probe_19', 5, true);
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'save_true' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", count(DISTINCT pid) = 1 AS same_worker, max(output) FILTER (WHERE input LIKE 'SELECT count%') = '1' AS state_preserved
FROM task WHERE "group" = 'save_true' AND plan > :ct::timestamp GROUP BY "group";
