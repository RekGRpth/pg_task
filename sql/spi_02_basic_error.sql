DELETE FROM task WHERE "group" = 'basic_error';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('basic_error', 'SELECT 1/0 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'basic_error' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for task group ''basic_error'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = 'basic_error' AND plan > :ct::timestamp;
