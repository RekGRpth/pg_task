DELETE FROM task WHERE "group" = 'empty_input';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
-- no statement at all: nothing to run, so no output, the same as in local and remote mode
INSERT INTO task ("group", input, "delete") VALUES ('empty_input', ';', false), ('empty_input', '', false);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'empty_input' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''empty_input'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT quote_literal(input) AS input, output, error, state FROM task WHERE "group" = 'empty_input' AND plan > :ct::timestamp ORDER BY id;
DELETE FROM task WHERE "group" = 'empty_input';
