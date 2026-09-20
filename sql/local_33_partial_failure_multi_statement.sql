DELETE FROM task WHERE "group" = 'partial_failure';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('partial_failure', 'BEGIN; SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'partial_failure' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task WHERE "group" = 'partial_failure' AND plan > :ct::timestamp;
