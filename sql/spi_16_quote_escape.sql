DELETE FROM task WHERE "group" = 'quote_escape';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, quote, escape) VALUES ('quote_escape', $task$SELECT 'a' || '"' || 'b' || chr(92) || 'c' AS a$task$, '"', '\');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'quote_escape' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, output, error, state FROM task WHERE "group" = 'quote_escape' AND plan > :ct::timestamp;
