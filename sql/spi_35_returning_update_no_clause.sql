DELETE FROM task WHERE "group" = 'returning_update_no_clause';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('returning_update_no_clause', 'UPDATE returning_probe SET val = val + 1 WHERE id = 1');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'returning_update_no_clause' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", output, error, state FROM task WHERE "group" = 'returning_update_no_clause' AND plan > :ct::timestamp;
