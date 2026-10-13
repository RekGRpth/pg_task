DELETE FROM task WHERE "group" = 'returning_update_with_clause';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('returning_update_with_clause', 'UPDATE returning_probe SET val = val + 1 WHERE id = 2 RETURNING id');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..150 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'returning_update_with_clause' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 150 x pg_sleep(0.1) waiting for task group ''returning_update_with_clause'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", output, error, state FROM task WHERE "group" = 'returning_update_with_clause' AND plan > :ct::timestamp;
