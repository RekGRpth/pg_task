DELETE FROM task WHERE "group" = 'returning_insert_on_conflict';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input) VALUES ('returning_insert_on_conflict', 'INSERT INTO returning_probe (id, val) VALUES (1, 1) ON CONFLICT (id) DO NOTHING RETURNING id');
DO $body$ BEGIN
    FOR i IN 1..15 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'returning_insert_on_conflict' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", output, error, state FROM task WHERE "group" = 'returning_insert_on_conflict' AND plan > :ct::timestamp;
