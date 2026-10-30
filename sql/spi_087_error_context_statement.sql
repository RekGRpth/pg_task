-- the statement of the error context SPI adds, SQL statement "...", is the statement itself, with no ; that ends it nor what stands between it and the next statement
DELETE FROM task WHERE "group" = 'error_context_statement';
INSERT INTO task ("group", max, "delete", input) VALUES
    ('error_context_statement', 0, false, 'SELECT 1; SELECT 1 / 0 ;   SELECT 2'),
    ('error_context_statement', 0, false, 'SELECT 1 / 0;'),
    ('error_context_statement', 0, false, E'SELECT 1 / 0;\n\n'),
    ('error_context_statement', 0, false, 'DO $$ BEGIN PERFORM 1 / 0; END $$;');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'error_context_statement' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''error_context_statement'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, error FROM task WHERE "group" = 'error_context_statement' ORDER BY id;
DELETE FROM task WHERE "group" = 'error_context_statement';
