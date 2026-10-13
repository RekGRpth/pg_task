-- an input failing with FATAL fails its task with that error even if the server logs no FATAL, with log_min_messages = panic: rather than stay in WORK, to run again on every reset, as the log hook that takes the error sees only what goes to the log
DELETE FROM task WHERE "group" = 'fatal_unlogged';
ALTER SYSTEM SET log_min_messages = panic;
SELECT pg_reload_conf();
SELECT pg_sleep(0.5);
INSERT INTO task ("group", input) VALUES ('fatal_unlogged', 'SET exit_on_error = on; SELECT 1/0');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..100 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'fatal_unlogged' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 100 x pg_sleep(0.1) waiting for task group ''fatal_unlogged'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET log_min_messages;
SELECT pg_reload_conf();
SELECT state, error LIKE '%division by zero%' AS its_error FROM task WHERE "group" = 'fatal_unlogged';
DELETE FROM task WHERE "group" = 'fatal_unlogged';
