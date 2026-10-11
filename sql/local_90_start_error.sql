-- a task whose start fails on an error of the worker's own, a trigger of the table refusing its WORK say, is left in TAKE for the reset, rather than given back to PLAN on the way out and started anew on every pass, failing as often
CREATE SEQUENCE start_error_seq;
CREATE FUNCTION start_error_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state = 'WORK' AND NEW."group" = 'start_error' THEN PERFORM pg_catalog.nextval('public.start_error_seq'); RAISE EXCEPTION 'start_error refused'; END IF; RETURN NEW; END $$;
CREATE TRIGGER start_error_refuse BEFORE UPDATE OF state ON task FOR EACH ROW EXECUTE PROCEDURE start_error_refuse();
INSERT INTO task ("group", input) VALUES ('start_error', 'SELECT 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT last_value FROM start_error_seq) > 0 AND (SELECT is_called FROM start_error_seq) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the start of the task of group start_error to fail'; END IF;
END;$body$ LANGUAGE plpgsql;
-- some passes of pg_work, of pg_task.sleep each, for it to start the task again, were it given back
SELECT pg_sleep(2);
SELECT state, (SELECT last_value FROM start_error_seq) AS attempts FROM task WHERE "group" = 'start_error';
DROP TRIGGER start_error_refuse ON task;
DROP FUNCTION start_error_refuse();
DROP SEQUENCE start_error_seq;
DELETE FROM task WHERE "group" = 'start_error';
