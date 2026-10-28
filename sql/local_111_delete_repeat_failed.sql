-- a task to be deleted once done with no output nor error, of a repeat whose insert fails, a trigger refusing it say, is kept, done, rather than deleted with no trace of it, its repeats ended for good with a warning only
DELETE FROM task WHERE "group" = 'delete_repeat_failed';
CREATE FUNCTION delete_repeat_failed() RETURNS trigger LANGUAGE plpgsql AS $function$BEGIN
    RAISE EXCEPTION 'no repeat of the group delete_repeat_failed';
END;$function$;
CREATE TRIGGER delete_repeat_failed BEFORE INSERT ON task FOR EACH ROW WHEN (NEW."group" = 'delete_repeat_failed' AND NEW.parent IS NOT NULL) EXECUTE PROCEDURE delete_repeat_failed();
INSERT INTO task ("group", input, repeat, delete) VALUES ('delete_repeat_failed', 'SELECT 1 WHERE false', '1 hour', true);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'delete_repeat_failed' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group delete_repeat_failed to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
-- and one whose repeat is inserted, deleted as before
DROP TRIGGER delete_repeat_failed ON task;
INSERT INTO task ("group", input, repeat, delete) VALUES ('delete_repeat_failed', 'SELECT 2 WHERE false', '1 hour', true);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'delete_repeat_failed' AND state NOT IN ('DONE', 'GONE', 'FAIL') AND plan < statement_timestamp() + '1 min') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the second task of group delete_repeat_failed to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, parent IS NOT NULL AS repeat FROM task WHERE "group" = 'delete_repeat_failed' ORDER BY id;
DELETE FROM task WHERE "group" = 'delete_repeat_failed';
DROP FUNCTION delete_repeat_failed();
