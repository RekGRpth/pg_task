-- a trigger of the owner of the table failing on the update of the state of the bookkeeping, one only on DONE, or a NOTIFY of an output too long for its payload, records the task as failed by its error, with no output, rather than leaving it in WORK, to run again on every reset, its input done each time
DELETE FROM task WHERE "group" LIKE 'bookkeeping_trigger_%';
SET client_min_messages = warning;
DROP TABLE IF EXISTS bookkeeping_trigger_runs;
RESET client_min_messages;
CREATE TABLE bookkeeping_trigger_runs (g text);
CREATE FUNCTION bookkeeping_trigger_fail() RETURNS trigger LANGUAGE plpgsql AS $function$BEGIN
    RAISE EXCEPTION 'a trigger of the owner failing';
END;$function$;
CREATE FUNCTION bookkeeping_trigger_notify() RETURNS trigger LANGUAGE plpgsql AS $function$BEGIN
    PERFORM pg_notify('bookkeeping_trigger', COALESCE(NEW.output, ''));
    RETURN NULL;
END;$function$;
CREATE TRIGGER bookkeeping_trigger_fail AFTER UPDATE OF state ON task FOR EACH ROW WHEN (NEW."group" = 'bookkeeping_trigger_done' AND NEW.state = 'DONE') EXECUTE PROCEDURE bookkeeping_trigger_fail();
CREATE TRIGGER bookkeeping_trigger_notify AFTER UPDATE OF state ON task FOR EACH ROW WHEN (NEW."group" = 'bookkeeping_trigger_notify') EXECUTE PROCEDURE bookkeeping_trigger_notify();
\set remote NULL
INSERT INTO task ("group", input, remote) VALUES
    ('bookkeeping_trigger_done', $q$INSERT INTO bookkeeping_trigger_runs VALUES ('done')$q$, :remote),
    ('bookkeeping_trigger_notify', $q$INSERT INTO bookkeeping_trigger_runs VALUES ('notify'); SELECT repeat('x', 9000)$q$, :remote);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'bookkeeping_trigger_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of groups ''bookkeeping_trigger_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, output IS NULL AS no_output, split_part(error, E'\n', 1) AS error, (SELECT count(*) FROM bookkeeping_trigger_runs WHERE g = substr("group", 21)) AS runs FROM task WHERE "group" LIKE 'bookkeeping_trigger_%' ORDER BY "group";
DROP TRIGGER bookkeeping_trigger_fail ON task;
DROP TRIGGER bookkeeping_trigger_notify ON task;
DROP FUNCTION bookkeeping_trigger_fail();
DROP FUNCTION bookkeeping_trigger_notify();
DROP TABLE bookkeeping_trigger_runs;
DELETE FROM task WHERE "group" LIKE 'bookkeeping_trigger_%';
