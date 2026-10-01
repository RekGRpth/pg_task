-- the task's bookkeeping runs as pg_task.user with an empty search_path, after a failed task too, rather than with the author's one, which the author's own functions could shadow the ones of a trigger on the task table with
DELETE FROM task WHERE "group" = 'search_path_after_error';
CREATE TABLE search_path_after_error_log (input text, state text, search_path text);
CREATE FUNCTION search_path_after_error_log() RETURNS trigger AS $function$BEGIN INSERT INTO public.search_path_after_error_log VALUES (NEW.input, NEW.state, pg_catalog.current_setting('search_path')); RETURN NULL; END;$function$ LANGUAGE plpgsql;
CREATE TRIGGER search_path_after_error_log AFTER UPDATE OF state ON task FOR EACH ROW WHEN (NEW."group" = 'search_path_after_error' AND NEW.state IN ('DONE', 'FAIL')) EXECUTE PROCEDURE search_path_after_error_log();
INSERT INTO task ("group", input) VALUES ('search_path_after_error', 'SELECT 1'), ('search_path_after_error', 'SELECT 1/0');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'search_path_after_error' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''search_path_after_error'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, quote_literal(search_path) AS search_path FROM search_path_after_error_log ORDER BY input;
DROP TRIGGER search_path_after_error_log ON task;
DROP FUNCTION search_path_after_error_log();
DROP TABLE search_path_after_error_log;
DELETE FROM task WHERE "group" = 'search_path_after_error';
