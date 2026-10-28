-- an error in the syntax of the body of a function, of a DO or of a CREATE FUNCTION, one of several statements of the input or alone, is at its position within the whole input, as remote mode reports it too, rather than within the body, the body as the query
DELETE FROM task WHERE "group" = 'body_error_position';
INSERT INTO task (id, "group", input) VALUES
    (-1071, 'body_error_position', $q$SELECT 1; DO $$BEGIN PERFORM 1 +; END$$$q$),
    (-1072, 'body_error_position', $q$SELECT 1;  CREATE FUNCTION body_error_position() RETURNS int LANGUAGE sql AS $$SELECT 1 +$$$q$),
    (-1073, 'body_error_position', $q$DO $$BEGIN PERFORM 1 +; END$$$q$);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'body_error_position' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group body_error_position to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT -id AS n, state, error FROM task WHERE "group" = 'body_error_position' ORDER BY id DESC;
DELETE FROM task WHERE "group" = 'body_error_position';
