-- a worker reused after a failed task must still record the command tag of the next one, rather than leave it without output and so have it deleted
DELETE FROM task WHERE "group" = 'skip_after_error';
DROP TABLE IF EXISTS skip_after_error_test;
INSERT INTO task ("group", max, count, input) VALUES ('skip_after_error', 0, 10, 'SELECT 1/0'), ('skip_after_error', 0, 10, 'CREATE TABLE skip_after_error_test ()'), ('skip_after_error', 0, 10, 'SELECT 42 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'skip_after_error' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''skip_after_error'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) AS tasks, count(DISTINCT pid) = 1 AS one_worker FROM task WHERE "group" = 'skip_after_error';
SELECT input, state, output, error IS NOT NULL AS failed FROM task WHERE "group" = 'skip_after_error' ORDER BY id;
DELETE FROM task WHERE "group" = 'skip_after_error';
DROP TABLE IF EXISTS skip_after_error_test;
