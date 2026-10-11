-- the timeout of a task is capped by the statement_timeout of the server, the database and the role, not by the session's one a task before it set with save = true, which is for the next tasks with no timeout of their own only
DELETE FROM task WHERE "group" = 'save_timeout_cap';
INSERT INTO task ("group", input, count, save, timeout) VALUES
    ('save_timeout_cap', 'SET statement_timeout = 1000', 10, true, '0 sec'),
    ('save_timeout_cap', 'SELECT pg_sleep(2)', 10, true, '5 sec');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'save_timeout_cap' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''save_timeout_cap'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, error FROM task WHERE "group" = 'save_timeout_cap' ORDER BY id;
SELECT count(DISTINCT pid) = 1 AS one_worker FROM task WHERE "group" = 'save_timeout_cap';
DELETE FROM task WHERE "group" = 'save_timeout_cap';
