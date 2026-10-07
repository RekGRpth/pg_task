-- with save, the search_path a failed input committed itself (SET search_path = ...; COMMIT; SELECT 1/0) is kept for the next task of the worker, as its statement_timeout is, and as on a remote connection, rather than taken back to that of the task before
DELETE FROM task WHERE "group" = 'save_search_path_failed';
CREATE SCHEMA IF NOT EXISTS save_search_path_failed;
INSERT INTO task ("group", save, count, input) VALUES ('save_search_path_failed', true, 10, 'SET search_path = save_search_path_failed; COMMIT; SELECT 1/0'), ('save_search_path_failed', true, 10, 'SHOW search_path');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'save_search_path_failed' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''save_search_path_failed'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, replace(output, E'\n', '|') AS output, pid = (SELECT min(pid) FROM task WHERE "group" = 'save_search_path_failed') AS same_worker FROM task WHERE "group" = 'save_search_path_failed' ORDER BY id;
DELETE FROM task WHERE "group" = 'save_search_path_failed';
DROP SCHEMA save_search_path_failed;
