-- a remote task that finds no file descriptor left for its connection, every one pg_work may hold for others than files (a third of max_files_per_process at most, see test.conf) taken by the other remote tasks, goes back to PLAN, to be run once one of them is done, rather than fail
DELETE FROM task WHERE "group" LIKE 'fd_budget_%';
INSERT INTO task ("group", input, remote) SELECT 'fd_budget_' || i, 'SELECT pg_sleep(1)', 'dbname=' || :'DBNAME' FROM generate_series(1, 30) AS i;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..600 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'fd_budget_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 600 x pg_sleep(0.1) waiting for task groups ''fd_budget_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, count(*), error FROM task WHERE "group" LIKE 'fd_budget_%' GROUP BY state, error ORDER BY state;
DELETE FROM task WHERE "group" LIKE 'fd_budget_%';
