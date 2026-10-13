-- a remote task that finds no file descriptor left for its connection, every one pg_work may hold for others than files (a third of max_files_per_process at most, see test.conf) taken by the other remote tasks, goes back to PLAN, to be run once one of them is done, rather than fail
DELETE FROM task WHERE "group" LIKE 'fd_budget_%';
-- pg_work, not to be taken down meanwhile
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()
\gset
INSERT INTO task ("group", input, remote) SELECT 'fd_budget_' || i, 'SELECT pg_sleep(1)', 'dbname=' || :'DBNAME' FROM generate_series(1, 30) AS i;
-- and local tasks among them, whose task workers pg_work waits to start with WaitLatch(), which on 13 takes a file descriptor of the same budget for a wait event set of its own, and errors with none left, taking pg_work down with every remote task it runs: one left over for it
INSERT INTO task ("group", input) SELECT 'fd_budget_local_' || i, 'SELECT 1' FROM generate_series(1, 5) AS i;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..600 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'fd_budget_%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 600 x pg_sleep(0.1) waiting for task groups ''fd_budget_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, count(*), error FROM task WHERE "group" LIKE 'fd_budget_%' GROUP BY state, error ORDER BY state;
SELECT pid = :work_pid AS same_work FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database();
DELETE FROM task WHERE "group" LIKE 'fd_budget_%';
