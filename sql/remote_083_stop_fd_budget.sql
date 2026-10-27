-- a STOP of every remote task at once, the file descriptors pg_work may hold for others than files (a third of max_files_per_process at most, see test.conf) all taken by them, gets the cancel request of each through, on the one it holds for it, rather than wait for a running one to be done, or fail with too many open files, under a ulimit -n below max_files_per_process, the sockets of the cancels uncounted
DELETE FROM task WHERE "group" LIKE 'stop_fd_budget_%';
INSERT INTO task ("group", input, remote) SELECT 'stop_fd_budget_' || i, 'SELECT pg_sleep(60) AS stop_fd_budget', 'dbname=' || :'DBNAME' FROM generate_series(1, 30) AS i;
-- as many of them running as the file descriptors let, from 13 on, the rest back to PLAN: their number the same for a second
DO $body$ DECLARE ok boolean := false; last bigint := -1; running bigint; same int := 0; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT count(*) INTO running FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(60) AS stop_fd_budget' AND state = 'active';
        IF running > 0 AND running = last THEN same := same + 1; ELSE same := 0; END IF;
        IF same >= 10 THEN ok := true; EXIT; END IF;
        last := running;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the remote tasks of groups ''stop_fd_budget_%%'' running to be as many for a second'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT set_config('pg_task_test.stop_at', clock_timestamp()::text, false) AS ignored
\gset
UPDATE task SET state = 'STOP' WHERE "group" LIKE 'stop_fd_budget_%' AND state IN ('PLAN', 'WORK');
-- one taken just then, as a probe for a file descriptor, may not go to STOP: stopped once back in PLAN, or in WORK
DO $body$ BEGIN
    FOR i IN 1..100 LOOP
        EXIT WHEN NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'stop_fd_budget_%' AND state = 'TAKE');
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'STOP' WHERE "group" LIKE 'stop_fd_budget_%' AND state IN ('PLAN', 'WORK');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'stop_fd_budget_%' AND start IS NOT NULL AND stop IS NULL) AND NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(60) AS stop_fd_budget' AND state = 'active') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the remote tasks of groups ''stop_fd_budget_%%'' to be cancelled, their bookkeeping done'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) AS stopped, count(*) FILTER (WHERE start IS NOT NULL) > 0 AS some_ran, bool_and(error LIKE '%canceling statement due to user request%') FILTER (WHERE start IS NOT NULL) AS ran_cancelled, clock_timestamp() - current_setting('pg_task_test.stop_at')::timestamptz < '30 sec' AS cancelled_promptly FROM task WHERE "group" LIKE 'stop_fd_budget_%' AND state = 'STOP';
DELETE FROM task WHERE "group" LIKE 'stop_fd_budget_%';
