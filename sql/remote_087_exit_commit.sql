-- a remote task whose input leaves its transaction open, which pg_task commits itself once its results are through, its COMMIT sent but not read, pg_work held up meanwhile, on a lock of the table say, is booked by pg_work on its way out, waiting for the result of its COMMIT, rather than cancelled with its input committed, its row left in WORK, for the next pg_work to reset it and run it again
DELETE FROM task WHERE "group" LIKE 'exit_commit%';
SET client_min_messages = warning;
DROP TABLE IF EXISTS exit_commit_runs;
RESET client_min_messages;
CREATE TABLE exit_commit_runs (id bigint);
INSERT INTO task ("group", input, remote) VALUES ('exit_commit', 'BEGIN; INSERT INTO exit_commit_runs VALUES (1); SELECT pg_sleep(1)', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'exit_commit' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group exit_commit to start'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()
\gset
SELECT set_config('pg_task_test.work_pid', :'work_pid', false) IS NOT NULL AS work_pid_saved;
-- the table locked for a while by a local task, pg_work held up on its next query, the remote input through with its transaction open meanwhile; nothing here reading the table till then
INSERT INTO task ("group", input) VALUES ('exit_commit_lock', 'LOCK TABLE task; SELECT pg_sleep(4)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE query = 'LOCK TABLE task; SELECT pg_sleep(4)' AND state = 'active') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task locking the table to run'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_sleep(2);
SELECT pg_terminate_backend(:work_pid) AS work_terminated;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = current_setting('pg_task_test.work_pid')::int) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work to be gone'; END IF;
END;$body$ LANGUAGE plpgsql;
-- its COMMIT read and booked by the pg_work gone, once the lock was let go of, before any next one could reset it
SELECT state FROM task WHERE "group" = 'exit_commit';
-- the next pg_work up, idle, for the tests after this one, and the task run once all the same
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..900 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work public task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) AND NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'exit_commit%' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 900 x pg_sleep(0.1) waiting for the next pg_work to be idle'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) AS runs FROM exit_commit_runs;
DELETE FROM task WHERE "group" LIKE 'exit_commit%';
DROP TABLE exit_commit_runs;
