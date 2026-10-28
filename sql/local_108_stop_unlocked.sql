-- STOP cancels a task whose input let go of the locks of pg_task, as pg_advisory_unlock_all() does, its task worker taking them back only after the command, or in spi mode after the whole input: the task is told by the slot of its worker then, rather than by the lock of its id only, and not run to the end
DELETE FROM task WHERE "group" LIKE 'stop_unlocked_%';
INSERT INTO task ("group", input) VALUES
    ('stop_unlocked_command', 'SELECT pg_advisory_unlock_all(), pg_sleep(60)'),
    ('stop_unlocked_input', 'SELECT pg_advisory_unlock_all(); SELECT pg_sleep(60)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'stop_unlocked_%' AND state = 'WORK') = 2 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of groups ''stop_unlocked_%%'' to start'; END IF;
END;$body$ LANGUAGE plpgsql;
-- their locks let go of by then
SELECT pg_sleep(1);
SELECT set_config('pg_task_test.stop_at', clock_timestamp()::text, false) AS ignored
\gset
UPDATE task SET state = 'STOP' WHERE "group" LIKE 'stop_unlocked_%';
DO $body$ BEGIN
    FOR i IN 1..300 LOOP
        EXIT WHEN NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'stop_unlocked_%' AND error IS NULL);
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, error LIKE '%canceling statement due to user request%' AS cancelled, clock_timestamp() - current_setting('pg_task_test.stop_at')::timestamptz < '20 sec' AS stopped_promptly FROM task WHERE "group" LIKE 'stop_unlocked_%' ORDER BY "group";
DELETE FROM task WHERE "group" LIKE 'stop_unlocked_%';
