-- STOP cancels a task's input together with the processes it started, as pg_cancel_backend() does by signalling the whole process group of the backend, rather than leaving COPY ... PROGRAM waiting for its program to end by itself
DELETE FROM task WHERE "group" = 'stop_program';
\set remote NULL
INSERT INTO task ("group", remote, input) VALUES ('stop_program', :remote, 'COPY (SELECT 1) TO PROGRAM ''sleep 60''');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE query = 'COPY (SELECT 1) TO PROGRAM ''sleep 60''' AND state = 'active') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task running its program to appear in pg_stat_activity'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT set_config('pg_task_test.stop_at', clock_timestamp()::text, false) AS ignored
\gset
UPDATE task SET state = 'STOP' WHERE "group" = 'stop_program';
DO $body$ BEGIN
    FOR i IN 1..300 LOOP
        EXIT WHEN (SELECT error FROM task WHERE "group" = 'stop_program') IS NOT NULL;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT state, error IS NOT NULL AS failed, clock_timestamp() - current_setting('pg_task_test.stop_at')::timestamptz < '20 sec' AS stopped_promptly FROM task WHERE "group" = 'stop_program';
DELETE FROM task WHERE "group" = 'stop_program';
