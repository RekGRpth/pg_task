-- STOP cancels a local task of id 0, which an author may insert, as any other, its slot marked by a flag rather than by an id of 0 for none, and found among those of task workers only, not that of pg_work, whose id is 0 too, see init_stop()
DELETE FROM task WHERE "group" = 'zero_id_stop';
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()
\gset
INSERT INTO task (id, "group", input) VALUES (0, 'zero_id_stop', 'SELECT pg_sleep(60)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'zero_id_stop' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group zero_id_stop to start'; END IF;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'STOP' WHERE "group" = 'zero_id_stop';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'zero_id_stop' AND stop IS NOT NULL) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group zero_id_stop to be cancelled'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id, state, error LIKE '%canceling statement due to user request%' AS cancelled FROM task WHERE "group" = 'zero_id_stop';
SELECT pg_stat_clear_snapshot();
SELECT count(*) = 1 AS same_work FROM pg_catalog.pg_stat_activity WHERE pid = :work_pid;
DELETE FROM task WHERE "group" = 'zero_id_stop';
