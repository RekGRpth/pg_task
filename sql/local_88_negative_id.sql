-- a task of a negative id, which an author may insert, is found by the lock of its id, whose high 32 bits are taken unsigned, as for any other: pg_work stops it, rather than fail on an oid out of range and go down
DELETE FROM task WHERE "group" = 'negative_id';
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()
\gset
INSERT INTO task (id, "group", input) VALUES (-1, 'negative_id', 'SELECT pg_sleep(30)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'negative_id' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group negative_id to start'; END IF;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'STOP' WHERE "group" = 'negative_id';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'negative_id' AND stop IS NOT NULL) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group negative_id to be cancelled'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id, state, error LIKE '%canceling statement due to user request%' AS cancelled FROM task WHERE "group" = 'negative_id';
SELECT pg_stat_clear_snapshot();
SELECT count(*) = 1 AS same_work FROM pg_catalog.pg_stat_activity WHERE pid = :work_pid;
DELETE FROM task WHERE "group" = 'negative_id';
