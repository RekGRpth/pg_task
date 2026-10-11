DELETE FROM task WHERE "group" = 'stop_cancels_repeat';
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, repeat, remote) VALUES ('stop_cancels_repeat', 'SELECT pg_sleep(30)', '3 sec', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..900 LOOP
        IF (SELECT state FROM task WHERE "group" = 'stop_cancels_repeat' AND parent IS NULL) = 'WORK' THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 900 x pg_sleep(0.1) waiting for root task in group ''stop_cancels_repeat'' to reach WORK state'; END IF;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'STOP' WHERE "group" = 'stop_cancels_repeat' AND parent IS NULL AND state = 'WORK';
-- the STOP set here is there at once: wait for the task to be cancelled, its bookkeeping done, which inserts no repeat after a STOP, before looking, or deleting its row, which, ahead of the cancel, would leave its input to run to the end
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT state = 'STOP' AND stop IS NOT NULL FROM task WHERE "group" = 'stop_cancels_repeat' AND parent IS NULL) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for root task in group ''stop_cancels_repeat'' to be cancelled in STOP, its bookkeeping done'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) = 1 AS no_repeat_after_stop, bool_and(state = 'STOP') AS cancelled_cleanly FROM task WHERE "group" = 'stop_cancels_repeat';
DELETE FROM task WHERE "group" = 'stop_cancels_repeat'; -- a STOP row is terminal and never cleaned up by pg_task itself; leaving it behind would make every later "wait for everything to finish" loop spin out to its full bound
