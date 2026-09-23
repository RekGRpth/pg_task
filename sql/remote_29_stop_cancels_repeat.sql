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
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..150 LOOP
        IF (SELECT state FROM task WHERE "group" = 'stop_cancels_repeat' AND parent IS NULL) = 'STOP' THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 150 x pg_sleep(0.1) waiting for root task in group ''stop_cancels_repeat'' to reach STOP state'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) = 1 AS no_repeat_after_stop, bool_and(state = 'STOP') AS cancelled_cleanly FROM task WHERE "group" = 'stop_cancels_repeat';
DELETE FROM task WHERE "group" = 'stop_cancels_repeat'; -- a STOP row is terminal and never cleaned up by pg_task itself; leaving it behind would make every later "wait for everything to finish" loop spin out to its full bound
