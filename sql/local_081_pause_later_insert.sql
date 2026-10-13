-- a pause, which a task of a negative max schedules as it's done, holds a task of the group inserted after that too, till its end, rather than only those planned as it's done, whose plans are put off: a producer inserting its tasks one at a time gets them paced as well
DELETE FROM task WHERE "group" = 'pause_later_insert';
INSERT INTO task ("group", max, drift, input) VALUES ('pause_later_insert', -3000, true, 'SELECT 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'pause_later_insert' AND state = 'DONE') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the first task in group ''pause_later_insert'' to be done'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task ("group", max, drift, input) VALUES ('pause_later_insert', -3000, true, 'SELECT 2');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'pause_later_insert' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''pause_later_insert'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT b.state, b.start - a.stop >= interval '3 sec' AS paused FROM task AS a, task AS b WHERE a."group" = 'pause_later_insert' AND a.input = 'SELECT 1' AND b."group" = 'pause_later_insert' AND b.input = 'SELECT 2';
DELETE FROM task WHERE "group" = 'pause_later_insert';
