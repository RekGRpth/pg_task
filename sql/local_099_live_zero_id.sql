-- the next task of a group, taken by the worker that did the one before, of count say, runs on it even of id 0, which an author may insert, rather than be taken for none, left in TAKE till the reset, see task_live()
DELETE FROM task WHERE "group" = 'live_zero_id';
INSERT INTO task ("group", input, count) VALUES ('live_zero_id', 'SELECT pg_sleep(2)', 2);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'live_zero_id' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the first task of group live_zero_id to start'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task (id, "group", input, count) VALUES (0, 'live_zero_id', 'SELECT 0 AS a', 2);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'live_zero_id' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of group live_zero_id to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT id = 0 AS zero, state, output, error FROM task WHERE "group" = 'live_zero_id' ORDER BY id = 0;
SELECT count(DISTINCT pid) = 1 AS same_worker FROM task WHERE "group" = 'live_zero_id';
DELETE FROM task WHERE "group" = 'live_zero_id';
