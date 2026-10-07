-- a worker kept alive by live takes the next task of its group only if the group fits its own max, as any task taken does, not running it alongside tasks of a higher max that filled the group since
DELETE FROM task WHERE "group" = 'live_group_max';
INSERT INTO task ("group", max, live, input) VALUES ('live_group_max', 0, '10 sec', 'SELECT ''a''; SELECT pg_sleep(3)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'live_group_max' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task in group ''live_group_max'' to start'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task ("group", max, input) VALUES ('live_group_max', 2, 'SELECT ''x''; SELECT pg_sleep(5)'), ('live_group_max', 2, 'SELECT ''x''; SELECT pg_sleep(5)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'live_group_max' AND state = 'WORK') = 3 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of max 2 in group ''live_group_max'' to start'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task ("group", max, live, input) VALUES ('live_group_max', 0, '10 sec', 'SELECT ''b''');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'live_group_max' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''live_group_max'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT b.state, b.start >= (SELECT max(stop) FROM task WHERE "group" = 'live_group_max' AND input LIKE 'SELECT ''x''%') AS after_x, b.pid <> a.pid AS own_worker FROM task AS b, task AS a WHERE b."group" = 'live_group_max' AND b.input = 'SELECT ''b''' AND a."group" = 'live_group_max' AND a.input LIKE 'SELECT ''a''%';
DELETE FROM task WHERE "group" = 'live_group_max';
