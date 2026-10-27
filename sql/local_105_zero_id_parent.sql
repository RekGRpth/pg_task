-- a task inserted by a task has it for its parent, by pg_task.id, one of id 0, which an author may insert, too, while one inserted out of a task has none, pg_task.id empty there, see the default of parent
DELETE FROM task WHERE "group" LIKE 'zero_id_parent%';
SELECT current_setting('pg_task.id') = '' AS id_empty;
INSERT INTO task ("group", input) VALUES ('zero_id_parent_root', 'SELECT 1');
INSERT INTO task (id, "group", input) VALUES
    (0, 'zero_id_parent', $q$INSERT INTO task ("group", input) VALUES ('zero_id_parent_of_0', 'SELECT 1')$q$),
    (-1051, 'zero_id_parent', $q$INSERT INTO task ("group", input) VALUES ('zero_id_parent_of_1051', 'SELECT 1')$q$);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'zero_id_parent%' AND state IN ('DONE', 'GONE', 'FAIL')) = 5 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of groups zero_id_parent%% and their children to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, parent FROM task WHERE "group" LIKE 'zero_id_parent%' ORDER BY "group", id;
DELETE FROM task WHERE "group" LIKE 'zero_id_parent%';
