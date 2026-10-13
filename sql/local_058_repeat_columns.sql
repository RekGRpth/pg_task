-- a repeat copies the columns the task table has at the time, a user's own included, but not generated or identity ones, which take no value to insert; a column added or dropped since doesn't fail the repeats of a worker or connection kept for several tasks
DELETE FROM task WHERE "group" = 'repeat_columns';
\set remote NULL
ALTER TABLE task ADD COLUMN rc_tmp text;
DO $body$ BEGIN
    IF current_setting('server_version_num')::int >= 120000 THEN EXECUTE 'ALTER TABLE task ADD COLUMN rc_gen int GENERATED ALWAYS AS (1) STORED'; END IF;
END;$body$ LANGUAGE plpgsql;
INSERT INTO task ("group", max, count, "repeat", remote, input) VALUES ('repeat_columns', 0, 10, '1 hour', :remote, 'SELECT 1 AS a'), ('repeat_columns', 0, 10, '1 hour', :remote, 'SELECT pg_sleep(2)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'repeat_columns' AND parent IS NULL AND input = 'SELECT 1 AS a' AND state = 'DONE') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the first task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER TABLE task DROP COLUMN rc_tmp;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'repeat_columns' AND parent IS NULL AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the second task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, (SELECT count(*) FROM task r WHERE r.parent = t.id AND r.state = 'PLAN') AS repeats FROM task t WHERE "group" = 'repeat_columns' AND parent IS NULL ORDER BY id;
DELETE FROM task WHERE "group" = 'repeat_columns';
SET client_min_messages = warning;
ALTER TABLE task DROP COLUMN IF EXISTS rc_tmp, DROP COLUMN IF EXISTS rc_gen;
RESET client_min_messages;
