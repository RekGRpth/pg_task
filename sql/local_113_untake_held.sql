-- a task given back to PLAN on the way out of its worker, which was terminated within the bookkeeping that starts it, its row taken by someone else by then, a lock waiting for the one of the worker, is given back all the same, once it's let go of, rather than left in TAKE till the next reset, see dest_untake(), from 9.5 on and but in Greenplum
SELECT current_setting('server_version_num')::int < 90500 OR EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE name = 'gp_role') AS left_in_take
\gset
DELETE FROM task WHERE "group" IN ('untake_held', 'untake_held_lock');
CREATE SEQUENCE untake_held_seq;
-- the first WORK of the task held up, for the worker to be terminated meanwhile
CREATE FUNCTION untake_held_slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state = 'WORK' AND NEW."group" = 'untake_held' AND pg_catalog.nextval('public.untake_held_seq') = 1 THEN PERFORM pg_catalog.pg_sleep(10); END IF; RETURN NEW; END $$;
CREATE TRIGGER untake_held_slow BEFORE UPDATE OF state ON task FOR EACH ROW EXECUTE PROCEDURE untake_held_slow();
INSERT INTO task ("group", input) VALUES ('untake_held', 'SELECT 1') RETURNING id AS held_id
\gset
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'UPDATE % SET "state" = ''WORK''%') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task worker of group untake_held to start its bookkeeping'; END IF;
END;$body$ LANGUAGE plpgsql;
-- another task, which waits for the row of the first one, held by its worker, and keeps it for a while once it has it
INSERT INTO task ("group", input) VALUES ('untake_held_lock', format('DO $$ BEGIN PERFORM 1 FROM public.task WHERE id = %s FOR UPDATE; PERFORM pg_sleep(2); END $$', :held_id));
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid WHERE NOT l.granted AND a.datname = current_database() AND a.application_name LIKE '% untake_held_lock') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task worker of group untake_held_lock to wait for the row of the task of group untake_held'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_terminate_backend(pid) AS terminated FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'UPDATE % SET "state" = ''WORK''%';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF current_setting('server_version_num')::int < 90500 OR EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE name = 'gp_role') OR NOT EXISTS (SELECT 1 FROM task WHERE "group" IN ('untake_held', 'untake_held_lock') AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of groups untake_held and untake_held_lock to be given back and run (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state = 'DONE' OR :'left_in_take' AS done FROM task WHERE "group" IN ('untake_held', 'untake_held_lock') ORDER BY "group";
DROP TRIGGER untake_held_slow ON task;
DROP FUNCTION untake_held_slow();
DROP SEQUENCE untake_held_seq;
DELETE FROM task WHERE "group" IN ('untake_held', 'untake_held_lock');
