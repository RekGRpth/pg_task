-- a local task whose worker is terminated within the bookkeeping that starts it, task_work(), its lock taken already but its WORK not committed, is given back to PLAN on the way out, from 9.5 on and but in Greenplum, rather than left in TAKE till the next reset
SELECT current_setting('server_version_num')::int < 90500 OR EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE name = 'gp_role') AS left_in_take
\gset
CREATE SEQUENCE terminate_take_seq;
-- the first WORK of the task held up, for the worker to be terminated meanwhile
CREATE FUNCTION terminate_take_slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state = 'WORK' AND NEW."group" = 'terminate_take' AND pg_catalog.nextval('public.terminate_take_seq') = 1 THEN PERFORM pg_catalog.pg_sleep(10); END IF; RETURN NEW; END $$;
CREATE TRIGGER terminate_take_slow BEFORE UPDATE OF state ON task FOR EACH ROW EXECUTE PROCEDURE terminate_take_slow();
INSERT INTO task ("group", input) VALUES ('terminate_take', 'SELECT 1');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'UPDATE % SET "state" = ''WORK''%') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task worker of group terminate_take to start its bookkeeping'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_terminate_backend(pid) AS terminated FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND state = 'active' AND query LIKE 'UPDATE % SET "state" = ''WORK''%';
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF current_setting('server_version_num')::int < 90500 OR EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE name = 'gp_role') OR NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'terminate_take' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group terminate_take to be given back and run (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'DONE' OR :'left_in_take' AS done FROM task WHERE "group" = 'terminate_take';
DROP TRIGGER terminate_take_slow ON task;
DROP FUNCTION terminate_take_slow();
DROP SEQUENCE terminate_take_seq;
DELETE FROM task WHERE "group" = 'terminate_take';
