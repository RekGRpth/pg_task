-- the self-provisioning's DDL waits for a table busy with its tasks for 2 s at a time, five times, and then gives up, rather than waiting for as long as it's busy: pg_work's own handler of SIGINT, its wake-up, which the lock timeout signals through too, lets that one through
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || current_user || '","schema":"ddl_lock_schema"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'ddl_lock_schema' AND c.relname = 'task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work ddl_lock_schema task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of ddl_lock_schema.task to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work ddl_lock_schema task %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of ddl_lock_schema.task to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
-- a trigger to make again, and a task of the main table holding the table meanwhile, as one writing to it does, which the checks before the DDL don't wait for
DO $body$ BEGIN
    EXECUTE (SELECT format('DROP TRIGGER %I ON ddl_lock_schema.task', tgname) FROM pg_catalog.pg_trigger WHERE tgrelid = 'ddl_lock_schema.task'::regclass AND NOT tgisinternal ORDER BY tgname LIMIT 1);
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = 'ddl_lock_holder';
INSERT INTO task ("group", input) VALUES ('ddl_lock_holder', 'LOCK TABLE ddl_lock_schema.task IN ROW EXCLUSIVE MODE; SELECT pg_sleep(60)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE relation = 'ddl_lock_schema.task'::regclass AND mode = 'RowExclusiveLock' AND granted) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of group ''ddl_lock_holder'' to lock ddl_lock_schema.task'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
-- pg_work comes up, waits for the table and gives up within about 5 x 2 s, while the table is still busy
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work ddl_lock_schema task %' AND (to_json(a) ->> 'wait_event_type' = 'Lock' OR to_json(a) ->> 'waiting' = 'true')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of ddl_lock_schema.task to wait for its table'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT set_config('pg_task_test.ddl_lock_pid', (SELECT pid::text FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work ddl_lock_schema task %'), false) IS NOT NULL AS pid_saved;
-- that one, as another one starts a second after (pg_work.restart = 1)
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = current_setting('pg_task_test.ddl_lock_pid')::int) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of ddl_lock_schema.task to give up on its busy table'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE relation = 'ddl_lock_schema.task'::regclass AND mode = 'RowExclusiveLock' AND granted) AS table_still_busy;
UPDATE task SET state = 'STOP' WHERE "group" = 'ddl_lock_holder' AND state = 'WORK';
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work ddl_lock_schema task %' OR query LIKE 'LOCK TABLE ddl_lock_schema.task%') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of ddl_lock_schema.task and the task holding it to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" = 'ddl_lock_holder';
SET client_min_messages TO WARNING;
DROP SCHEMA ddl_lock_schema CASCADE;
RESET client_min_messages;
