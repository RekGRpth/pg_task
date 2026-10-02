-- a task author may only set its tasks to STOP, while queued or running; the table owner (pg_task.user), whose bookkeeping drives a task through its states, may make every transition, and doing so by hand to a running task takes neither its worker nor pg_work down
DELETE FROM task WHERE "group" LIKE 'author\_state%';
SET client_min_messages = warning;
CREATE ROLE task_state_author LOGIN;
RESET client_min_messages;
SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\pset tuples_only on
\pset format unaligned
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'task' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
\i :gp_utility_file
\pset tuples_only off
\pset format aligned
GRANT SELECT, INSERT, UPDATE, DELETE ON task TO task_state_author;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq TO task_state_author;
\connect :DBNAME
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND application_name LIKE 'pg_work public task %'
\gset
SET ROLE task_state_author;
INSERT INTO task ("group", input) VALUES ('author_state_work', 'SELECT pg_sleep(30)');
INSERT INTO task ("group", plan, input) VALUES ('author_state_plan', now() + '1 hour', 'SELECT 1');
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'author_state_work' AND state = 'WORK') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of the author to start'; END IF;
END;$body$ LANGUAGE plpgsql;
SET ROLE task_state_author;
DO $body$ BEGIN
    UPDATE task SET state = 'DONE' WHERE "group" = 'author_state_work';
    RAISE NOTICE 'WORK to DONE: allowed';
EXCEPTION WHEN raise_exception THEN RAISE NOTICE 'WORK to DONE: %', SQLERRM;
END;$body$ LANGUAGE plpgsql;
DO $body$ BEGIN
    UPDATE task SET state = 'TAKE' WHERE "group" = 'author_state_plan';
    RAISE NOTICE 'PLAN to TAKE: allowed';
EXCEPTION WHEN raise_exception THEN RAISE NOTICE 'PLAN to TAKE: %', SQLERRM;
END;$body$ LANGUAGE plpgsql;
DO $body$ BEGIN
    UPDATE task SET state = 'STOP' WHERE "group" = 'author_state_plan';
    RAISE NOTICE 'PLAN to STOP: allowed';
EXCEPTION WHEN raise_exception THEN RAISE NOTICE 'PLAN to STOP: %', SQLERRM;
END;$body$ LANGUAGE plpgsql;
DO $body$ BEGIN
    UPDATE task SET state = 'STOP' WHERE "group" = 'author_state_work';
    RAISE NOTICE 'WORK to STOP: allowed';
EXCEPTION WHEN raise_exception THEN RAISE NOTICE 'WORK to STOP: %', SQLERRM;
END;$body$ LANGUAGE plpgsql;
RESET ROLE;
INSERT INTO task ("group", input) VALUES ('author_state_owner_local', 'SELECT pg_sleep(2)');
INSERT INTO task ("group", input, remote) VALUES ('author_state_owner_remote', 'SELECT pg_sleep(2)', 'dbname=' || :'DBNAME');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'author\_state\_owner\_%' AND state = 'WORK') = 2 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of the owner to start'; END IF;
END;$body$ LANGUAGE plpgsql;
UPDATE task SET state = 'DONE' WHERE "group" LIKE 'author\_state\_owner\_%';
SELECT pg_sleep(4);
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'author_state_work' AND stop IS NOT NULL) = 1 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of the author in STOP to be cancelled'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, output FROM task WHERE "group" LIKE 'author\_state%' ORDER BY id;
SELECT pg_stat_clear_snapshot();
SELECT count(*) = 1 AS same_work FROM pg_catalog.pg_stat_activity WHERE pid = :work_pid;
DELETE FROM task WHERE "group" LIKE 'author\_state%';
SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\pset tuples_only on
\pset format unaligned
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'task' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
\i :gp_utility_file
\pset tuples_only off
\pset format aligned
REVOKE SELECT, INSERT, UPDATE, DELETE ON task FROM task_state_author;
REVOKE USAGE, SELECT, UPDATE ON SEQUENCE task_id_seq FROM task_state_author;
\connect :DBNAME
DROP ROLE task_state_author;
