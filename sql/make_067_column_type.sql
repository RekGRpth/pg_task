-- a column of the task table of another type than pg_task reads it by, of a table its owner made, char for "char" say, is refused, pg_work taking no task, rather than every task failing to start and, a remote one, taking pg_work down, as pg_work changes no column it didn't make, see make_column(): once its type is right, the tasks run
SELECT current_setting('pg_task.json') AS json_baseline
\gset
SELECT current_user AS test_user
\gset
ALTER SYSTEM SET pg_work.restart = 1;
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"column_type_schema","table":"column_type"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'column_type_schema' AND c.relname = 'column_type') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work column_type_schema column_type %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for table column_type_schema.column_type to be created by the pg_work worker'; END IF;
END;$body$ LANGUAGE plpgsql;
DROP TRIGGER column_type_delimiter ON column_type_schema.column_type;
ALTER TABLE column_type_schema.column_type ALTER COLUMN "delimiter" TYPE char(1);
SELECT pg_terminate_backend(pid) AS work_terminated FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work column_type_schema column_type %' AND datname = current_database();
SELECT pg_sleep(3);
INSERT INTO column_type_schema.column_type (input) VALUES ('SELECT 1 AS a');
SELECT pg_sleep(3);
SELECT state FROM column_type_schema.column_type;
ALTER TABLE column_type_schema.column_type ALTER COLUMN "delimiter" TYPE "char" USING "delimiter"::"char";
-- the triggers made again first, the one dropped above, which before 9.5 locks the table as a whole, the wait for the task below, in a transaction of its own reading the table, keeping it from that till lock_timeout otherwise
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'column_type_schema' AND c.relname = 'column_type') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work column_type_schema column_type %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of column_type_schema.column_type to be idle again, its triggers made'; END IF;
END;$body$ LANGUAGE plpgsql;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM column_type_schema.column_type WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of column_type_schema.column_type to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output FROM column_type_schema.column_type;
SELECT count(*) AS delimiter_trigger FROM pg_catalog.pg_trigger WHERE tgrelid = 'column_type_schema.column_type'::regclass AND tgname = 'column_type_delimiter';
ALTER SYSTEM RESET pg_work.restart;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work column_type_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker(s) matching ''pg_work column_type_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS column_type_schema CASCADE;
RESET client_min_messages;
