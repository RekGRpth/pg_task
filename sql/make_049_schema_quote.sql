-- a schema whose name has dollar quotes and quotes in it, which pg_task.schema may come with from the settings of the database, as its owner may change them, here from the entry itself, not to move the other entries there, is provisioned as any other: its name in the default of the state column and in the bodies of the trigger functions is no longer put in the text of a dollar quoted string, which it would end
SELECT current_setting('pg_task.json') AS json_baseline, 'q$$ $function$ '' "' AS schema_name
\gset
SELECT left(:'json_baseline', -1) || ',' || json_build_object('data', :'DBNAME', 'user', current_user, 'schema', :'schema_name', 'table', 'quote_task')::text || ']' AS json_val
\gset
SELECT set_config('pg_task_test.schema', :'schema_name', false) IS NOT NULL AS schema_saved;
ALTER SYSTEM SET pg_work.restart = 1;
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_setting('pg_task_test.schema') AND c.relname = 'quote_task') AND EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE application_name LIKE 'pg_work % quote_task %' AND datname = current_database() AND state = 'idle' AND CASE WHEN current_setting('server_version_num')::int < 100000 THEN a.query LIKE 'WITH %' OR a.query LIKE 'SELECT COALESCE(LEAST(%' ELSE to_json(a) ->> 'wait_event_type' = 'Extension' END) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of quote_task to become idle'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT format('INSERT INTO %I.quote_task (input) VALUES (''SELECT 1'')', :'schema_name') AS insert_task
\gset
:insert_task;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_setting('pg_task_test.schema') AND c.relname = 'quote_task') THEN
            EXECUTE format('SELECT NOT EXISTS (SELECT 1 FROM %I.quote_task WHERE state NOT IN (''DONE'', ''GONE'', ''FAIL''))', current_setting('pg_task_test.schema')) INTO ok;
            EXIT WHEN ok;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of quote_task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT format('SELECT state, output FROM %I.quote_task', :'schema_name') AS select_task
\gset
:select_task;
SELECT count(*) AS functions FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = :'schema_name';
ALTER SYSTEM RESET pg_work.restart;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work % quote_task %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of quote_task to go away'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
SELECT format('DROP SCHEMA %I CASCADE', :'schema_name') AS drop_schema
\gset
:drop_schema;
RESET client_min_messages;
