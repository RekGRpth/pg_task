\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
SELECT set_config('pg_task_test.dbname', :'DBNAME', false) AS ignored
\gset
DO $$ BEGIN
    PERFORM set_config('pg_task_test.had_create', has_database_privilege(current_setting('pg_task_test.dbname'), current_setting('pg_task_test.dbname'), 'CREATE')::text, false);
    IF NOT has_database_privilege(current_setting('pg_task_test.dbname'), current_setting('pg_task_test.dbname'), 'CREATE') THEN
        EXECUTE format('GRANT CREATE ON DATABASE %I TO %I', current_setting('pg_task_test.dbname'), current_setting('pg_task_test.dbname'));
    END IF;
END $$;
SELECT current_setting('pg_task_test.had_create') AS had_create
\gset
SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","schema":"task_make_test_schema","table":"task_make_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('task_make_test_schema.task_make_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('task_make_test_schema.task_make_test') IS NOT NULL AS table_created;
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'task_make_test_schema.task_make_test' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
SELECT count(*) = 29 AS column_count_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attnum > 0 AND NOT attisdropped;
SELECT array_agg(enumlabel::text ORDER BY enumsortorder) = ARRAY['PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP'] AS enum_ok FROM pg_catalog.pg_enum WHERE enumtypid = 'task_make_test_schema.state'::regtype;
SELECT bool_and(attnotnull) AS not_null_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attname IN ('id', 'plan', 'active', 'live', 'repeat', 'timeout', 'count', 'max', 'state', 'delete', 'drift', 'header', 'save', 'string', 'delimiter', 'escape', 'quote', 'group', 'input', 'null');
SELECT bool_and(NOT attnotnull) AS nullable_ok FROM pg_catalog.pg_attribute WHERE attrelid = 'task_make_test_schema.task_make_test'::regclass AND attname IN ('parent', 'start', 'stop', 'pid', 'data', 'error', 'output', 'remote');
SELECT count(*) = 6 AS index_count_ok FROM pg_catalog.pg_index WHERE indrelid = 'task_make_test_schema.task_make_test'::regclass;
SELECT count(*) = 3 AS trigger_ok FROM pg_catalog.pg_trigger WHERE tgrelid = 'task_make_test_schema.task_make_test'::regclass AND NOT tgisinternal;
INSERT INTO task_make_test_schema.task_make_test (input) VALUES ('SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task_make_test_schema.task_make_test WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task_make_test_schema.task_make_test;
INSERT INTO task_make_test_schema.task_make_test (input, plan) VALUES ('SELECT 1 AS a', CURRENT_TIMESTAMP + interval '1 hour') RETURNING id AS state_test_id
\gset
SELECT set_config('pg_task_test.state_id', :'state_test_id', false) AS ignored
\gset
DO $$ BEGIN
    UPDATE task_make_test_schema.task_make_test SET state = 'DONE' WHERE id = current_setting('pg_task_test.state_id')::bigint;
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM <> 'invalid state transition' THEN RAISE; END IF;
END $$;
DO $$ BEGIN
    UPDATE task_make_test_schema.task_make_test SET state = 'WORK' WHERE id = current_setting('pg_task_test.state_id')::bigint;
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM <> 'invalid state transition' THEN RAISE; END IF;
END $$;
SELECT state = 'PLAN' AS invalid_state_transition_rejected FROM task_make_test_schema.task_make_test WHERE id = :state_test_id;
UPDATE task_make_test_schema.task_make_test SET state = 'STOP' WHERE id = :state_test_id;
SELECT state = 'STOP' AS manual_stop_accepted FROM task_make_test_schema.task_make_test WHERE id = :state_test_id;
DO $$ BEGIN
    UPDATE task_make_test_schema.task_make_test SET state = 'PLAN' WHERE id = current_setting('pg_task_test.state_id')::bigint;
EXCEPTION WHEN OTHERS THEN
    IF SQLERRM <> 'invalid state transition' THEN RAISE; END IF;
END $$;
SELECT state = 'STOP' AS stop_is_terminal FROM task_make_test_schema.task_make_test WHERE id = :state_test_id;
DELETE FROM task_make_test_schema.task_make_test WHERE id = :state_test_id;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_make_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
\i :gp_utility_file
DROP SCHEMA task_make_test_schema CASCADE;
\connect :DBNAME
SELECT set_config('pg_task_test.dbname', :'DBNAME', false) AS ignored1, set_config('pg_task_test.had_create', :'had_create', false) AS ignored2
\gset
CREATE ROLE task_role_test LOGIN SUPERUSER;
ALTER ROLE task_role_test SET pg_task.schema = 'role_test_schema';
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","user":"task_role_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('role_test_schema.task') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('role_test_schema.task') IS NOT NULL AS table_created_via_role_override;
INSERT INTO role_test_schema.task (input) VALUES ('SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM role_test_schema.task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM role_test_schema.task;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work role_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
\i :gp_utility_file
DROP SCHEMA role_test_schema CASCADE;
\connect :DBNAME
SELECT set_config('pg_task_test.dbname', :'DBNAME', false) AS ignored1, set_config('pg_task_test.had_create', :'had_create', false) AS ignored2
\gset
DROP ROLE task_role_test;
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","schema":"task_column_drift_test_schema","table":"task_column_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('task_column_drift_test_schema.task_column_drift_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_column_drift_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
\i :gp_utility_file
ALTER TABLE task_column_drift_test_schema.task_column_drift_test DROP COLUMN "delimiter";
\connect :DBNAME
SELECT set_config('pg_task_test.dbname', :'DBNAME', false) AS ignored1, set_config('pg_task_test.had_create', :'had_create', false) AS ignored2
\gset
SELECT count(*) = 0 AS column_dropped FROM pg_catalog.pg_attribute WHERE attrelid = 'task_column_drift_test_schema.task_column_drift_test'::regclass AND attname = 'delimiter' AND NOT attisdropped;
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","schema":"task_column_drift_test_schema","table":"task_column_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM pg_catalog.pg_attribute WHERE attrelid = 'task_column_drift_test_schema.task_column_drift_test'::regclass AND attname = 'delimiter' AND NOT attisdropped) = 1 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) = 1 AS column_healed FROM pg_catalog.pg_attribute WHERE attrelid = 'task_column_drift_test_schema.task_column_drift_test'::regclass AND attname = 'delimiter' AND NOT attisdropped;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_column_drift_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
\i :gp_utility_file
DROP SCHEMA task_column_drift_test_schema CASCADE;
\connect :DBNAME
SELECT set_config('pg_task_test.dbname', :'DBNAME', false) AS ignored1, set_config('pg_task_test.had_create', :'had_create', false) AS ignored2
\gset
CREATE SCHEMA task_enum_drift_test_schema;
CREATE TYPE task_enum_drift_test_schema.state AS ENUM ('PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL');
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","schema":"task_enum_drift_test_schema","table":"task_enum_drift_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF to_regclass('task_enum_drift_test_schema.task_enum_drift_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT array_agg(enumlabel::text ORDER BY enumsortorder) = ARRAY['PLAN', 'GONE', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP'] AS enum_healed FROM pg_catalog.pg_enum WHERE enumtypid = 'task_enum_drift_test_schema.state'::regtype;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work task_enum_drift_test_schema %') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DROP SCHEMA IF EXISTS task_enum_drift_test_schema CASCADE;
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"' || :'DBNAME' || '","user":"task_make_user_test","schema":"task_user_make_test_schema","table":"task_user_make_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'task_make_user_test') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'task_make_user_test') AS role_created;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_make_user_test') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DROP ROLE task_make_user_test;
SELECT '[{"data":"' || :'DBNAME' || '"},{"data":"task_make_data_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'task_make_data_test') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'task_make_data_test') AS database_created;
ALTER SYSTEM RESET pg_task.json;
SELECT pg_reload_conf();
DO $$ BEGIN PERFORM pg_sleep(5); END $$;
DROP DATABASE task_make_data_test;
DO $body$ BEGIN
    FOR i IN 1..30 LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE usename = 'task_make_data_test') THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
DROP ROLE task_make_data_test;
DO $$ BEGIN
    IF NOT current_setting('pg_task_test.had_create')::boolean THEN
        EXECUTE format('REVOKE CREATE ON DATABASE %I FROM %I', current_setting('pg_task_test.dbname'), current_setting('pg_task_test.dbname'));
    END IF;
END $$;
