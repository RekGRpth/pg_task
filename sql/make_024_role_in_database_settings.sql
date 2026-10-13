-- a setting of another role in the database (ALTER ROLE ... IN DATABASE) isn't one of the database's: it must not turn a configured table into another one, which pg_conf would start a second pg_work for
SET client_min_messages = warning;
CREATE ROLE role_in_db_test;
RESET client_min_messages;
ALTER ROLE role_in_db_test IN DATABASE :DBNAME SET pg_task.schema = 'role_in_db_test_schema';
SELECT pg_reload_conf();
SELECT pg_sleep(3);
SELECT pg_stat_clear_snapshot();
SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'role_in_db_test_schema') AS no_foreign_schema, NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work role_in_db_test_schema %') AS no_foreign_work;
ALTER ROLE role_in_db_test IN DATABASE :DBNAME RESET pg_task.schema;
DROP ROLE role_in_db_test;
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work role_in_db_test_schema %') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker(s) matching ''pg_work role_in_db_test_schema %%'' to stop'; END IF;
END;$body$ LANGUAGE plpgsql;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS role_in_db_test_schema CASCADE;
RESET client_min_messages;
