-- an entry pg_conf couldn't start, its database not made as template1 was in use, is tried again after its pg_work.restart, rather than only on the next reload
SELECT current_database() AS test_db, current_setting('pg_task.json') AS json_baseline
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"conf_retry_data"}]' AS json_val
\gset
\c template1
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
-- CREATE DATABASE of pg_conf waits 5 seconds for this session to leave template1, then fails
SELECT pg_sleep(8);
\c :test_db
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = 'conf_retry_data') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the pg_work worker of database conf_retry_data'; END IF;
END;$body$ LANGUAGE plpgsql;
ALTER SYSTEM SET pg_task.json = :'json_baseline';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname = 'conf_retry_data' OR usename = 'conf_retry_data') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for backend(s) of database or role conf_retry_data to disconnect'; END IF;
END;$body$ LANGUAGE plpgsql;
DROP DATABASE IF EXISTS conf_retry_data;
DROP ROLE IF EXISTS conf_retry_data;
