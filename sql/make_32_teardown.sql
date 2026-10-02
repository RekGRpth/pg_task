SELECT value AS base_json FROM pg_task_test_state WHERE key = 'base_json'
\gset
SELECT set_config('pg_task_test.base_json_added', (SELECT value FROM pg_task_test_state WHERE key = 'base_json_added'), false) AS ignored
\gset
DROP TABLE pg_task_test_state;
ALTER SYSTEM RESET pg_work.restart;
ALTER SYSTEM RESET pg_task.sleep;
ALTER SYSTEM SET pg_task.json = :'base_json';
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    IF current_setting('pg_task_test.base_json_added') = 't' THEN
        FOR i IN 1..300 LOOP
            PERFORM pg_stat_clear_snapshot();
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()) THEN ok := true; EXIT; END IF;
            PERFORM pg_sleep(0.1);
        END LOOP;
        IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for pg_work worker for public.task to stop'; END IF;
    END IF;
END;$body$ LANGUAGE plpgsql;
