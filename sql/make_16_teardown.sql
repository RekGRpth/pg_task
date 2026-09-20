SELECT value AS base_json FROM pg_task_test_state WHERE key = 'base_json'
\gset
SELECT set_config('pg_task_test.base_json_added', value, false) AS ignored FROM pg_task_test_state WHERE key = 'base_json_added'
\gset
DROP TABLE pg_task_test_state;
ALTER SYSTEM SET pg_task.json = :'base_json';
SELECT pg_reload_conf();
DO $body$ BEGIN
    IF current_setting('pg_task_test.base_json_added') = 't' THEN
        FOR i IN 1..120 LOOP
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()) THEN EXIT; END IF;
            PERFORM pg_sleep(1);
        END LOOP;
    END IF;
END;$body$ LANGUAGE plpgsql;
