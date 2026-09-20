SELECT current_setting('pg_task.json') AS json_baseline
\gset
DELETE FROM pg_task_test_state WHERE key = 'json_baseline';
INSERT INTO pg_task_test_state VALUES ('json_baseline', :'json_baseline');
SELECT current_user AS test_user
\gset
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"' || :'test_user' || '","schema":"task_make_test_schema","table":"task_make_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF to_regclass('task_make_test_schema.task_make_test') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('task_make_test_schema.task_make_test') IS NOT NULL AS table_created;
