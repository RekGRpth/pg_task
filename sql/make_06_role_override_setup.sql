CREATE ROLE task_role_test LOGIN SUPERUSER;
ALTER ROLE task_role_test SET pg_task.schema = 'role_test_schema';
SELECT current_setting('pg_task.json') AS json_baseline
\gset
DELETE FROM pg_task_test_state WHERE key = 'json_baseline';
INSERT INTO pg_task_test_state VALUES ('json_baseline', :'json_baseline');
SELECT left(:'json_baseline', -1) || ',{"data":"' || :'DBNAME' || '","user":"task_role_test"}]' AS json_val
\gset
ALTER SYSTEM SET pg_task.json = :'json_val';
SELECT pg_reload_conf();
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF to_regclass('role_test_schema.task') IS NOT NULL THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT to_regclass('role_test_schema.task') IS NOT NULL AS table_created_via_role_override;
