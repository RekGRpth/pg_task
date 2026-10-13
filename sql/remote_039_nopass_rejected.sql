DELETE FROM remote_nopass_test_schema.task WHERE "group" = 'nopass';
SET ROLE task_remote_nopass_test;
INSERT INTO remote_nopass_test_schema.task ("group", input, remote) VALUES ('nopass', 'SELECT 1 AS a', 'dbname=' || :'DBNAME');
RESET ROLE;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..150 LOOP
        IF (SELECT count(*) FROM remote_nopass_test_schema.task WHERE "group" = 'nopass' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 150 x pg_sleep(0.1) waiting for task group ''nopass'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state = 'FAIL' AS rejected_without_password, error LIKE '%password is required%' AS password_error_ok FROM remote_nopass_test_schema.task WHERE "group" = 'nopass';
