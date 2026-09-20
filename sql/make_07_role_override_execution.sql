DELETE FROM role_test_schema.task;
INSERT INTO role_test_schema.task (input) VALUES ('SELECT 1 AS a');
DO $body$ BEGIN
    FOR i IN 1..120 LOOP
        IF (SELECT count(*) FROM role_test_schema.task WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM role_test_schema.task;
