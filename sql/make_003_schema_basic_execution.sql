DELETE FROM task_make_test_schema.task_make_test;
INSERT INTO task_make_test_schema.task_make_test (input) VALUES ('SELECT 1 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task_make_test_schema.task_make_test WHERE state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for all tasks in task_make_test_schema.task_make_test to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT output, error, state FROM task_make_test_schema.task_make_test;
