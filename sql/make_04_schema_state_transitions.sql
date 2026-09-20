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
