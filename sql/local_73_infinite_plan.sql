-- a plan of infinity holds a task back, one of -infinity makes it due at once: neither fails pg_work, which subtracts the time from the plan of the next task to wait for it going idle, and from that of a task done for the pause of its group, nor the repeat of the task, planned from now then
DELETE FROM task WHERE "group" LIKE 'infinite_plan_%';
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database()
\gset
SELECT set_config('pg_task_test.work_pid', :'work_pid', false) IS NOT NULL AS work_pid_saved;
INSERT INTO task ("group", input, plan) VALUES ('infinite_plan_held', 'SELECT 1', 'infinity');
INSERT INTO task ("group", input, plan, max, repeat) VALUES ('infinite_plan_due', 'SELECT 2', '-infinity', -1000, '1 hour');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM task WHERE "group" = 'infinite_plan_due' AND state = 'DONE') AND EXISTS (SELECT 1 FROM task WHERE "group" = 'infinite_plan_due' AND state = 'PLAN') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task in group ''infinite_plan_due'' to be done and repeated'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pg_sleep(3);
SELECT pg_stat_clear_snapshot();
SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name LIKE 'pg_work public task %' AND datname = current_database() AND pid = current_setting('pg_task_test.work_pid')::int) AS same_work;
SELECT "group", state, isfinite(plan) AS finite, plan > now() AS later FROM task WHERE "group" LIKE 'infinite_plan_%' ORDER BY id;
DELETE FROM task WHERE "group" LIKE 'infinite_plan_%';
