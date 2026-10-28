-- the pattern of the README to retry till success: the last statement of the input cancels the repeat of its own task, by pg_task.id, which the bookkeeping reads after the input, no repeat inserted then, while a failed one, its update rolled back, is repeated; only the task's own run may change its repeat once it left PLAN, the update of another failing still
DELETE FROM task WHERE "group" LIKE 'retry_success_%';
INSERT INTO task ("group", input, repeat) VALUES
    ('retry_success_done', $q$SELECT 1; UPDATE task SET repeat = '0 sec' WHERE id = current_setting('pg_task.id')::bigint$q$, '1 hour'),
    ('retry_success_fail', $q$SELECT 1 / 0; UPDATE task SET repeat = '0 sec' WHERE id = current_setting('pg_task.id')::bigint$q$, '1 hour');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" LIKE 'retry_success_%' AND state NOT IN ('DONE', 'GONE', 'FAIL') AND plan < statement_timestamp() + '1 min') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the tasks of groups ''retry_success_%%'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", state, repeat, count(*) OVER (PARTITION BY "group") AS rows FROM task WHERE "group" LIKE 'retry_success_%' AND parent IS NULL ORDER BY "group";
SELECT "group", state FROM task WHERE "group" LIKE 'retry_success_%' AND parent IS NOT NULL ORDER BY "group";
\set VERBOSITY terse
UPDATE task SET repeat = '0 sec' WHERE "group" = 'retry_success_fail' AND parent IS NULL;
\set VERBOSITY default
DELETE FROM task WHERE "group" LIKE 'retry_success_%';
