-- a task of id 0, which an author may insert, whose worker is terminated before it ran, its WORK not committed, is given back to PLAN, as any other, to run once taken again, rather than be taken for no task and left in TAKE till the reset, see dest_shmem_exit()
DELETE FROM task WHERE "group" = 'zero_id_terminate';
CREATE TABLE zero_id_terminate_hold (hold boolean);
INSERT INTO zero_id_terminate_hold VALUES (true);
CREATE FUNCTION zero_id_terminate_hold() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $function$BEGIN
    IF NEW.id = 0 AND NEW.state = 'WORK' THEN
        PERFORM set_config('application_name', 'zero_id_terminate_hold', false);
        FOR i IN 1..300 LOOP
            EXIT WHEN NOT EXISTS (SELECT 1 FROM public.zero_id_terminate_hold);
            PERFORM pg_sleep(0.1);
        END LOOP;
    END IF;
    RETURN NEW;
END;$function$;
CREATE TRIGGER zero_id_terminate_hold BEFORE UPDATE ON task FOR EACH ROW EXECUTE PROCEDURE zero_id_terminate_hold();
INSERT INTO task (id, "group", input) VALUES (0, 'zero_id_terminate', 'SELECT 0 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = 'zero_id_terminate_hold') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the worker of the task of id 0 to start it'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT pid AS hold_pid FROM pg_catalog.pg_stat_activity WHERE application_name = 'zero_id_terminate_hold'
\gset
SELECT set_config('pg_task_test.hold_pid', :'hold_pid', false) IS NOT NULL AS hold_pid_saved;
SELECT pg_terminate_backend(:hold_pid) AS terminated;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = current_setting('pg_task_test.hold_pid')::int) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the worker to be gone'; END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM zero_id_terminate_hold;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'zero_id_terminate' AND state NOT IN ('DONE', 'GONE', 'FAIL') AND (current_setting('server_version_num')::int >= 90500 OR state <> 'TAKE')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the task of id 0 to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
-- before 9.5, with no SKIP LOCKED, a worker going down gives back no task, of any id, left in TAKE till the reset, see dest_shmem_exit()
SELECT id, current_setting('server_version_num')::int < 90500 OR (state = 'DONE' AND output = '0' AND error IS NULL) AS done FROM task WHERE "group" = 'zero_id_terminate';
DROP TRIGGER zero_id_terminate_hold ON task;
DROP FUNCTION zero_id_terminate_hold();
DROP TABLE zero_id_terminate_hold;
DELETE FROM task WHERE "group" = 'zero_id_terminate';
