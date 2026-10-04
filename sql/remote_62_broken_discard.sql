-- a remote connection broken while DISCARD ALL runs after a task, with the next task of the group taken into TAKE already, gives that one back to PLAN, to run on a new connection, rather than fail it with the error of the connection, never run (the first task makes DISCARD ALL take a while, dropping its many temporary tables)
DELETE FROM task WHERE "group" = 'broken_discard';
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO task ("group", max, count, remote, input) VALUES
    ('broken_discard', 0, 2, :remote, 'DO $$BEGIN FOR i IN 1..2000 LOOP EXECUTE format(''CREATE TEMP TABLE broken_discard_%s (i int)'', i); END LOOP; END$$'),
    ('broken_discard', 0, 2, :remote, 'SELECT 1 AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..3000 LOOP
        PERFORM pg_stat_clear_snapshot();
        IF EXISTS (SELECT pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE application_name = 'pg_task public task broken_discard' AND query LIKE 'DISCARD ALL%' AND state = 'active') THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 3000 x pg_sleep(0.01) waiting for the remote connection of task group ''broken_discard'' to run DISCARD ALL'; END IF;
END;$body$ LANGUAGE plpgsql;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'broken_discard' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''broken_discard'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input LIKE 'DO %' AS first, state, output, error FROM task WHERE "group" = 'broken_discard' ORDER BY id;
DELETE FROM task WHERE "group" = 'broken_discard';
