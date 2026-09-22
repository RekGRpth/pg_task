DELETE FROM task WHERE "group" IN ('run_sys_a', 'run_sys_b', 'run_sys_c');
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
ALTER SYSTEM SET pg_task.run = 1;
SELECT pg_reload_conf();
INSERT INTO task ("group", input) VALUES ('run_sys_a', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('run_sys_b', 'SELECT pg_sleep(5) AS a');
INSERT INTO task ("group", input) VALUES ('run_sys_c', 'SELECT pg_sleep(5) AS a');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FILTER (WHERE state != 'PLAN') FROM task WHERE "group" IN ('run_sys_a', 'run_sys_b', 'run_sys_c')) >= 1 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for at least one task in groups ''run_sys_a'', ''run_sys_b'', ''run_sys_c'' to leave PLAN state'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT count(*) FILTER (WHERE state != 'PLAN') >= 1 AS some_dispatched, count(*) FILTER (WHERE state = 'PLAN') >= 1 AS some_capped FROM task WHERE "group" IN ('run_sys_a', 'run_sys_b', 'run_sys_c') AND plan > :ct::timestamp;
ALTER SYSTEM RESET pg_task.run;
SELECT pg_reload_conf();
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..30 LOOP
        IF (SELECT count(*) FROM task WHERE "group" IN ('run_sys_a', 'run_sys_b', 'run_sys_c') AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 30 x pg_sleep(1) waiting for task groups ''run_sys_a'', ''run_sys_b'', ''run_sys_c'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
DELETE FROM task WHERE "group" IN ('run_sys_a', 'run_sys_b', 'run_sys_c');
