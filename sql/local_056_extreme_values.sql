-- values out of range for the arithmetic pg_work does on a task fail the author's own insert, rather than pg_work and every task it runs; a timeout longer than about 24.8 days works as that long, and a max of INT_MIN as a pause that long
DELETE FROM task WHERE "group" LIKE 'extreme\_%';
\set remote NULL
SELECT pid AS work_pid FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND application_name LIKE 'pg_work public task %'
\gset
INSERT INTO task ("group", timeout, remote, input) VALUES ('extreme_timeout', '1 month', :remote, 'SELECT 1 AS a');
INSERT INTO task ("group", max, remote, input) VALUES ('extreme_max', -2147483648, :remote, 'SELECT 2 AS a'), ('extreme_max', -2147483648, :remote, 'SELECT 3 AS a');
DO $body$ BEGIN
    INSERT INTO task ("group", active, input) VALUES ('extreme_active', '300000 years', 'SELECT 4 AS a');
    RAISE NOTICE 'accepted';
EXCEPTION WHEN SQLSTATE '22008' THEN RAISE NOTICE 'rejected: %', SQLERRM;
END;$body$ LANGUAGE plpgsql;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" LIKE 'extreme\_%' AND state IN ('DONE', 'FAIL')) = 2 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for the timeout task and the first max task to finish'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT "group", input, state, output, plan > now() + '1 day' AS pushed_away FROM task WHERE "group" LIKE 'extreme\_%' ORDER BY id;
SELECT count(*) = 1 AS same_work FROM pg_catalog.pg_stat_activity WHERE pid = :work_pid;
DELETE FROM task WHERE "group" LIKE 'extreme\_%';
