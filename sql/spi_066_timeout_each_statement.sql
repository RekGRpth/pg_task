-- the timeout of a task bounds each statement of its input, as statement_timeout does, three statements of 0.6 seconds running under one second: in spi mode on any version, and in local mode from 13 on, where exec_simple_query() starts it anew for each statement, before 13 for those up to a COMMIT at once
DELETE FROM task WHERE "group" = 'timeout_each_statement';
INSERT INTO task ("group", input, timeout) VALUES ('timeout_each_statement', 'SELECT pg_sleep(0.6); SELECT pg_sleep(0.6); SELECT pg_sleep(0.6)', '1 sec');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'timeout_each_statement' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''timeout_each_statement'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state::text = CASE WHEN current_setting('server_version_num')::int >= 130000 OR current_setting('pg_task.spi')::bool THEN 'DONE' ELSE 'FAIL' END AS state_ok FROM task WHERE "group" = 'timeout_each_statement';
DELETE FROM task WHERE "group" = 'timeout_each_statement';
