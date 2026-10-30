-- a FATAL of the remote server is the one error of the task, as of a local one, not followed by a second one, of the connection it closes
DELETE FROM task WHERE "group" = 'fatal_one_error';
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO task ("group", remote, input) VALUES ('fatal_one_error', :remote, 'SET exit_on_error = on; SELECT 1/0');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'fatal_one_error' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''fatal_one_error'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, output, error FROM task WHERE "group" = 'fatal_one_error';
DELETE FROM task WHERE "group" = 'fatal_one_error';
