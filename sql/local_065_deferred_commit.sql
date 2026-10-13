-- an error the commit of the input raises, here that of a deferred unique constraint, fails the task, run once, in every mode, rather than its worker in spi mode, for the task to be run again on every reset
DELETE FROM task WHERE "group" = 'deferred_commit';
SET client_min_messages = warning;
DROP TABLE IF EXISTS deferred_commit_unique;
DROP SEQUENCE IF EXISTS deferred_commit_runs;
RESET client_min_messages;
CREATE TABLE deferred_commit_unique (i int UNIQUE DEFERRABLE INITIALLY DEFERRED);
CREATE SEQUENCE deferred_commit_runs;
\set remote NULL
INSERT INTO task ("group", remote, input) VALUES ('deferred_commit', :remote, 'SELECT nextval(''deferred_commit_runs'') AS n; INSERT INTO deferred_commit_unique VALUES (2), (2)');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'deferred_commit' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''deferred_commit'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT state, error LIKE '%duplicate key value violates unique constraint%' AS deferred_error, (SELECT last_value FROM deferred_commit_runs) AS runs FROM task WHERE "group" = 'deferred_commit';
DELETE FROM task WHERE "group" = 'deferred_commit';
DROP TABLE deferred_commit_unique;
DROP SEQUENCE deferred_commit_runs;
