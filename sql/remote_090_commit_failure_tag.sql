-- an input whose commit fails, on a deferred constraint or on a cursor WITH HOLD, leaves no command tag of its last statement in the output, in every mode, spi mode too, as it is sent after the commit, those of the statements before it, and their rows, staying
DELETE FROM task WHERE "group" = 'commit_failure_tag';
SET client_min_messages = warning;
DROP TABLE IF EXISTS cft_u, cft_t;
RESET client_min_messages;
CREATE TABLE cft_u (i int UNIQUE DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE cft_t (i int);
SELECT quote_literal('dbname=' || :'DBNAME') AS remote
\gset
INSERT INTO task ("group", max, "delete", remote, input) VALUES
    ('commit_failure_tag', 0, false, :remote, 'INSERT INTO cft_u VALUES (2), (2)'),
    ('commit_failure_tag', 0, false, :remote, 'SELECT 1 AS n; INSERT INTO cft_u VALUES (3), (3)'),
    ('commit_failure_tag', 0, false, :remote, 'INSERT INTO cft_t VALUES (1); INSERT INTO cft_u VALUES (4), (4)'),
    ('commit_failure_tag', 0, false, :remote, 'DECLARE cft_c CURSOR WITH HOLD FOR SELECT 1 / (i - i) FROM generate_series(1, 2) AS i');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'commit_failure_tag' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''commit_failure_tag'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT left(input, 40) AS input, state, output, left(error, 50) AS error FROM task WHERE "group" = 'commit_failure_tag' ORDER BY id;
DELETE FROM task WHERE "group" = 'commit_failure_tag';
DROP TABLE cft_u, cft_t;
