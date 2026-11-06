-- a remote input is split into statements to send one at a time: an SQL-standard routine body (PostgreSQL 14+) holds statements of its own between its BEGIN ATOMIC and END, each ending in a ; of its own at no depth of parentheses, the whole CREATE one statement, not cut at them, its END told from an END of a CASE by its place at a statement boundary
DELETE FROM task WHERE "group" = 'begin_atomic';
SET client_min_messages = warning;
DROP TABLE IF EXISTS begin_atomic_runs;
RESET client_min_messages;
CREATE TABLE begin_atomic_runs (v int);
SELECT quote_literal(CURRENT_TIMESTAMP) AS ct
\gset
INSERT INTO task ("group", input, remote)
SELECT 'begin_atomic', 'INSERT INTO begin_atomic_runs VALUES (0); CREATE FUNCTION pg_task_test_begin_atomic() RETURNS void LANGUAGE SQL BEGIN ATOMIC INSERT INTO begin_atomic_runs VALUES (1); INSERT INTO begin_atomic_runs VALUES (2); END; SELECT pg_task_test_begin_atomic()', 'dbname=' || :'DBNAME'
WHERE current_setting('server_version_num')::int >= 140000;
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF (SELECT count(*) FROM task WHERE "group" = 'begin_atomic' AND state NOT IN ('DONE', 'GONE', 'FAIL')) = 0 THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''begin_atomic'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT current_setting('server_version_num')::int < 140000
    OR ((SELECT count(*) = 1 FROM task WHERE "group" = 'begin_atomic' AND state = 'DONE' AND error IS NULL AND plan > :ct::timestamptz)
        AND (SELECT array_agg(v ORDER BY v) FROM begin_atomic_runs) = ARRAY[0, 1, 2]) AS ok;
SET client_min_messages = warning;
DROP FUNCTION IF EXISTS pg_task_test_begin_atomic();
RESET client_min_messages;
DELETE FROM task WHERE "group" = 'begin_atomic';
DROP TABLE begin_atomic_runs;
