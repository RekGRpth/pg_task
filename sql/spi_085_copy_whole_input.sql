-- COPY FROM STDIN, which pg_task has no data to feed, and, in local mode and spi mode, COPY TO STDOUT, fail the whole input before any statement of it runs, whatever stands before the COPY, with the same error in every mode that has it, rather than once it gets there, the statements before it run and their output kept
DELETE FROM task WHERE "group" = 'copy_whole_input';
SET client_min_messages = warning;
DROP TABLE IF EXISTS cwi_t;
RESET client_min_messages;
CREATE TABLE cwi_t (i int);
\set remote NULL
INSERT INTO task ("group", max, "delete", remote, input) VALUES
    ('copy_whole_input', 0, false, :remote, 'SELECT 1; COPY (SELECT 2) TO STDOUT'),
    ('copy_whole_input', 0, false, :remote, 'INSERT INTO cwi_t VALUES (1); COPY (SELECT 2) TO STDOUT'),
    ('copy_whole_input', 0, false, :remote, 'INSERT INTO cwi_t VALUES (3); COPY cwi_t FROM STDIN'),
    ('copy_whole_input', 0, false, :remote, 'COPY (SELECT 4) TO STDOUT; SELECT 5'),
    ('copy_whole_input', 0, false, :remote, 'COPY cwi_t FROM STDIN; SELECT 6');
DO $body$ DECLARE ok boolean := false; BEGIN
    FOR i IN 1..300 LOOP
        IF NOT EXISTS (SELECT 1 FROM task WHERE "group" = 'copy_whole_input' AND state NOT IN ('DONE', 'GONE', 'FAIL')) THEN ok := true; EXIT; END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
    IF NOT ok THEN RAISE EXCEPTION 'timed out after 300 x pg_sleep(0.1) waiting for task group ''copy_whole_input'' to finish (leave PLAN/TAKE/WORK)'; END IF;
END;$body$ LANGUAGE plpgsql;
SELECT input, state, output, error FROM task WHERE "group" = 'copy_whole_input' ORDER BY id;
SELECT count(*) AS rows_inserted FROM cwi_t;
DELETE FROM task WHERE "group" = 'copy_whole_input';
DROP TABLE cwi_t;
