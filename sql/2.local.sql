\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

BEGIN;

INSERT INTO task (input) VALUES ('SELECT 1/0 AS a');

INSERT INTO task (input) VALUES ('SELECT 1 AS a');
INSERT INTO task (input) VALUES ('SELECT 1 AS a, 2 AS b');
INSERT INTO task (input) VALUES ('SELECT 1 AS a;SELECT 2 AS b');
INSERT INTO task (input) VALUES ('SELECT 1 AS a, 2 AS b;SELECT 3 AS c');
INSERT INTO task (input) VALUES ('SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d');

COMMIT;

DO $body$ <<local>> DECLARE
    count bigint;
BEGIN
    WHILE true LOOP
        SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL') INTO local.count;
        IF local.count = 0 THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;

SELECT input, output, state FROM task WHERE input = 'SELECT 1/0 AS a' ORDER BY id desc LIMIT 1;

SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a' ORDER BY id desc LIMIT 1;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a, 2 AS b' ORDER BY id desc LIMIT 1;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a;SELECT 2 AS b' ORDER BY id desc LIMIT 1;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c' ORDER BY id desc LIMIT 1;
SELECT input, output, state FROM task WHERE input = 'SELECT 1 AS a, 2 AS b;SELECT 3 AS c, 4 AS d' ORDER BY id desc LIMIT 1;
