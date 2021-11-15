\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

BEGIN;

INSERT INTO task (input) VALUES ('SELECT 1');
INSERT INTO task (input) VALUES ('SELECT 1/0');

COMMIT;

DO $body$ <<local>> DECLARE
    count bigint;
BEGIN
    WHILE true LOOP
        SELECT count(*) FROM task WHERE state NOT IN ('DONE', 'FAIL') INTO local.count;
        IF local.count = 0 THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(1);
    END LOOP;
END;$body$ LANGUAGE plpgsql;

SELECT input, output, state FROM task WHERE input = 'SELECT 1' ORDER BY id desc LIMIT 1;
SELECT input, output, state FROM task WHERE input = 'SELECT 1/0' ORDER BY id desc LIMIT 1;
