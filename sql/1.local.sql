\unset ECHO
\set QUIET 1
\pset format unaligned
\pset tuples_only true
\pset pager off
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

BEGIN;

INSERT INTO task (input) VALUES ('SELECT 1');

COMMIT;

SELECT pg_sleep(2);

BEGIN;

CREATE EXTENSION pgtap;

SELECT plan(1);

SELECT row_eq($$SELECT input, output, error, state FROM task ORDER BY id desc LIMIT 1$$, ROW('SELECT 1'::text, '1'::text, ''::text, 'DONE'::state), 'SELECT 1');

SELECT * FROM finish();

ROLLBACK;
