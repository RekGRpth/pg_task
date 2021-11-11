\unset ECHO
\set QUIET 1
-- Turn off echo and keep things quiet.

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager off

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Load the TAP functions.
BEGIN;
--\i pgtap.sql
CREATE EXTENSION pgtap;
SELECT plan(5);
SELECT has_schema(current_setting('pg_task.default_schema', false)::name);
SELECT has_enum(current_setting('pg_task.default_schema', false)::name, 'state'::name);
SELECT enum_has_labels(current_setting('pg_task.default_schema', false)::name, 'state'::name, ARRAY['PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP']);
SELECT has_table(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name);
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'id'::name, 'Column ' || quote_ident(current_setting('pg_task.default_schema', false)) || '.' || quote_ident('id') || ' should exist');
SELECT * FROM finish();
ROLLBACK;
