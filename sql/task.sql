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
SELECT plan(3);
SELECT has_schema(current_setting('pg_task.default_schema', false)::name);
SELECT has_enum(current_setting('pg_task.default_schema', false)::name, 'state'::name);
SELECT has_table(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name);
SELECT * FROM finish(true);
ROLLBACK;
