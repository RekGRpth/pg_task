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

SELECT plan(94);

SELECT has_schema(current_setting('pg_task.default_schema', false)::name);

SELECT has_enum(current_setting('pg_task.default_schema', false)::name, 'state'::name);

SELECT enum_has_labels(current_setting('pg_task.default_schema', false)::name, 'state'::name, ARRAY['PLAN', 'TAKE', 'WORK', 'DONE', 'FAIL', 'STOP']);

SELECT has_table(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name);

SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'id'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('id') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'parent'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('parent') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'plan'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('plan') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'start'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('start') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'stop'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('stop') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'live'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('live') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'timeout'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('timeout') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'repeat'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('repeat') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'hash'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('hash') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'count'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('count') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'max'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('max') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'pid'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('pid') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'state'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('state') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delete'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('delete') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'drift'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('drift') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'header'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('header') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'string'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('string') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delimiter'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('delimiter') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'escape'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('escape') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'quote'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('quote') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'error'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('error') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'group'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('group') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'input'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('input') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'null'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('null') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'output'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('output') || ' should exist');
SELECT has_column(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'remote'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('remote') || ' should exist');

SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'id'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'plan'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'live'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'timeout'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'repeat'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'hash'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'count'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'max'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'state'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delete'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'drift'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'header'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'string'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delimiter'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'group'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'input'::name);
SELECT col_not_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'null'::name);

SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'parent'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'start'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'stop'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'pid'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'escape'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'quote'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'error'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'output'::name);
SELECT col_is_null(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'remote'::name);

SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'id'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('id') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'parent'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('parent') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'plan'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('plan') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'live'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('live') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'timeout'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('timeout') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'repeat'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('repeat') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'hash'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('hash') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'count'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('count') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'max'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('max') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'state'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('state') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delete'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('delete') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'drift'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('drift') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'header'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('header') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'string'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('string') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'delimiter'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('delimiter') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'group'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('group') || ' should have a default');
SELECT col_has_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'null'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('null') || ' should have a default');

SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'start'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('start') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'stop'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('stop') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'pid'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('pid') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'escape'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('escape') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'quote'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('quote') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'error'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('error') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'input'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('input') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'output'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('output') || ' should not have a default');
SELECT col_hasnt_default(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'remote'::name, 'Column ' || quote_ident(current_setting('pg_task.default_table', false)) || '.' || quote_ident('remote') || ' should not have a default');

SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'id'::name, 'bigint'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'parent'::name, 'bigint'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'plan'::name, 'timestamp with time zone'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'start'::name, 'timestamp with time zone'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'stop'::name, 'timestamp with time zone'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'live'::name, 'interval'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'timeout'::name, 'interval'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'repeat'::name, 'interval'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'hash'::name, 'integer'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'count'::name, 'integer'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'max'::name, 'integer'::name);
SELECT col_type_is(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name, 'pid'::name, 'integer'::name);

SELECT * FROM finish();

ROLLBACK;
