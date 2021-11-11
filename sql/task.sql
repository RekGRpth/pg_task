\unset ECHO
\i test_setup.sql

SELECT plan(2);
SELECT has_schema(current_setting('pg_task.default_schema', false)::name);
SELECT has_table(current_setting('pg_task.default_schema', false)::name, current_setting('pg_task.default_table', false)::name);
SELECT * FROM finish(true);
ROLLBACK;