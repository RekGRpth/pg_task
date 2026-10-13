SELECT (SELECT count(*) FROM pg_catalog.pg_settings WHERE name = 'gp_role') > 0 AS is_gp
\gset
SELECT '/tmp/pg_task_gp_policy_' || pg_backend_pid() || '.sql' AS gp_policy_file
\gset
\pset tuples_only on
\pset format unaligned
\o :gp_policy_file
SELECT CASE WHEN :'is_gp' = 't' THEN 'SELECT NOT EXISTS (SELECT 1 FROM gp_dist_random(' || chr(39) || 'pg_class' || chr(39) || ') WHERE oid = ' || chr(39) || 'task_column_drift_test_schema.task_column_drift_test' || chr(39) || '::regclass) AS need_gp_utility' ELSE 'SELECT false AS need_gp_utility' END;
SELECT '\gset';
\o
\i :gp_policy_file
SELECT '/tmp/pg_task_gp_utility_' || pg_backend_pid() || '.sql' AS gp_utility_file
\gset
\o :gp_utility_file
SELECT CASE WHEN :'need_gp_utility' = 't' THEN '\connect "dbname=' || :'DBNAME' || ' options=' || chr(39) || '-c gp_session_role=utility' || chr(39) || '"' ELSE '' END;
\o
\i :gp_utility_file
\pset tuples_only off
\pset format aligned
ALTER TABLE task_column_drift_test_schema.task_column_drift_test DROP COLUMN "delimiter" CASCADE;
\connect :DBNAME
SELECT count(*) = 0 AS column_dropped FROM pg_catalog.pg_attribute WHERE attrelid = 'task_column_drift_test_schema.task_column_drift_test'::regclass AND attname = 'delimiter' AND NOT attisdropped;
